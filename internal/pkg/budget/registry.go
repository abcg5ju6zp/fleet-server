// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package budget implements a resource-agnostic two-level budget kernel:
// a global capacity ceiling plus per-owner guaranteed shares with
// work-conserving borrow and contention-triggered reclaim.
//
// Admission is always non-blocking. Owners below their guarantee are
// preferred: contention markers (set when an owner is rejected while below
// its guarantee) block new borrow admissions of other owners until the
// contended owner makes progress or the marker ages out. In-flight work is
// never preempted; reclaim completes as borrowed leases drain.
package budget

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// OtherOwner aggregates owners beyond Config.MaxTrackedOwners so that metrics
// label cardinality stays bounded.
const OtherOwner tenant.Owner = "_other"

// Clock allows tests to drive time deterministically.
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

// Option configures a Registry at construction.
type Option func(*Registry)

// WithClock injects a clock.
func WithClock(c Clock) Option {
	return func(r *Registry) {
		if c != nil {
			r.clock = c
		}
	}
}

// Registry owns the budgets for every managed resource. A single instance is
// shared by the HTTP listeners, bulker, monitors, dispatchers and caches of
// one Fleet Server process; it survives server-level reconfigurations.
type Registry struct {
	mu        sync.Mutex
	cfg       Config
	clock     Clock
	resources map[Resource]*resourceBudget
}

// New creates a Registry from cfg.
func New(cfg Config, opts ...Option) *Registry {
	r := &Registry{
		cfg:   cfg.Defaulted(),
		clock: wallClock{},
	}
	for _, o := range opts {
		o(r)
	}
	r.resources = make(map[Resource]*resourceBudget, len(r.cfg.Resources))
	for res, rc := range r.cfg.Resources {
		r.resources[res] = newResourceBudget(rc, r.cfg)
	}
	return r
}

// Run keeps active-owner state compact until ctx is canceled.
func (r *Registry) Run(ctx context.Context) {
	interval := r.cfg.IdleTTL / 4
	if interval <= 0 {
		interval = defaultIdleTTL / 4
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.gc()
		}
	}
}

// Enabled reports the master switch.
func (r *Registry) Enabled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.Enabled
}

// Reconfigure applies a new configuration in place: in-flight leases are
// preserved, counters are not reset. Shrunk capacity rejects new admissions
// until the resource drains.
func (r *Registry) Reconfigure(cfg Config) {
	cfg = cfg.Defaulted()
	r.mu.Lock()
	defer r.mu.Unlock()

	next := make(map[Resource]*resourceBudget, len(cfg.Resources))
	for res, rc := range cfg.Resources {
		if rb, ok := r.resources[res]; ok {
			rb.reconfigure(rc, cfg)
			next[res] = rb
		} else {
			next[res] = newResourceBudget(rc, cfg)
		}
	}
	r.resources = next
	r.cfg = cfg
}

// TryAcquire claims one slot of res for owner. It never blocks: on success a
// Lease is returned whose Release must run (deferred) on every exit path; on
// failure a *RejectError is returned.
func (r *Registry) TryAcquire(res Resource, owner tenant.Owner) (Lease, error) {
	return r.TryAcquireN(res, owner, 1)
}

// TryAcquireN claims n weighted units of res for owner. Concurrency slots use
// n=1 (see TryAcquire); byte-priced resources such as the shared cache charge
// the entry cost as n. The returned lease releases exactly n units. A request
// larger than the whole capacity is admitted against capacity-sized head room
// when the pool is otherwise free, matching the underlying cache's treatment
// of oversized entries; the lease still carries the real weight so accounting
// reconverges on release.
func (r *Registry) TryAcquireN(res Resource, owner tenant.Owner, n int64) (Lease, error) {
	if n <= 0 {
		return noopLease{owner: normalize(owner), resource: res}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.clock.Now()
	rc, ok := r.cfg.Resources[res]
	if !ok || !rc.managed(r.cfg.Enabled) {
		return noopLease{owner: normalize(owner), resource: res}, nil
	}
	rb := r.resources[res]
	if rb == nil {
		rb = newResourceBudget(rc, r.cfg)
		r.resources[res] = rb
	}
	o := rb.track(owner, now, r.cfg.MaxTrackedOwners)

	if wait := o.backoffUntil.Sub(now); wait > 0 {
		rb.reject(o, ReasonBackoff)
		return nil, &RejectError{Reason: ReasonBackoff, RetryAfter: wait}
	}

	rb.ensureGuarantees()
	want := n
	if want > rb.cfg.Capacity {
		want = rb.cfg.Capacity
	}
	if o.inUse+want <= o.guarantee {
		// An owner below its guarantee only yields to a fully exhausted
		// global pool; the rejection records contention so borrowed slots
		// are reclaimed to it as leases drain.
		if rb.total+want > rb.cfg.Capacity {
			rb.reject(o, ReasonMax)
			o.starvedAt = now
			return nil, &RejectError{Reason: ReasonMax}
		}
		// Guaranteed progress clears any previous contention marker.
		o.starvedAt = time.Time{}
		rb.admit(o, n)
		return &lease{registry: r, resource: res, owner: o.name, amount: n}, nil
	}

	if rb.hasContention(now, o.name, r.cfg.ReclaimTTL) {
		rb.reject(o, ReasonBorrow)
		return nil, &RejectError{Reason: ReasonBorrow}
	}
	if o.inUse+want > o.guarantee+rb.cfg.BorrowMax {
		rb.reject(o, ReasonBorrow)
		return nil, &RejectError{Reason: ReasonBorrow}
	}
	if rb.total+want > rb.cfg.Capacity {
		rb.reject(o, ReasonMax)
		return nil, &RejectError{Reason: ReasonMax}
	}
	rb.admit(o, n)
	return &lease{registry: r, resource: res, owner: o.name, amount: n}, nil
}

// IsManaged reports whether res currently participates in namespace
// accounting; components such as the cache keep no ownership metadata for
// unmanaged resources.
func (r *Registry) IsManaged(res Resource) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rc, ok := r.cfg.Resources[res]
	return ok && rc.managed(r.cfg.Enabled)
}

// release returns claimed units. It is invoked by lease.Release and
// tolerates resources that disappeared through reconfiguration.
func (r *Registry) release(res Resource, owner tenant.Owner, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rb := r.resources[res]
	if rb == nil {
		return
	}
	o := rb.owners[owner]
	if o == nil && owner == OtherOwner {
		o = rb.other
	}
	rb.release(o, n)
}

// ReportFailure records a failed operation attributed to owner and arms its
// exponential cooldown once the failure threshold is reached.
func (r *Registry) ReportFailure(res Resource, owner tenant.Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rb, ok := r.cfg.Resources[res]
	if !ok || !rb.managed(r.cfg.Enabled) {
		return
	}
	rb2 := r.resources[res]
	if rb2 == nil {
		rb2 = newResourceBudget(rb, r.cfg)
		r.resources[res] = rb2
	}
	o := rb2.track(owner, r.clock.Now(), r.cfg.MaxTrackedOwners)
	now := r.clock.Now()
	o.failures++
	if o.failures >= r.cfg.Backoff.FailureThreshold {
		step := o.failures - r.cfg.Backoff.FailureThreshold
		d := r.cfg.Backoff.Initial << min(step, 16)
		if d > r.cfg.Backoff.Max || d <= 0 {
			d = r.cfg.Backoff.Max
		}
		o.backoffUntil = now.Add(d)
	}
}

// ReportSuccess clears owner failure/backoff state for a resource.
func (r *Registry) ReportSuccess(res Resource, owner tenant.Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rc, ok := r.cfg.Resources[res]
	if !ok || !rc.managed(r.cfg.Enabled) {
		return
	}
	rb := r.resources[res]
	if rb == nil {
		return
	}
	o := rb.owners[owner]
	if o == nil && owner == OtherOwner {
		o = rb.other
	}
	if o == nil {
		return
	}
	o.failures = 0
	o.backoffUntil = time.Time{}
}

// IsBackedOff reports whether owner currently rejects new admissions for res.
func (r *Registry) IsBackedOff(res Resource, owner tenant.Owner) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, o := r.locate(res, owner)
	return o != nil && o.backoffUntil.After(r.clock.Now())
}

func (r *Registry) locate(res Resource, owner tenant.Owner) (*resourceBudget, *ownerState) {
	rb := r.resources[res]
	if rb == nil {
		return nil, nil
	}
	o := rb.owners[owner]
	if o == nil && owner == OtherOwner {
		o = rb.other
	}
	return rb, o
}

func (r *Registry) gc() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	for _, rb := range r.resources {
		removed := false
		for name, o := range rb.owners {
			if o.inUse == 0 && now.Sub(o.lastSeen) > r.cfg.IdleTTL {
				delete(rb.owners, name)
				removed = true
				continue
			}
			if !o.starvedAt.IsZero() && now.Sub(o.starvedAt) > r.cfg.ReclaimTTL {
				o.starvedAt = time.Time{}
			}
		}
		if rb.other != nil && rb.other.inUse == 0 && now.Sub(rb.other.lastSeen) > r.cfg.IdleTTL {
			rb.other = nil
		}
		if removed {
			rb.dirty = true
		}
	}
}

// resourceBudget holds the accounting for one resource class.
type resourceBudget struct {
	cfg       ResourceConfig
	overrides map[string]OwnerOverride
	total     int64
	owners    map[tenant.Owner]*ownerState
	other     *ownerState
	dirty     bool
}

func newResourceBudget(rc ResourceConfig, cfg Config) *resourceBudget {
	return &resourceBudget{
		cfg:       rc,
		overrides: cfg.Overrides,
		owners:    make(map[tenant.Owner]*ownerState),
		dirty:     true,
	}
}

func (rb *resourceBudget) reconfigure(rc ResourceConfig, cfg Config) {
	rb.cfg = rc
	rb.overrides = cfg.Overrides
	rb.dirty = true
}

// track returns the live owner state, creating or aggregating it and
// refreshing activity. Callers hold the registry lock.
func (rb *resourceBudget) track(owner tenant.Owner, now time.Time, maxTracked int) *ownerState {
	owner = normalize(owner)
	if o := rb.owners[owner]; o != nil {
		o.lastSeen = now
		return o
	}
	if len(rb.owners) >= maxTracked {
		if rb.other == nil {
			rb.other = newOwnerState(OtherOwner, now)
			rb.dirty = true
		}
		rb.other.lastSeen = now
		return rb.other
	}
	o := newOwnerState(owner, now)
	rb.owners[owner] = o
	rb.dirty = true
	return o
}

func (rb *resourceBudget) admit(o *ownerState, n int64) {
	rb.total += n
	o.inUse += n
	o.admitted++
}

func (rb *resourceBudget) reject(o *ownerState, reason Reason) {
	if o.rejected == nil {
		o.rejected = make(map[Reason]int64, 3)
	}
	o.rejected[reason]++
}

func (rb *resourceBudget) release(o *ownerState, n int64) {
	if o == nil {
		return
	}
	if o.inUse >= n {
		o.inUse -= n
	} else {
		o.inUse = 0
	}
	if rb.total >= n {
		rb.total -= n
	} else {
		rb.total = 0
	}
}

// hasContention reports whether any other owner currently waits below its
// guarantee. Callers hold the registry lock.
func (rb *resourceBudget) hasContention(now time.Time, exclude tenant.Owner, ttl time.Duration) bool {
	for _, o := range rb.owners {
		if o.name == exclude {
			continue
		}
		if !o.starvedAt.IsZero() && now.Sub(o.starvedAt) <= ttl {
			return true
		}
	}
	if rb.other != nil && rb.other.name != exclude &&
		!rb.other.starvedAt.IsZero() && now.Sub(rb.other.starvedAt) <= ttl {
		return true
	}
	return false
}

// ownerState is mutable accounting for one owner on one resource.
type ownerState struct {
	name         tenant.Owner
	inUse        int64
	guarantee    int64
	weight       float64
	explicit     int64
	admitted     int64
	rejected     map[Reason]int64
	lastSeen     time.Time
	starvedAt    time.Time
	failures     int
	backoffUntil time.Time
}

func newOwnerState(name tenant.Owner, now time.Time) *ownerState {
	return &ownerState{name: name, weight: 1, lastSeen: now, rejected: map[Reason]int64{}}
}

// ensureGuarantees recomputes guaranteed shares when the active set changed.
func (rb *resourceBudget) ensureGuarantees() {
	if !rb.dirty {
		return
	}
	defer func() { rb.dirty = false }()

	cap := rb.cfg.Capacity
	states := make([]*ownerState, 0, len(rb.owners)+1)
	for _, o := range rb.owners {
		states = append(states, o)
	}
	if rb.other != nil {
		states = append(states, rb.other)
	}

	// Reset derived fields; explicit guarantees are fixed first.
	var fixed int64
	for _, o := range states {
		o.guarantee = 0
		o.weight = 1
		o.explicit = 0
	}

	// Deterministic order keeps allocation reproducible regardless of map
	// iteration order.
	sort.Slice(states, func(i, j int) bool { return states[i].name < states[j].name })

	// Weights/explicit guarantees are sourced from the global override map
	// through resolveOverride, wired by reconfigure callers below.
	for _, o := range states {
		if ov, ok := rb.overrides[string(o.name)]; ok {
			o.weight = ov.Weight
			if ov.Guarantee > 0 {
				g := clampInt64(ov.Guarantee, rb.cfg.GuaranteeMin, rb.cfg.GuaranteeMax)
				o.guarantee = min(g, cap)
				o.explicit = o.guarantee
				fixed += o.guarantee
			}
		}
		if o.weight <= 0 {
			o.weight = 1
		}
	}

	remaining := cap - fixed
	if remaining < 0 {
		remaining = 0
	}
	var weightSum float64
	for _, o := range states {
		if o.explicit == 0 {
			weightSum += o.weight
		}
	}
	for _, o := range states {
		if o.explicit > 0 {
			continue
		}
		var raw int64
		if weightSum > 0 && remaining > 0 {
			raw = int64(math.Round(float64(remaining) * o.weight / weightSum))
		}
		o.guarantee = clampInt64(raw, rb.cfg.GuaranteeMin, rb.cfg.GuaranteeMax)
	}
}

// normalize maps empty owners to the shared default bucket.
func normalize(o tenant.Owner) tenant.Owner {
	if o == "" {
		return tenant.Default
	}
	return o
}

func clampInt64(v, lo, hi int64) int64 {
	if lo > 0 && v < lo {
		return lo
	}
	if hi > 0 && v > hi {
		return hi
	}
	return v
}
