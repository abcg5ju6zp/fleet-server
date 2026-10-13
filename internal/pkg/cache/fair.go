// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cache

import (
	"container/list"
	"sort"
	"sync"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// reclaimRounds bounds how many victim batches a single set produces.
const reclaimRounds = 2

// fEntry is the envelope stored as the cache value while per-owner cost
// accounting is active. It carries the entry's admission-time owner and the
// lease that holds its cost in the budget registry until the entry exits.
type fEntry struct {
	key   string
	owner tenant.Owner
	cost  int64
	val   any
	seq   uint64

	lease    budget.Lease
	elem     *list.Element
	detached bool

	// reclaimed marks entries explicitly removed to honor another owner's
	// guaranteed share; reset marks entries dropped by a cache Reconfigure.
	// Both keep the exit-callback metrics attributed correctly.
	reclaimed bool
	reset     bool
}

// fairOwnerStats counts cache outcomes per owner for the metrics layer.
type fairOwnerStats struct {
	sets          int64
	rejected      int64
	evicted       int64
	evictedCost   int64
	reclaimed     int64
	reclaimedCost int64
}

// fairCache layers per-owner guaranteed cost shares over a single shared
// ristretto cache. The underlying TinyLFU policy is untouched: admission
// only rejects when an owner is beyond its guarantee under contention, and
// capacity pressure is relieved by explicitly deleting borrowed entries
// before owners lose their guaranteed entries.
//
// All cost accounting lives in the budget registry through weighted leases
// attached to entries; fairCache only keeps the per-owner insertion-ordered
// metadata needed to pick eviction victims.
type fairCache struct {
	registry *budget.Registry
	res      budget.Resource
	cacher   Cacher

	mu      sync.Mutex
	meta    map[string]*fEntry
	pending map[*fEntry]struct{}
	order   map[tenant.Owner]*list.List
	seq     uint64
	stats   map[tenant.Owner]*fairOwnerStats
}

func newFairCache(reg *budget.Registry, cacher Cacher) *fairCache {
	return &fairCache{
		registry: reg,
		res:      budget.ResourceCacheCost,
		cacher:   cacher,
		meta:     make(map[string]*fEntry),
		pending:  make(map[*fEntry]struct{}),
		order:    make(map[tenant.Owner]*list.List),
		stats:    make(map[tenant.Owner]*fairOwnerStats),
	}
}

func (fc *fairCache) enabled() bool {
	return fc != nil && fc.registry != nil && fairEvictionAccounting && fc.registry.IsManaged(fc.res)
}

func (fc *fairCache) statsLocked(o tenant.Owner) *fairOwnerStats {
	s := fc.stats[o]
	if s == nil {
		s = &fairOwnerStats{}
		fc.stats[o] = s
	}
	return s
}

func (fc *fairCache) ownerList(o tenant.Owner) *list.List {
	l := fc.order[o]
	if l == nil {
		l = list.New()
		fc.order[o] = l
	}
	return l
}

// set stores val under key attributed to owner, charging cost to the owner's
// cache-cost budget. A rejected (beyond-guarantee under contention) set
// returns false: caching stays best effort and never fails the request that
// populated the entry.
func (fc *fairCache) set(key string, val any, cost int64, ttl time.Duration, owner tenant.Owner) bool {
	if owner == "" {
		owner = tenant.Default
	}
	if !fc.enabled() || cost <= 0 {
		return fc.cacher.SetWithTTL(key, val, cost, ttl)
	}

	fc.mu.Lock()
	fc.seq++
	e := &fEntry{key: key, owner: owner, cost: cost, val: val, seq: fc.seq}

	// Same-key overwrite with identical weight transfers the existing
	// lease to the new envelope, so rewriting a hot key never perturbs the
	// owner's cost share. Different weight/cost re-admits normally; on
	// rejection the previous entry is left untouched.
	var lease budget.Lease
	if prev := fc.meta[key]; prev != nil && prev.owner == owner && prev.cost == cost {
		lease = prev.lease
		prev.lease = nil
		fc.detachLocked(prev)
	} else {
		l, err := fc.admitLocked(owner, cost)
		if err != nil {
			fc.statsLocked(owner).rejected++
			fc.mu.Unlock()
			return false
		}
		lease = l
		if prev := fc.meta[key]; prev != nil {
			fc.detachLocked(prev)
			prev.lease.Release()
		}
	}
	e.lease = lease
	fc.mu.Unlock()

	ok := fc.cacher.SetWithTTL(key, e, cost, ttl)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	if !ok {
		// The write was dropped before entering the cache.
		delete(fc.pending, e)
		e.lease.Release()
		return false
	}
	if _, exited := fc.pending[e]; exited {
		// The policy rejected the entry before this commit ran.
		delete(fc.pending, e)
		fc.statsLocked(owner).evicted++
		fc.statsLocked(owner).evictedCost += cost
		return true
	}
	cur := fc.meta[key]
	if cur != nil && cur.seq > e.seq {
		// A concurrent newer write for the same key won; the synchronous
		// overwrite callback already accounted for this envelope, so it
		// must not be installed.
		e.lease.Release()
		return true
	}
	fc.meta[key] = e
	e.elem = fc.ownerList(owner).PushBack(e)
	fc.statsLocked(owner).sets++
	return true
}

// admitLocked runs the non-blocking weighted admission, explicitly
// reclaiming borrowed entries on rejection. It must be called with fc.mu
// held and returns with fc.mu held; underlying cache deletes run without
// the lock because Del hands the value to the exit callback synchronously.
func (fc *fairCache) admitLocked(owner tenant.Owner, cost int64) (budget.Lease, error) {
	var lastErr error
	for range reclaimRounds {
		lease, err := fc.registry.TryAcquireN(fc.res, owner, cost)
		if err == nil {
			return lease, nil
		}
		lastErr = err
		victims := fc.planVictimsLocked(owner, cost, err)
		if len(victims) == 0 {
			return nil, err
		}
		keys := fc.detachLocked(victims...)
		fc.mu.Unlock()
		for _, k := range keys {
			// Entries are already detached, so the synchronous exit
			// callback performs no accounting.
			fc.cacher.Del(k)
		}
		fc.mu.Lock()
	}
	return nil, lastErr
}

// planVictimsLocked chooses entries that may be deleted to make room for a
// cost-sized admission. Guaranteed admissions first reclaim other owners'
// borrowed entries (largest borrowers first, oldest entries first) and then
// the owner's own oldest entries; borrowing admissions may only replace
// their own borrowed entries. Callers hold fc.mu.
func (fc *fairCache) planVictimsLocked(owner tenant.Owner, cost int64, admitErr error) []*fEntry {
	rej, _ := budget.As(admitErr)

	snap := fc.registry.Snapshot()
	var rs *budget.ResourceSnapshot
	for i := range snap.Resource {
		if snap.Resource[i].Resource == fc.res {
			rs = &snap.Resource[i]
			break
		}
	}
	if rs == nil {
		return nil
	}
	want := cost
	if want > rs.Capacity {
		want = rs.Capacity
	}
	need := rs.InUse + want - rs.Capacity
	if need <= 0 {
		return nil
	}

	// A request whose post-set usage stays within the owner's guarantee
	// is entitled to reclaim borrowed capacity.
	var g, inUse int64
	for _, os := range rs.Owners {
		if tenant.Owner(os.Owner) == owner {
			g, inUse = os.Guarantee, os.InUse
		}
	}
	guaranteed := rej != nil && rej.Reason == budget.ReasonMax && inUse+want <= g

	var victims []*fEntry
	if guaranteed {
		type borrower struct {
			owner    tenant.Owner
			borrowed int64
		}
		var borrowers []borrower
		for _, os := range rs.Owners {
			if tenant.Owner(os.Owner) == owner {
				continue
			}
			if b := os.InUse - os.Guarantee; b > 0 {
				borrowers = append(borrowers, borrower{owner: tenant.Owner(os.Owner), borrowed: b})
			}
		}
		sort.Slice(borrowers, func(i, j int) bool { return borrowers[i].borrowed > borrowers[j].borrowed })
		for _, b := range borrowers {
			l := fc.order[b.owner]
			if l == nil {
				continue
			}
			remaining := b.borrowed
			for el := l.Front(); el != nil && remaining > 0 && need > 0; {
				next := el.Next()
				e, ok := el.Value.(*fEntry)
				if ok && !e.detached && e.cost <= remaining {
					e.reclaimed = true
					victims = append(victims, e)
					remaining -= e.cost
					need -= e.cost
				}
				el = next
			}
			if need <= 0 {
				return victims
			}
		}
	}

	// Finally the owner may replace its own oldest entries to make room:
	// replacing entries within one's own share is ordinary cache turnover
	// and never takes capacity from another owner's guarantee. This also
	// keeps the single-owner case equivalent to the previous global LRU
	// behavior. Borrowing admissions skip other owners' borrowed entries;
	// they still reach this self-replacement step.
	selfAllowance := inUse
	if l := fc.order[owner]; l != nil {
		for el := l.Front(); el != nil && selfAllowance > 0 && need > 0; {
			next := el.Next()
			e, ok := el.Value.(*fEntry)
			if ok && !e.detached && e.cost <= selfAllowance {
				e.reclaimed = true
				victims = append(victims, e)
				selfAllowance -= e.cost
				need -= e.cost
			}
			el = next
		}
	}
	return victims
}

// detachLocked removes entries from the key and LRU metadata and releases
// their cost leases. It returns the physical cache keys to delete; callers
// delete outside fc.mu so the synchronous ristretto exit callback can take
// the lock. Callers hold fc.mu.
func (fc *fairCache) detachLocked(entries ...*fEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.detached {
			continue
		}
		e.detached = true
		if cur := fc.meta[e.key]; cur == e {
			delete(fc.meta, e.key)
		}
		if e.elem != nil {
			if l := fc.order[e.owner]; l != nil {
				l.Remove(e.elem)
			}
			e.elem = nil
		}
		if e.lease != nil {
			e.lease.Release()
		}
		if e.reclaimed {
			s := fc.statsLocked(e.owner)
			s.reclaimed++
			s.reclaimedCost += e.cost
		}
		keys = append(keys, e.key)
	}
	return keys
}

// onExit is the universal ristretto exit callback (eviction, TTL expiry,
// delete, rejection and replacement). Accounting is idempotent: explicit
// reclaims and overwrites release the lease themselves.
func (fc *fairCache) onExit(v any) {
	e, ok := v.(*fEntry)
	if !ok || e == nil || e.lease == nil {
		return
	}
	fc.mu.Lock()
	if !e.detached {
		committed := false
		if cur := fc.meta[e.key]; cur == e {
			delete(fc.meta, e.key)
			committed = true
		}
		if e.elem != nil {
			if l := fc.order[e.owner]; l != nil {
				l.Remove(e.elem)
			}
			e.elem = nil
		}
		e.detached = true
		switch {
		case e.reset:
			// Cache Reconfigure: no per-owner metrics for the bulk drop.
		case e.reclaimed:
			s := fc.statsLocked(e.owner)
			s.reclaimed++
			s.reclaimedCost += e.cost
		case !committed:
			// Rejected by the admission policy (or replaced) before the
			// originating set committed its metadata: tell it so.
			fc.pending[e] = struct{}{}
		default:
			s := fc.statsLocked(e.owner)
			s.evicted++
			s.evictedCost += e.cost
		}
	}
	fc.mu.Unlock()
	e.lease.Release()
}

// reset releases every tracked lease and forgets all metadata. It runs
// before the old underlying cache is closed on Reconfigure; close callbacks
// then find detached envelopes and perform no accounting.
func (fc *fairCache) reset() {
	if fc == nil {
		return
	}
	fc.mu.Lock()
	entries := make([]*fEntry, 0, len(fc.meta))
	for _, e := range fc.meta {
		e.reset = true
		entries = append(entries, e)
	}
	fc.detachLocked(entries...)
	// Entries that were rejected between set and commit also clear.
	for e := range fc.pending {
		e.lease.Release()
	}
	fc.pending = make(map[*fEntry]struct{})
	fc.order = make(map[tenant.Owner]*list.List)
	fc.mu.Unlock()
}

// bindCacher swaps the underlying cache after a Reconfigure.
func (fc *fairCache) bindCacher(c Cacher) {
	fc.mu.Lock()
	fc.cacher = c
	fc.mu.Unlock()
}

// unwrap returns the user value of a stored entry, transparently handling
// both fair envelopes and raw legacy values.
func unwrap(v any) any {
	if e, ok := v.(*fEntry); ok {
		return e.val
	}
	return v
}

// FairOwnerStats is a point-in-time copy of one owner's cache outcomes.
type FairOwnerStats struct {
	Sets          int64 `json:"sets"`
	Rejected      int64 `json:"rejected"`
	Evicted       int64 `json:"evicted"`
	EvictedCost   int64 `json:"evicted_cost"`
	Reclaimed     int64 `json:"reclaimed"`
	ReclaimedCost int64 `json:"reclaimed_cost"`
}

// FairStatsSnapshot is a point-in-time copy of per-owner cache statistics.
type FairStatsSnapshot struct {
	Enabled bool                            `json:"enabled"`
	Owners  map[tenant.Owner]FairOwnerStats `json:"owners"`
}

// statsSnapshot returns a lock-consistent copy of the per-owner counters.
func (fc *fairCache) statsSnapshot() FairStatsSnapshot {
	out := FairStatsSnapshot{Enabled: fc.enabled(), Owners: map[tenant.Owner]FairOwnerStats{}}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for o, s := range fc.stats {
		out.Owners[o] = FairOwnerStats{
			Sets:          s.sets,
			Rejected:      s.rejected,
			Evicted:       s.evicted,
			EvictedCost:   s.evictedCost,
			Reclaimed:     s.reclaimed,
			ReclaimedCost: s.reclaimedCost,
		}
	}
	return out
}
