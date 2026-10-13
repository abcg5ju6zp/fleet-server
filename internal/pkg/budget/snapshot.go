// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import (
	"sort"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// OwnerSnapshot is the explainable state of one owner on one resource.
type OwnerSnapshot struct {
	Owner        string           `json:"owner"`
	Guarantee    int64            `json:"guarantee"`
	InUse        int64            `json:"in_use"`
	Borrowed     int64            `json:"borrowed"`
	Admitted     int64            `json:"admitted"`
	Rejected     map[Reason]int64 `json:"rejected"`
	Failures     int64            `json:"failures"`
	BackoffUntil *time.Time       `json:"backoff_until,omitempty"`
	Starved      bool             `json:"starved"`
	LastSeen     time.Time        `json:"last_seen"`
}

// ResourceSnapshot is the explainable state of one resource class.
type ResourceSnapshot struct {
	Resource     Resource        `json:"resource"`
	Enabled      bool            `json:"enabled"`
	Capacity     int64           `json:"capacity"`
	InUse        int64           `json:"in_use"`
	ActiveOwners int             `json:"active_owners"`
	Owners       []OwnerSnapshot `json:"owners"`
}

// Snapshot is a point-in-time, lock-consistent view of every budget.
type Snapshot struct {
	Enabled  bool               `json:"enabled"`
	Resource []ResourceSnapshot `json:"resources"`
}

// Snapshot returns a copied, deterministic view safe to retain outside the
// registry lock (owners sorted by name for stable metrics/health output).
func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := Snapshot{Enabled: r.cfg.Enabled}
	keys := make([]Resource, 0, len(r.resources))
	for res := range r.resources {
		keys = append(keys, res)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, res := range keys {
		rb := r.resources[res]
		rb.ensureGuarantees()
		snap := ResourceSnapshot{
			Resource: res,
			Enabled:  rb.cfg.managed(r.cfg.Enabled),
			Capacity: rb.cfg.Capacity,
			InUse:    rb.total,
		}
		names := make([]tenant.Owner, 0, len(rb.owners)+1)
		for name := range rb.owners {
			names = append(names, name)
		}
		sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
		for _, name := range names {
			snap.Owners = append(snap.Owners, ownerSnapshot(rb.owners[name]))
		}
		if rb.other != nil {
			snap.Owners = append(snap.Owners, ownerSnapshot(rb.other))
		}
		snap.ActiveOwners = len(snap.Owners)
		out.Resource = append(out.Resource, snap)
	}
	return out
}

func ownerSnapshot(o *ownerState) OwnerSnapshot {
	s := OwnerSnapshot{
		Owner:     string(o.name),
		Guarantee: o.guarantee,
		InUse:     o.inUse,
		Borrowed:  max64(o.inUse-o.guarantee, 0),
		Admitted:  o.admitted,
		Failures:  int64(o.failures),
		Rejected:  make(map[Reason]int64, len(o.rejected)),
		Starved:   !o.starvedAt.IsZero(),
		LastSeen:  o.lastSeen,
	}
	for k, v := range o.rejected {
		s.Rejected[k] = v
	}
	if !o.backoffUntil.IsZero() {
		t := o.backoffUntil
		s.BackoffUntil = &t
	}
	return s
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
