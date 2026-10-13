// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import (
	"sync"
	"testing"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func testConfig(capacity int64) Config {
	return Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Backoff: BackoffConfig{
			Initial:          time.Second,
			Max:              30 * time.Second,
			FailureThreshold: 2,
		},
		Resources: map[Resource]ResourceConfig{
			ResourceCheckin: {
				Enabled:      true,
				Capacity:     capacity,
				GuaranteeMin: 1,
				GuaranteeMax: capacity,
				BorrowMax:    capacity,
			},
		},
	}
}

func TestSingleOwnerGetsFullCapacity(t *testing.T) {
	r := New(testConfig(4), WithClock(newFakeClock()))

	for i := 0; i < 4; i++ {
		l, err := r.TryAcquire(ResourceCheckin, "a")
		require.NoError(t, err)
		defer l.Release()
	}
	// Fifth is rejected by the global ceiling.
	_, err := r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	rej, ok := As(err)
	require.True(t, ok)
	assert.Equal(t, ReasonMax, rej.Reason)

	snap := resourceSnap(t, r, ResourceCheckin)
	require.Len(t, snap.Owners, 1)
	assert.Equal(t, int64(4), snap.Capacity)
	assert.Equal(t, int64(4), snap.Owners[0].Guarantee)
	assert.Equal(t, int64(4), snap.Owners[0].InUse)
	assert.Equal(t, int64(0), snap.Owners[0].Borrowed, "sole owner uses guaranteed, never borrowed")
}

func TestBorrowAndReclaim(t *testing.T) {
	clk := newFakeClock()
	r := New(testConfig(4), WithClock(clk))

	// A is the only active owner and fills the resource by guarantee.
	leasesA := make([]Lease, 0, 4)
	for i := 0; i < 4; i++ {
		l, err := r.TryAcquire(ResourceCheckin, "a")
		require.NoError(t, err)
		leasesA = append(leasesA, l)
	}
	ga := resourceSnap(t, r, ResourceCheckin).Owners[0].Guarantee
	require.Equal(t, int64(4), ga)

	// B activates while the resource is full: rejected while below its
	// guarantee and records contention.
	_, err := r.TryAcquire(ResourceCheckin, "b")
	require.Error(t, err)
	assert.Equal(t, ReasonMax, mustReject(t, err).Reason)

	snap := resourceSnap(t, r, ResourceCheckin)
	require.Len(t, snap.Owners, 2)
	assert.Equal(t, int64(2), snap.Owners[0].Guarantee, "guarantees split evenly once B is active")
	bState := snap.Owners[1]
	require.Equal(t, "b", bState.Owner)
	assert.True(t, bState.Starved)

	// A's new admission is beyond its guarantee: borrow is blocked by the
	// contention marker.
	_, err = r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	assert.Equal(t, ReasonBorrow, mustReject(t, err).Reason)

	// One A lease drains: the freed slot must NOT go back to A while B is
	// contending below its guarantee.
	leasesA[0].Release()
	_, err = r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	assert.Equal(t, ReasonBorrow, mustReject(t, err).Reason)

	// B claims the reclaimed slot on its guarantee; contention clears.
	lb, err := r.TryAcquire(ResourceCheckin, "b")
	require.NoError(t, err)
	defer lb.Release()
	snap = resourceSnap(t, r, ResourceCheckin)
	bState = ownerSnap(t, snap, "b")
	assert.False(t, bState.Starved)

	// A cannot re-fill while B still holds the slot (global full).
	_, err = r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)

	// B stops contending (releases): A borrows the freed share again.
	lb.Release()
	l, err := r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	defer l.Release()
	assert.Equal(t, int64(4), resourceSnap(t, r, ResourceCheckin).InUse)
}

func TestContentionMarkerExpires(t *testing.T) {
	clk := newFakeClock()
	r := New(testConfig(4), WithClock(clk))

	a := acquireN(t, r, "a", 4)
	defer releaseAll(a)
	_, err := r.TryAcquire(ResourceCheckin, "b") // below guarantee, rejected, starves
	require.Error(t, err)
	clk.advance(31 * time.Second)
	r.gc() // reclaims the marker

	// A is full: global capacity rejects regardless of the stale marker.
	_, err = r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	assert.Equal(t, ReasonMax, mustReject(t, err).Reason)

	// After one A lease drains and B stays idle, A may borrow the slot
	// again once the marker expired.
	a[0].Release()
	l, err := r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	defer l.Release()
}

func TestDisabledResourceUnmanaged(t *testing.T) {
	cfg := Config{Enabled: false}
	r := New(cfg)
	leases := make([]Lease, 0, 20)
	for i := 0; i < 20; i++ {
		l, err := r.TryAcquire(ResourceCheckin, "a")
		require.NoError(t, err)
		leases = append(leases, l)
	}
	for _, l := range leases {
		l.Release()
	}
	assert.Empty(t, r.Snapshot().Resource)
}

func TestBackoff(t *testing.T) {
	clk := newFakeClock()
	r := New(testConfig(4), WithClock(clk))

	r.ReportFailure(ResourceCheckin, "a")
	l, err := r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err, "below threshold")
	l.Release()

	r.ReportFailure(ResourceCheckin, "a")
	_, err = r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	rej := mustReject(t, err)
	assert.Equal(t, ReasonBackoff, rej.Reason)
	assert.Equal(t, time.Second, rej.RetryAfter)

	// Other owners are unaffected.
	lb, err := r.TryAcquire(ResourceCheckin, "b")
	require.NoError(t, err)
	lb.Release()

	clk.advance(time.Second)
	l, err = r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	l.Release()

	// Success reports clear the failure state.
	r.ReportFailure(ResourceCheckin, "a")
	r.ReportSuccess(ResourceCheckin, "a")
	l, err = r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	l.Release()
}

func TestIdleGC(t *testing.T) {
	clk := newFakeClock()
	r := New(testConfig(4), WithClock(clk))

	l, err := r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	l.Release()
	require.Len(t, resourceSnap(t, r, ResourceCheckin).Owners, 1)

	clk.advance(61 * time.Second)
	r.gc()
	assert.Empty(t, resourceSnap(t, r, ResourceCheckin).Owners)
}

func TestReconfigurePreservesLeases(t *testing.T) {
	clk := newFakeClock()
	cfg := testConfig(4)
	r := New(cfg, WithClock(clk))
	leases := acquireN(t, r, "a", 2)
	defer releaseAll(leases)

	// Capacity shrink does not preempt; new admissions are rejected while
	// in-flight leases drain to the new ceiling.
	cfg2 := testConfig(2)
	r.Reconfigure(cfg2)
	_, err := r.TryAcquire(ResourceCheckin, "a")
	require.Error(t, err)
	assert.Equal(t, int64(2), resourceSnap(t, r, ResourceCheckin).InUse, "in-flight leases survive reconfigure")
	assert.Equal(t, int64(2), resourceSnap(t, r, ResourceCheckin).Capacity)

	// Growing again admits.
	r.Reconfigure(testConfig(4))
	l2, err := r.TryAcquire(ResourceCheckin, "a")
	require.NoError(t, err)
	l2.Release()
}

func TestMaxTrackedOwnersAggregated(t *testing.T) {
	cfg := testConfig(10)
	cfg.MaxTrackedOwners = 2
	r := New(cfg, WithClock(newFakeClock()))

	owners := []tenant.Owner{"a", "b", "c", "d"}
	leases := make([]Lease, 0, len(owners))
	for _, o := range owners {
		l, err := r.TryAcquire(ResourceCheckin, o)
		require.NoError(t, err)
		leases = append(leases, l)
	}
	defer releaseAll(leases)
	snap := resourceSnap(t, r, ResourceCheckin)
	assert.Len(t, snap.Owners, 3, "two tracked plus the aggregate bucket")
	assert.NotNil(t, ownerSnap(t, snap, OtherOwner))
	assert.Equal(t, int64(2), ownerSnap(t, snap, OtherOwner).InUse)
}

func TestExplicitGuaranteeOverride(t *testing.T) {
	cfg := testConfig(10)
	cfg.Overrides = map[string]OwnerOverride{"a": {Weight: 3}}
	r := New(cfg, WithClock(newFakeClock()))

	for _, o := range []tenant.Owner{"a", "b"} {
		l, err := r.TryAcquire(ResourceCheckin, o)
		require.NoError(t, err)
		defer l.Release()
	}
	snap := resourceSnap(t, r, ResourceCheckin)
	ga := ownerSnap(t, snap, "a").Guarantee
	gb := ownerSnap(t, snap, "b").Guarantee
	assert.Equal(t, int64(8), ga, "3:1 weighted share of 10")
	assert.Equal(t, int64(3), gb, "half rounded away from zero")
}

func TestConcurrentInvariant(t *testing.T) {
	r := New(testConfig(6), WithClock(newFakeClock()))

	var wg sync.WaitGroup
	for _, o := range []tenant.Owner{"a", "b", "c"} {
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 200; i++ {
					l, err := r.TryAcquire(ResourceCheckin, o)
					if err != nil {
						continue
					}
					time.Sleep(time.Millisecond)
					l.Release()
					l.Release() // idempotent
				}
			}()
		}
	}
	wg.Wait()
	snap := resourceSnap(t, r, ResourceCheckin)
	assert.Equal(t, int64(0), snap.InUse, "all leases returned")
}

// TestGuaranteeFloorProtectsSmallShare ensures clamps keep usable
// guarantees: four equal owners of ten round to 3 each (instead of 2),
// bounded by GuaranteeMax; a wider owner set hits GuaranteeMin.
func TestGuaranteeFloorAndCeiling(t *testing.T) {
	cfg := testConfig(10)
	cfg.Resources[ResourceCheckin] = ResourceConfig{
		Enabled: true, Capacity: 10, GuaranteeMin: 2, GuaranteeMax: 6, BorrowMax: 10,
	}
	r := New(cfg, WithClock(newFakeClock()))
	for _, o := range []tenant.Owner{"a", "b", "c", "d"} {
		l, err := r.TryAcquire(ResourceCheckin, o)
		require.NoError(t, err)
		defer l.Release()
	}
	snap := resourceSnap(t, r, ResourceCheckin)
	for _, o := range snap.Owners {
		assert.Equal(t, int64(3), o.Guarantee, "rounded weighted share of 10 across 4 owners")
	}

	for _, o := range []tenant.Owner{"e", "f", "g", "h", "i", "j"} {
		l, err := r.TryAcquire(ResourceCheckin, o)
		require.NoError(t, err)
		defer l.Release()
	}
	snap = resourceSnap(t, r, ResourceCheckin)
	for _, o := range snap.Owners {
		assert.GreaterOrEqual(t, o.Guarantee, int64(2), "floor clamp")
	}
}

func acquireN(t *testing.T, r *Registry, o tenant.Owner, n int) []Lease {
	t.Helper()
	out := make([]Lease, 0, n)
	for i := 0; i < n; i++ {
		l, err := r.TryAcquire(ResourceCheckin, o)
		require.NoError(t, err)
		out = append(out, l)
	}
	return out
}

func releaseAll(ls []Lease) {
	for _, l := range ls {
		l.Release()
	}
}

func mustReject(t *testing.T, err error) *RejectError {
	t.Helper()
	rej, ok := As(err)
	require.True(t, ok, "expected *RejectError, got %v", err)
	return rej
}

func resourceSnap(t *testing.T, r *Registry, res Resource) ResourceSnapshot {
	t.Helper()
	for _, s := range r.Snapshot().Resource {
		if s.Resource == res {
			return s
		}
	}
	t.Fatalf("resource %s missing", res)
	return ResourceSnapshot{}
}

func ownerSnap(t *testing.T, s ResourceSnapshot, o tenant.Owner) OwnerSnapshot {
	t.Helper()
	for _, os := range s.Owners {
		if tenant.Owner(os.Owner) == o {
			return os
		}
	}
	t.Fatalf("owner %s missing", o)
	return OwnerSnapshot{}
}
