// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import (
	"testing"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func weightedConfig() Config {
	return Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Resources: map[Resource]ResourceConfig{
			ResourceCacheCost: {
				Enabled:   true,
				Capacity:  weightedCapacity,
				BorrowMax: weightedCapacity,
			},
		},
	}
}

const weightedCapacity int64 = 10000

// TryAcquireN charges arbitrary weights (bytes); weighted leases release the
// exact weight and contention/reclaim behaves like the unit-slot kernel.
func TestWeightedAcquireAndReclaim(t *testing.T) {
	clk := newFakeClock()
	r := New(weightedConfig(), WithClock(clk))

	la, err := r.TryAcquireN(ResourceCacheCost, "a", 6000)
	require.NoError(t, err)
	defer la.Release()

	snap := resourceSnap(t, r, ResourceCacheCost)
	assert.Equal(t, int64(10000), ownerSnap(t, snap, "a").Guarantee)
	assert.Equal(t, int64(6000), snap.InUse)

	// B activates with a 4000-byte guaranteed claim: both owners get 5000.
	lb, err := r.TryAcquireN(ResourceCacheCost, "b", 4000)
	require.NoError(t, err)
	defer lb.Release()
	snap = resourceSnap(t, r, ResourceCacheCost)
	assert.Equal(t, int64(5000), ownerSnap(t, snap, "b").Guarantee)
	assert.Equal(t, int64(10000), snap.InUse)

	// One byte more for B exceeds the pool; the rejection arms B's
	// contention marker.
	_, err = r.TryAcquireN(ResourceCacheCost, "b", 1)
	require.Error(t, err)
	assert.Equal(t, ReasonMax, mustReject(t, err).Reason)

	// A's new borrow is now blocked by B's contention marker.
	_, err = r.TryAcquireN(ResourceCacheCost, "a", 500)
	require.Error(t, err)
	assert.Equal(t, ReasonBorrow, mustReject(t, err).Reason)

	// B finishes and releases its weighted lease; once the contention
	// marker ages out (bounded reclaim lag), A can borrow again.
	lb.Release()
	clk.advance(31 * time.Second)
	lb2, err := r.TryAcquireN(ResourceCacheCost, "a", 3000)
	require.NoError(t, err)
	defer lb2.Release()
	snap = resourceSnap(t, r, ResourceCacheCost)
	assert.Equal(t, int64(9000), snap.InUse)
	// B is still in the active set until idle GC, so A's guarantee stays
	// at 5000 and 4000 of its 9000 are borrowed.
	assert.Equal(t, int64(4000), ownerSnap(t, snap, "a").Borrowed)
}

// Weighted releases subtract exactly the claimed weight.
func TestWeightedLeaseRelease(t *testing.T) {
	r := New(weightedConfig())

	l, err := r.TryAcquireN(ResourceCacheCost, "a", 3000)
	require.NoError(t, err)
	assert.Equal(t, int64(3000), resourceSnap(t, r, ResourceCacheCost).InUse)
	l.Release()
	l.Release() // idempotent
	assert.Equal(t, int64(0), resourceSnap(t, r, ResourceCacheCost).InUse)
}

// An entry heavier than the whole capacity is admitted against
// capacity-sized head room when the pool is otherwise empty, matching the
// cache's treatment of oversized entries; the lease keeps its true weight.
func TestWeightedOversizedAdmission(t *testing.T) {
	r := New(weightedConfig())

	l, err := r.TryAcquireN(ResourceCacheCost, "a", 15000)
	require.NoError(t, err)
	snap := resourceSnap(t, r, ResourceCacheCost)
	assert.Equal(t, int64(15000), snap.InUse)
	l.Release()
	assert.Equal(t, int64(0), resourceSnap(t, r, ResourceCacheCost).InUse)

	// Oversized admission is refused while other owners hold capacity.
	lo, err := r.TryAcquireN(ResourceCacheCost, "b", 4000)
	require.NoError(t, err)
	defer lo.Release()
	_, err = r.TryAcquireN(ResourceCacheCost, "a", 15000)
	require.Error(t, err)
}

// Zero-weight admissions are untracked no-ops.
func TestWeightedZeroIsNoop(t *testing.T) {
	r := New(weightedConfig())

	l, err := r.TryAcquireN(ResourceCacheCost, tenant.Default, 0)
	require.NoError(t, err)
	l.Release()
	assert.Empty(t, resourceSnap(t, r, ResourceCacheCost).Owners)
}
