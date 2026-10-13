// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

func tenantTestRegistry(t *testing.T, dispatch, flush int64, threshold int) *budget.Registry {
	t.Helper()
	cfg := budget.Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceBulkDispatch: {Enabled: true, Capacity: dispatch, GuaranteeMin: 1, GuaranteeMax: dispatch, BorrowMax: dispatch},
			budget.ResourceBulkFlush:    {Enabled: true, Capacity: flush, GuaranteeMin: 1, GuaranteeMax: flush, BorrowMax: flush},
		},
	}
	if threshold > 0 {
		cfg.Backoff = budget.BackoffConfig{
			Initial:          20 * time.Millisecond,
			Max:              time.Second,
			FailureThreshold: threshold,
		}
	} else {
		cfg.Backoff = budget.BackoffConfig{
			Initial:          time.Second,
			Max:              30 * time.Second,
			FailureThreshold: 5,
		}
	}
	return budget.New(cfg)
}

// runningTestBulker starts a bulker backed by the per-block 201 mock
// transport with a short flush timer so admitted dispatches complete and
// return their leases quickly.
func runningTestBulker(t *testing.T, reg *budget.Registry) (*Bulker, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBulker(&mockBulkTransport{}, nil,
		WithBudgets(reg),
		WithFlushInterval(20*time.Millisecond),
	)
	go func() { _ = b.Run(ctx) }()
	t.Cleanup(cancel)
	return b, cancel
}

func TestDispatchOwnerSnapshotImmuneToCancel(t *testing.T) {
	// Pre-enqueue cancel: the block is never queued and its owner
	// leases are returned synchronously with the canceled dispatch, so
	// a disconnected caller cannot hold namespace slots. The channel is
	// filled to make the abort branch deterministic (a free channel plus
	// an already-canceled ctx would let the select race).
	reg := tenantTestRegistry(t, 1, 1, 0)
	b := NewBulker(nil, nil, WithBudgets(reg), WithBlockQueueSize(1))
	b.ch <- &bulkT{ch: make(chan respT, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	owner := tenant.Owner("space-a")
	blk := b.newBlk(ActionCreate, optionsT{owner: owner})
	resp := b.dispatch(ctx, blk)
	assert.ErrorIs(t, resp.err, context.Canceled)

	// The owner was admitted against space-a and everything is released
	// with the canceled request without waiting for ES or the drain.
	assert.Eventually(t, func() bool {
		for _, rs := range reg.Snapshot().Resource {
			if rs.InUse != 0 {
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)
}

// TestTenantDispatchGate proves one owner flooding dispatch cannot block
// queue entry for another owner. Blocks are parked directly in the entry
// channel (no Run): the leases exist exactly for the request lifetime,
// which is what HTTP long polls do as well.
func TestTenantDispatchGate(t *testing.T) {
	reg := tenantTestRegistry(t, 2, 8, 0)
	b := NewBulker(nil, nil, WithBudgets(reg))

	// Admitted blocks are parked (no Run consumes them); their leases
	// are claimed directly from the registry, mirroring what dispatch
	// claims before the entry channel send.
	park := func(owner tenant.Owner) *bulkT {
		dl, err := reg.TryAcquire(budget.ResourceBulkDispatch, owner)
		require.NoError(t, err)
		fl, err := reg.TryAcquire(budget.ResourceBulkFlush, owner)
		require.NoError(t, err)
		blk := b.newBlk(ActionCreate, optionsT{owner: owner})
		blk.dispatchLease = dl
		blk.flushLease = fl
		return blk
	}
	// reject runs dispatch; a saturated pool rejects before the entry
	// select, so it returns synchronously.
	reject := func(owner tenant.Owner) error {
		blk := b.newBlk(ActionCreate, optionsT{owner: owner})
		_, _ = blk.buf.WriteString(`{"create":{"_index":"t","_id":"1"}},{"x":1},`)
		err := b.dispatch(context.Background(), blk).err
		b.freeBlk(blk)
		return err
	}

	a1 := park("space-a")
	a2 := park("space-a")
	assert.Equal(t, int64(4), inUse(reg))

	// Third A fails fast while B has never activated: A is at its
	// dynamic guarantee and the pool is full.
	err := reject("space-a")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTenantDispatchLimit))
	assert.Equal(t, int64(4), inUse(reg))

	// B's first attempt is below its (as-yet inactive) share with a full
	// pool: rejected, and it arms the contention marker.
	err = reject("space-b")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTenantDispatchLimit))

	// One A slot drains; B now claims its guarantee immediately.
	a1.releaseOwnerLeases()
	b1 := park("space-b")
	assert.Equal(t, int64(4), inUse(reg), "a2 and b1 each hold two leases")

	// Release everything; parked blocks are discarded with the test.
	a2.releaseOwnerLeases()
	b1.releaseOwnerLeases()
	require.Eventually(t, func() bool { return inUse(reg) == 0 },
		time.Second, 10*time.Millisecond)
}

func inUse(reg *budget.Registry) int64 {
	var n int64
	for _, rs := range reg.Snapshot().Resource {
		n += rs.InUse
	}
	return n
}

func TestDispatchOwnerBackoff(t *testing.T) {
	reg := tenantTestRegistry(t, 4, 8, 1) // single failure arms cooldown
	b, _ := runningTestBulker(t, reg)

	reg.ReportFailure(budget.ResourceBulkDispatch, "space-a")

	err := func() error {
		ctx := tenant.WithOwner(context.Background(), "space-a")
		_, e := b.Create(ctx, "test-index", "id-a", []byte(`{"x":1}`))
		return e
	}()
	require.Error(t, err)
	var tdl *tenantDispatchError
	require.ErrorAs(t, err, &tdl)
	assert.Equal(t, "space-a", string(tdl.Owner()))
	assert.Equal(t, budget.ReasonBackoff, tdl.Reason())
	assert.Greater(t, tdl.RetryAfter(), time.Duration(0))

	// Backoff is owner-scoped: B rides normally.
	ctxB := tenant.WithOwner(context.Background(), "space-b")
	_, errB := b.Create(ctxB, "test-index", "id-b", []byte(`{"x":1}`))
	require.NoError(t, errB)
}

// TestFailQueueReleasesOwnerLeases proves blocks rejected wholesale by an
// ES transport error return their dispatch/flush reservations.
func TestFailQueueReleasesOwnerLeases(t *testing.T) {
	reg := tenantTestRegistry(t, 8, 8, 0)
	b := NewBulker(nil, nil, WithBudgets(reg))

	// Construct the post-admission state directly (dispatch itself
	// blocks until Run drains a response, which this failure-path test
	// replaces with failQueue).
	admit := func(owner tenant.Owner) *bulkT {
		dl, err := reg.TryAcquire(budget.ResourceBulkDispatch, owner)
		require.NoError(t, err)
		fl, err := reg.TryAcquire(budget.ResourceBulkFlush, owner)
		require.NoError(t, err)
		blk := b.newBlk(ActionCreate, optionsT{owner: owner})
		blk.dispatchLease = dl
		blk.flushLease = fl
		return blk
	}
	blks := []*bulkT{admit("space-a"), admit("space-a"), admit("space-b")}

	q := queueT{ty: kQueueBulk}
	q.head = blks[0]
	blks[0].next = blks[1]
	blks[1].next = blks[2]
	failQueue(q, errors.New("boom"))

	for _, blk := range blks {
		select {
		case resp := <-blk.ch:
			assert.EqualError(t, resp.err, "boom")
		default:
			t.Fatal("expected failure on block response channel")
		}
	}
	for _, rs := range reg.Snapshot().Resource {
		assert.Equal(t, int64(0), rs.InUse, "leases returned after failQueue for %s", rs.Resource)
	}
}

// TestReportOwnerBatchFailureAttributesOwners covers whole-batch failure
// accounting and per-item success clearing: only owners that rode the
// failed batch arm cooldown, and a later success clears it.
func TestReportOwnerBatchFailureAttributesOwners(t *testing.T) {
	reg := tenantTestRegistry(t, 8, 8, 1)
	b := NewBulker(nil, nil, WithBudgets(reg))

	// Only a and b are in the flushed batch that failed; c owns a block
	// that was still waiting and never rode the request.
	q := queueT{ty: kQueueBulk}
	a := b.newBlk(ActionCreate, optionsT{owner: "space-a"})
	bb := b.newBlk(ActionCreate, optionsT{owner: "space-b"})
	c := b.newBlk(ActionCreate, optionsT{owner: "space-c"})
	a.next = bb
	q.head = a
	_ = c

	b.reportOwnerBatchFailure(q)

	assert.True(t, reg.IsBackedOff(budget.ResourceBulkDispatch, "space-a"))
	assert.True(t, reg.IsBackedOff(budget.ResourceBulkDispatch, "space-b"))
	assert.False(t, reg.IsBackedOff(budget.ResourceBulkDispatch, "space-c"))

	b.reportOwnerItem(a, 200)
	assert.False(t, reg.IsBackedOff(budget.ResourceBulkDispatch, "space-a"))
	assert.True(t, reg.IsBackedOff(budget.ResourceBulkDispatch, "space-b"), "B stays backed off")
}
