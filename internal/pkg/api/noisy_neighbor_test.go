// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// TestNoisyNeighborAcrossHTTPResources is the httptest-scale end-to-end
// evidence for AC-16/AC-6: one namespace saturating check-in long polls and
// acking in a storm is the only owner ever rejected; another namespace keeps
// its guaranteed check-in admission, its acks succeed because resource
// budgets are independent, and disconnects return every slot to baseline.
func TestNoisyNeighborAcrossHTTPResources(t *testing.T) {
	const checkinCap int64 = 4
	reg := budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Backoff:          budget.BackoffConfig{Initial: time.Second, Max: time.Second, FailureThreshold: 10},
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCheckin: {
				Enabled:      true,
				Capacity:     checkinCap,
				GuaranteeMin: 1,
				GuaranteeMax: checkinCap,
				BorrowMax:    checkinCap,
			},
			budget.ResourceAck: {
				Enabled:      true,
				Capacity:     2,
				GuaranteeMin: 1,
				GuaranteeMax: 2,
				BorrowMax:    2,
			},
			budget.ResourceBulkDispatch: {
				Enabled:      true,
				Capacity:     4,
				GuaranteeMin: 1,
				GuaranteeMax: 4,
				BorrowMax:    4,
			},
		},
	})

	// Minimal post-authorization chain: identity comes from the trusted
	// X-Test-NS header (standing in for the authenticated agent's
	// namespaces); the same two-stage admission the real handlers use runs
	// before any work.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner := tenant.Owner(r.Header.Get("X-Test-NS"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/checkin"):
			ctx, lease, admitted := acquireTenantBudget(r, w, reg, budget.ResourceCheckin, owner)
			if !admitted {
				return
			}
			defer lease.Release()
			// Emulate the long poll: end when the client disconnects.
			<-ctx.Done()
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/acks"):
			_, lease, admitted := acquireTenantBudget(r, w, reg, budget.ResourceAck, owner)
			if !admitted {
				return
			}
			// Acks fan into a bulk dispatch; that budget is independent
			// from the HTTP concurrency budgets. Both slots are held for
			// the request's processing window, as in the real handler.
			bulkLease, err := reg.TryAcquire(budget.ResourceBulkDispatch, owner)
			if err != nil {
				lease.Release()
				http.Error(w, err.Error(), http.StatusTooManyRequests)
				return
			}
			select {
			case <-time.After(150 * time.Millisecond):
			case <-r.Context().Done():
			}
			bulkLease.Release()
			lease.Release()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	do := func(ns, path string, ctx context.Context) int {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, nil)
		req.Header.Set("X-Test-NS", ns)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// 1. A occupies the entire check-in capacity with cancellable long
	// polls (cancellable client contexts stand in for disconnecting
	// agents).
	var aPollWG sync.WaitGroup
	cancelA := make([]context.CancelFunc, 0, checkinCap)
	for range checkinCap {
		ctx, cancel := context.WithCancel(context.Background())
		cancelA = append(cancelA, cancel)
		aPollWG.Add(1)
		go func() {
			defer aPollWG.Done()
			do("ns-a", "/api/fleet/agents/a/checkin", ctx)
		}()
	}
	require.Eventually(t, func() bool {
		return ownerInUse(reg, budget.ResourceCheckin, "ns-a") == checkinCap
	}, 2*time.Second, 10*time.Millisecond, "A fills check-in")

	// 2. B's first check-in is rejected below its guarantee (global full);
	// it receives the same 429 a saturated deployment already returns.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.Equal(t, http.StatusTooManyRequests, do("ns-b", "/api/fleet/agents/b/checkin", ctx))
	}()
	require.Eventually(t, func() bool {
		return ownerRejects(reg, budget.ResourceCheckin, "ns-b", budget.ReasonMax) == 1
	}, time.Second, 5*time.Millisecond)

	// 3. A keeps bursting: beyond its now-halved guarantee its new polls are
	// rejected (borrow contention); responses are 429 and in-flight polls
	// are never preempted.
	var stormMu sync.Mutex
	aStormOK, aStorm429 := 0, 0
	var stormWG sync.WaitGroup
	for range 8 {
		stormWG.Add(1)
		go func() {
			defer stormWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			code := do("ns-a", "/api/fleet/agents/a/checkin", ctx)
			stormMu.Lock()
			switch code {
			case http.StatusTooManyRequests:
				aStorm429++
			case http.StatusOK:
				aStormOK++
			}
			stormMu.Unlock()
		}()
	}
	stormWG.Wait()
	assert.Equal(t, 0, aStormOK, "no new A long poll admitted over the guarantee")
	assert.Equal(t, 8, aStorm429)
	assert.Equal(t, checkinCap, ownerInUse(reg, budget.ResourceCheckin, "ns-a"),
		"the original A polls remain in flight (no preemption)")

	// 4. A's ack storm saturates the independent ack resource.
	ackOK, ack429 := 0, 0
	var ackMu sync.Mutex
	var ackWG sync.WaitGroup
	for range 4 {
		ackWG.Add(1)
		go func() {
			defer ackWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			code := do("ns-a", "/api/fleet/agents/a/acks", ctx)
			ackMu.Lock()
			if code == http.StatusOK {
				ackOK++
			} else {
				ack429++
			}
			ackMu.Unlock()
		}()
	}
	ackWG.Wait()
	assert.Equal(t, 2, ackOK)
	assert.Equal(t, 2, ack429)

	// 5. B's acks succeed while A still holds all check-in slots: budgets
	// are independent and B is not backed off.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.Equal(t, http.StatusOK, do("ns-b", "/api/fleet/agents/b/acks", ctx))
	}()
	assert.Equal(t, int64(0), ownerRejects(reg, budget.ResourceAck, "ns-b"), "B never rejected on ack")

	// 6. B's bulk work is admitted independently of check-in saturation.
	bl, err := reg.TryAcquire(budget.ResourceBulkDispatch, "ns-b")
	require.NoError(t, err)
	bl.Release()

	// 7. Reclaim: B retries its guaranteed check-in while one A poll
	// disconnects. B succeeds within a bounded time; no in-flight poll was
	// interrupted.
	var bWG sync.WaitGroup
	bWG.Add(1)
	go func() {
		defer bWG.Done()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			code := do("ns-b", "/api/fleet/agents/b/checkin", ctx)
			cancel()
			if code == http.StatusOK || code == 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Give B's first retry cycle a moment to park, then disconnect one A
	// long poll to return the borrowed slot. The canceled client goroutine
	// releases its WaitGroup count on exit.
	time.Sleep(150 * time.Millisecond)
	cancelA[0]()

	require.Eventually(t, func() bool {
		return ownerInUse(reg, budget.ResourceCheckin, "ns-b") == 1
	}, 2*time.Second, 20*time.Millisecond, "B obtains its guaranteed check-in as an A poll drains")

	// 8. While B holds a slot, A still cannot grow beyond its share: a new
	// A poll is 429; rejection remains precisely attributed to A.
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.Equal(t, http.StatusTooManyRequests, do("ns-a", "/api/fleet/agents/a/checkin", ctx))
	}()

	// Disconnect B and the remaining A polls; everything drains to zero.
	// (bWG's long-poll client is the last successful 500ms request; cancel
	// is implicit on its timeout.)
	for _, cancel := range cancelA[1:] {
		cancel()
	}
	aPollWG.Wait()
	bWG.Wait()

	assert.Eventually(t, func() bool {
		return ownerInUse(reg, budget.ResourceCheckin, "ns-a") == 0 &&
			ownerInUse(reg, budget.ResourceCheckin, "ns-b") == 0
	}, 2*time.Second, 20*time.Millisecond, "all check-in leases return after disconnects")

	// 9. Final attribution: rejections limited to the responsible owners,
	// B never backed off.
	snap := reg.Snapshot()
	var checkin budget.ResourceSnapshot
	for _, rs := range snap.Resource {
		if rs.Resource == budget.ResourceCheckin {
			checkin = rs
		}
	}
	for _, os := range checkin.Owners {
		switch os.Owner {
		case "ns-a":
			assert.Greater(t, os.Rejected[budget.ReasonBorrow]+os.Rejected[budget.ReasonMax], int64(0))
			assert.Zero(t, os.Rejected[budget.ReasonBackoff])
			assert.Nil(t, os.BackoffUntil, "A is not backed off without ES failures")
		case "ns-b":
			assert.GreaterOrEqual(t, os.Rejected[budget.ReasonMax], int64(1))
			assert.Zero(t, os.Rejected[budget.ReasonBackoff])
			assert.Nil(t, os.BackoffUntil)
		}
	}
}

func ownerInUse(reg *budget.Registry, res budget.Resource, owner tenant.Owner) int64 {
	for _, rs := range reg.Snapshot().Resource {
		if rs.Resource != res {
			continue
		}
		for _, os := range rs.Owners {
			if os.Owner == string(owner) {
				return os.InUse
			}
		}
	}
	return 0
}

// ownerRejects sums rejection counters for one owner; pass no reasons to
// sum every reason.
func ownerRejects(reg *budget.Registry, res budget.Resource, owner tenant.Owner, reasons ...budget.Reason) int64 {
	for _, rs := range reg.Snapshot().Resource {
		if rs.Resource != res {
			continue
		}
		for _, os := range rs.Owners {
			if os.Owner != string(owner) {
				continue
			}
			if len(reasons) == 0 {
				var total int64
				for _, n := range os.Rejected {
					total += n
				}
				return total
			}
			var total int64
			for _, reason := range reasons {
				total += os.Rejected[reason]
			}
			return total
		}
	}
	return 0
}
