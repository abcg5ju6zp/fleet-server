// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package policy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

const (
	testFairOwnerA = "space-a"
	testFairOwnerB = "space-b"
)

func TestPopFairAlternatesOwners(t *testing.T) {
	q := makeHead()
	q.pushBack(NewSub("p", "agent-1", 0, testFairOwnerA))
	q.pushBack(NewSub("p", "agent-2", 0, testFairOwnerA))
	q.pushBack(NewSub("p", "agent-3", 0, testFairOwnerB))
	lastServed := map[tenant.Owner]int64{}
	var got []tenant.Owner
	var seq int64
	for {
		s := q.popFair(nil, lastServed, nil)
		if s == nil {
			break
		}
		seq++
		lastServed[s.owner] = seq
		got = append(got, s.owner)
	}
	// First A, then B (never served beats A's credit), then the
	// remaining A: B's single update is delivered within a bounded
	// number of rounds even while A fills the queue.
	assert.Equal(t, []tenant.Owner{testFairOwnerA, testFairOwnerB, testFairOwnerA}, got)
}

func TestPopFairSkipsRejectedOwnersAndCloudPriority(t *testing.T) {
	q := makeHead()
	q.pushBack(NewSub("p", "a1", 0, testFairOwnerA))
	q.pushBack(NewSub("p", "b1", 0, testFairOwnerB))
	cloud := map[string]bool{cloudPolicyID: true}
	q.pushBack(NewSub(cloudPolicyID, "cloud-1", 0, testFairOwnerA))

	// Cloud policy jumps ahead regardless of queue position.
	s := q.popFair(cloud, map[tenant.Owner]int64{}, nil)
	require.NotNil(t, s)
	assert.Equal(t, cloudPolicyID, s.policyID)

	// B is skipped this round: only A remains eligible.
	s = q.popFair(cloud, map[tenant.Owner]int64{testFairOwnerA: 5, testFairOwnerB: 1}, map[tenant.Owner]struct{}{testFairOwnerB: {}})
	require.NotNil(t, s)
	assert.Equal(t, testFairOwnerA, string(s.owner))

	s = q.popFair(cloud, map[tenant.Owner]int64{testFairOwnerA: 5, testFairOwnerB: 1}, map[tenant.Owner]struct{}{testFairOwnerB: {}})
	assert.Nil(t, s, "only skipped owners are left")
}

// TestDispatchPendingBudgetFair proves the scheduler alternates owners
// across queued subscriptions through the budget layer, and that an owner
// rejected by its budget is deferred without disappearing (no
// 4xx/5xx: the subscription stays pending for the next deploy round).
func TestDispatchPendingBudgetFair(t *testing.T) {
	reg := budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourcePolicyDispatch: {Enabled: true, Capacity: 1, GuaranteeMin: 1, GuaranteeMax: 1, BorrowMax: 1},
		},
	})
	m := NewMonitor(nil, nil, config.ServerLimits{}, WithBudgets(reg))
	mon, ok := m.(*monitorT)
	require.True(t, ok)
	mon.policies["p"] = policyT{pp: ParsedPolicy{Policy: model.Policy{
		PolicyID: "p",
		Data: &model.PolicyData{
			Agent: map[string]any{},
			Fleet: map[string]any{},
		},
	}}}

	owners := []tenant.Owner{testFairOwnerA, testFairOwnerA, testFairOwnerB}
	var mut sync.Mutex
	var got []tenant.Owner
	ready := make(chan struct{}, len(owners))
	subs := make([]*subT, len(owners))
	for i, owner := range owners {
		subs[i] = NewSub("p", "agent-"+string(rune('1'+i)), 0, owner)
		mon.pendingQ.pushBack(subs[i])
		go func(s *subT) {
			<-s.ch
			mut.Lock()
			got = append(got, s.owner)
			mut.Unlock()
			ready <- struct{}{}
		}(subs[i])
	}

	mon.dispatchPending(context.Background())

	for range owners {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for policy dispatch")
		}
	}
	assert.Equal(t, []tenant.Owner{testFairOwnerA, testFairOwnerB, testFairOwnerA}, got,
		"queued policy updates alternate owners instead of A draining the queue")
}
