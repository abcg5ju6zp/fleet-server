// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package action

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/sqn"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

func actionFairRegistry(t *testing.T) *budget.Registry {
	t.Helper()
	return budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       30 * time.Second,
		MaxTrackedOwners: 10,
		Backoff: budget.BackoffConfig{
			Initial:          20 * time.Millisecond,
			Max:              time.Second,
			FailureThreshold: 1,
		},
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceActionDispatch: {Enabled: true, Capacity: 4, GuaranteeMin: 1, GuaranteeMax: 4, BorrowMax: 4},
			budget.ResourceActionFanout:   {Enabled: true, Capacity: 4, GuaranteeMin: 1, GuaranteeMax: 4, BorrowMax: 4},
		},
	})
}

func newFairDispatcher(t *testing.T, reg *budget.Registry) *Dispatcher {
	t.Helper()
	return NewDispatcher(nil, 0, 100, nil, WithBudgets(reg))
}

// TestActionDispatchBackoffIsOwnerScoped: a fan-out round rejected for
// the backed-off owner is dropped without affecting another owner.
func TestActionDispatchBackoffIsOwnerScoped(t *testing.T) {
	reg := actionFairRegistry(t)
	d := newFairDispatcher(t, reg)
	log := zerolog.Nop()

	subA := d.Subscribe(log, "agent-a", sqn.SeqNo{0}, "space-a")
	subB := d.Subscribe(log, "agent-b", sqn.SeqNo{0}, "space-b")

	reg.ReportFailure(budget.ResourceActionDispatch, "space-a")

	d.dispatch(t.Context(), "agent-a", []model.Action{{ActionID: "act-a"}})
	d.dispatch(t.Context(), "agent-b", []model.Action{{ActionID: "act-b"}})

	select {
	case <-subA.Ch():
		t.Fatal("backed-off owner must not receive fan-out")
	default:
	}
	select {
	case got := <-subB.Ch():
		require.Len(t, got, 1)
		assert.Equal(t, "act-b", got[0].ActionID)
	default:
		t.Fatal("healthy owner must still receive fan-out")
	}
	assert.False(t, reg.IsBackedOff(budget.ResourceActionDispatch, "space-b"))
}

// TestActionFanoutDropsOnFullChannel: a slow consumer whose cap-1
// channel is already full loses the round without blocking the
// dispatcher; the next round delivers normally.
func TestActionFanoutDropsOnFullChannel(t *testing.T) {
	reg := actionFairRegistry(t)
	d := newFairDispatcher(t, reg)
	log := zerolog.Nop()
	sub := d.Subscribe(log, "agent-a", sqn.SeqNo{0}, tenant.Default)

	// First fan-out fills the channel.
	d.dispatch(t.Context(), "agent-a", []model.Action{{ActionID: "first"}})
	// Second is non-blocking dropped: the dispatcher never waits on
	// the slow agent.
	done := make(chan struct{})
	go func() {
		d.dispatch(t.Context(), "agent-a", []model.Action{{ActionID: "second"}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fan-out blocked on a full channel")
	}

	got := <-sub.Ch()
	require.Len(t, got, 1)
	assert.Equal(t, "first", got[0].ActionID)
}
