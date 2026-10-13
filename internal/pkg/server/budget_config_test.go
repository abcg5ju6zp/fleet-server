// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
)

func testBudgetServerConfig(enabled bool) *config.Config {
	nb := config.ServerNamespaceBudgets{
		Enabled:           enabled,
		GuaranteeMin:      1,
		GuaranteeMax:      90,
		BorrowMaxRatio:    0.5,
		IdleTTL:           time.Minute,
		ReclaimTTL:        2 * time.Second,
		MaxTrackedNamespaces: 10,
		Backoff: config.NamespaceBackoff{
			Initial:          time.Second,
			Max:              10 * time.Second,
			FailureThreshold: 3,
		},
		Resources: map[string]config.NamespaceResourceBudget{
			"checkin": {GuaranteeMin: 4, GuaranteeMax: 40, BorrowRatio: 0.25},
		},
		Overrides: map[string]config.NamespaceBudgetOverride{
			"prod": {Weight: 3},
		},
	}
	return &config.Config{
		Inputs: []config.Input{{
			Server: config.Server{
				Limits: config.ServerLimits{
					CheckinLimit: config.Limit{Max: 100},
					AckLimit:     config.Limit{Max: 0}, // unlimited → unmanaged
				},
				Bulk: config.ServerBulk{FlushMaxPending: 8},
				NamespaceBudgets: nb,
			},
			Cache: config.Cache{MaxCost: 4096},
		}},
	}
}

func TestBuildBudgetConfig(t *testing.T) {
	cfg := buildBudgetConfig(testBudgetServerConfig(true))
	require.True(t, cfg.Enabled)
	require.Contains(t, cfg.Resources, budget.ResourceCheckin)

	rc := cfg.Resources[budget.ResourceCheckin]
	assert.Equal(t, int64(100), rc.Capacity)
	assert.Equal(t, int64(4), rc.GuaranteeMin, "resource override")
	assert.Equal(t, int64(40), rc.GuaranteeMax, "resource override")
	assert.Equal(t, int64(25), rc.BorrowMax, "ratio maps to slots")

	assert.Equal(t, int64(32), cfg.Resources[budget.ResourceBulkDispatch].Capacity)
	assert.Equal(t, int64(8), cfg.Resources[budget.ResourceBulkFlush].Capacity)
	assert.Equal(t, int64(8), cfg.Resources[budget.ResourcePolicyDispatch].Capacity)
	assert.Equal(t, int64(8), cfg.Resources[budget.ResourceActionDispatch].Capacity)
	assert.Equal(t, int64(4096), cfg.Resources[budget.ResourceCacheCost].Capacity)

	_, ok := cfg.Resources[budget.ResourceAck]
	assert.False(t, ok, "zero global capacity stays unmanaged")

	assert.Equal(t, 3.0, cfg.Overrides["prod"].Weight)
}

func TestBuildBudgetConfigDisabled(t *testing.T) {
	cfg := buildBudgetConfig(testBudgetServerConfig(false))
	assert.False(t, cfg.Enabled)
	// Resources may be present but are inert because admission checks the
	// master switch first.
	assert.False(t, cfg.Enabled)
	for _, rc := range cfg.Resources {
		assert.False(t, rc.Enabled)
	}
}

func TestConfigChangedBudgets(t *testing.T) {
	cur := testBudgetServerConfig(true)
	assert.True(t, configChangedBudgets(nil, cur))
	next := testBudgetServerConfig(true)
	assert.False(t, configChangedBudgets(cur, next), "value-equal configs do not trigger")

	next.Inputs[0].Server.NamespaceBudgets.ReclaimTTL = time.Hour
	assert.True(t, configChangedBudgets(cur, next))
}

// TestBudgetRegistryReconfigureSurvivesLeases is the unit-level evidence for
// the "in-flight accounting survives server rebuild" requirement: the
// Fleet wiring holds one Registry pointer and only calls Reconfigure when
// the budget section changes.
func TestBudgetRegistryReconfigureSurvivesLeases(t *testing.T) {
	cfg := buildBudgetConfig(testBudgetServerConfig(true))
	r := budget.New(cfg)

	l, err := r.TryAcquire(budget.ResourceCheckin, "a")
	require.NoError(t, err)
	defer l.Release()

	next := buildBudgetConfig(testBudgetServerConfig(true))
	checkin := next.Resources[budget.ResourceCheckin]
	checkin.Capacity = 200
	next.Resources[budget.ResourceCheckin] = checkin
	r.Reconfigure(next)

	snap := r.Snapshot()
	require.NotEmpty(t, snap.Resource)
	for _, rs := range snap.Resource {
		if rs.Resource == budget.ResourceCheckin {
			require.NotEmpty(t, rs.Owners)
			assert.Equal(t, int64(1), rs.InUse, "lease carried across reconfiguration")
			assert.Equal(t, int64(200), rs.Capacity)
			return
		}
	}
	t.Fatal("checkin resource missing from snapshot")
}

// TestBudgetRestartBaseline is the restart-side evidence for AC-14: a
// freshly constructed registry (the post-restart process state) rebuilds
// deterministically from configuration with zero occupancy, borrowing,
// rejection and backoff state, while the effective capacity baseline stays
// explainable through the snapshot.
func TestBudgetRestartBaseline(t *testing.T) {
	cfg := buildBudgetConfig(testBudgetServerConfig(true))

	// Simulate the previous process: admit, reject and back off some
	// owners on the old registry, which is then discarded.
	prev := budget.New(cfg)
	l, err := prev.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.NoError(t, err)
	l.Release()
	prev.ReportFailure(budget.ResourceCheckin, "ns-a")
	prev.ReportFailure(budget.ResourceCheckin, "ns-a")
	prev.ReportFailure(budget.ResourceCheckin, "ns-a")
	_, err = prev.TryAcquire(budget.ResourceBulkDispatch, "ns-b")
	require.NoError(t, err)

	// Process restart: a new registry builds from the same config.
	fresh := budget.New(cfg)
	snap := fresh.Snapshot()
	require.True(t, snap.Enabled)
	require.NotEmpty(t, snap.Resource, "effective resource baseline is explainable")
	for _, rs := range snap.Resource {
		assert.Equal(t, int64(0), rs.InUse, "%s starts empty", rs.Resource)
		assert.Equal(t, 0, rs.ActiveOwners, "%s starts without owners", rs.Resource)
		assert.Equal(t, cfg.Resources[rs.Resource].Capacity, rs.Capacity, "%s capacity rebuilt from config", rs.Resource)
		for _, os := range rs.Owners {
			assert.Equal(t, int64(0), os.InUse, "%s/%s", rs.Resource, os.Owner)
			assert.Equal(t, int64(0), os.Borrowed)
			assert.Equal(t, int64(0), os.Admitted)
			assert.Nil(t, os.BackoffUntil)
		}
	}
}
