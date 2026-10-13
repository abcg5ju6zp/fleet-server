// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

func testMetricsRegistry() *budget.Registry {
	return budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Hour,
		ReclaimTTL:       time.Minute,
		MaxTrackedOwners: 10,
		Backoff: budget.BackoffConfig{
			Initial:          time.Second,
			Max:              30 * time.Second,
			FailureThreshold: 2,
		},
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCheckin: {
				Enabled:      true,
				Capacity:     4,
				GuaranteeMin: 1,
				GuaranteeMax: 4,
				BorrowMax:    4,
			},
			budget.ResourceCacheCost: {
				Enabled:   true,
				Capacity:  10000,
				BorrowMax: 10000,
			},
		},
	})
}

// metricFamily is a tiny helper view over gathered prometheus families.
func family(t *testing.T, name string) map[string]float64 {
	t.Helper()
	mfs, err := registry.promReg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			lps := m.GetLabel()
			parts := make([]string, 0, len(lps))
			for _, lp := range lps {
				parts = append(parts, lp.GetName()+"="+lp.GetValue())
			}
			sort.Strings(parts)
			key := strings.Join(parts, ",")
			if m.GetGauge() != nil {
				out[key] = m.GetGauge().GetValue()
			} else if m.GetCounter() != nil {
				out[key] = m.GetCounter().GetValue()
			}
		}
	}
	return out
}

// TR-12.1: synthetic A-borrow/reject/backoff + B-healthy load shows up with
// the right resource/namespace/reason labels and values.
func TestNamespaceBudgetMetrics(t *testing.T) {
	reg := testMetricsRegistry()

	// A fills the checkin resource while it is the sole owner.
	aLeases := make([]budget.Lease, 0, 4)
	for range 4 {
		l, err := reg.TryAcquire(budget.ResourceCheckin, "ns-a")
		require.NoError(t, err)
		aLeases = append(aLeases, l)
	}
	// B activation is rejected once below its guarantee (global full),
	// which arms the contention marker.
	_, err := reg.TryAcquire(budget.ResourceCheckin, "ns-b")
	require.Error(t, err)

	// While B stays starved, A cannot borrow idle capacity: its new
	// admission is rejected with reason=borrow. A also records failures
	// that drive its per-owner backoff.
	_, err = reg.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.Error(t, err)
	reg.ReportFailure(budget.ResourceCheckin, "ns-a")
	reg.ReportFailure(budget.ResourceCheckin, "ns-a")

	// Free one A slot; B then takes its guaranteed share and clears the
	// contention marker on progress.
	aLeases[0].Release()
	lb, err := reg.TryAcquire(budget.ResourceCheckin, "ns-b")
	require.NoError(t, err)
	defer lb.Release()

	EnableNamespaceBudgetMetrics(reg, nil)

	inUse := family(t, "fleet_namespace_budget_in_use")
	assert.Equal(t, float64(3), inUse["namespace=ns-a,resource=checkin"])
	assert.Equal(t, float64(1), inUse["namespace=ns-b,resource=checkin"])

	rejected := family(t, "fleet_namespace_budget_rejected_total")
	assert.Equal(t, float64(1), rejected["namespace=ns-b,reason=max,resource=checkin"])
	assert.GreaterOrEqual(t, rejected["namespace=ns-a,reason=borrow,resource=checkin"], float64(1))

	backoff := family(t, "fleet_namespace_budget_backoff_seconds")
	assert.Greater(t, backoff["namespace=ns-a,resource=checkin"], 0.0)

	failures := family(t, "fleet_namespace_budget_failures_total")
	assert.Equal(t, float64(2), failures["namespace=ns-a,resource=checkin"])

	enabled := family(t, "fleet_namespace_budget_enabled")
	assert.Equal(t, float64(1), enabled[""])
}

const testArtifactIdent = "art"

// Cache-level per-owner metrics: B's guaranteed writes reclaim A's borrowed
// entries and the eviction/reclaim counters reflect it. Ristretto evicts
// asynchronously and charges a small internal per-entry cost, so assertions
// tolerate that bounded jitter and wait for convergence.
func TestNamespaceCacheMetrics(t *testing.T) {
	const cap int64 = 100000
	reg := budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Hour,
		ReclaimTTL:       time.Minute,
		MaxTrackedOwners: 10,
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCacheCost: {Enabled: true, Capacity: cap, BorrowMax: cap},
		},
	})
	cfg := config.Cache{NumCounters: 1000000, MaxCost: cap}
	c, err := cache.New(cfg, cache.WithBudgets(reg))
	require.NoError(t, err)
	defer func() { _ = c.Reconfigure(cfg) }()

	a := tenant.Owner("ns-a")
	for i := range 10 {
		c.SetArtifact(model.Artifact{Identifier: testArtifactIdent, DecodedSha256: "a" + fmt.Sprintf("%02d", i), Body: make([]byte, 10000)}, a)
	}
	b := tenant.Owner("ns-b")
	for i := range 5 {
		c.SetArtifact(model.Artifact{Identifier: testArtifactIdent, DecodedSha256: "b" + fmt.Sprintf("%02d", i), Body: make([]byte, 10000)}, b)
	}

	EnableNamespaceBudgetMetrics(reg, c)

	assert.Eventually(t, func() bool {
		return family(t, "fleet_namespace_cache_reclaimed_cost_bytes_total")["namespace=ns-a"] >= 30000
	}, 2*time.Second, 10*time.Millisecond, "A borrowed bytes should be reclaimed for B")

	reclaimed := family(t, "fleet_namespace_cache_reclaimed_cost_bytes_total")["namespace=ns-a"]
	assert.LessOrEqual(t, reclaimed, float64(60000))
	sets := family(t, "fleet_namespace_cache_sets_total")
	assert.Equal(t, float64(10), sets["namespace=ns-a"])
	assert.GreaterOrEqual(t, sets["namespace=ns-b"], float64(4))
}

// TR-12.2: pre-existing metric families remain exposed under their original
// names; namespace metrics are strictly additive.
func TestExistingMetricFamiliesUnchanged(t *testing.T) {
	reg := testMetricsRegistry()
	EnableNamespaceBudgetMetrics(reg, nil)

	// Pre-existing directly-registered gauges/counters always scrape.
	mfs, err := registry.promReg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	assert.True(t, names["http_server_tcp_open"])
	assert.True(t, names["http_server_routes_checkin_active"])

	// Collector families only appear in a scrape when they carry at least
	// one sample, so assert their descriptors are registered instead.
	described := map[string]bool{}
	ch := make(chan *prometheus.Desc, 64)
	namespaceBudgetCollector{}.Describe(ch)
	namespaceCacheCollector{}.Describe(ch)
	close(ch)
	re := regexp.MustCompile(`fqName: "([^"]+)"`)
	for d := range ch {
		if m := re.FindStringSubmatch(d.String()); m != nil {
			described[m[1]] = true
		}
	}
	assert.True(t, described["fleet_namespace_budget_in_use"])
	assert.True(t, described["fleet_namespace_budget_rejected_total"])
	assert.True(t, described["fleet_namespace_cache_reclaimed_total"])
}

// When budgets are disabled the namespace collectors expose enabled=0 and no
// per-owner series.
func TestNamespaceMetricsDisabled(t *testing.T) {
	reg := budget.New(budget.Config{
		Enabled:          false,
		IdleTTL:          time.Hour,
		ReclaimTTL:       time.Minute,
		MaxTrackedOwners: 10,
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCheckin: {Enabled: true, Capacity: 4, BorrowMax: 4},
		},
	})
	EnableNamespaceBudgetMetrics(reg, nil)
	assert.Equal(t, float64(0), family(t, "fleet_namespace_budget_enabled")[""])
}
