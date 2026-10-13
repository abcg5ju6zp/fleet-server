// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
)

// Namespace budget metric names are additive: no pre-existing metric name
// or type changes (FR-32). Cardinality is bounded server-side by the
// registry's max_tracked_namespaces; overflow owners are aggregated into
// the "_other" namespace label.
const (
	nsMetricBudget = "fleet_namespace_budget_"
	nsMetricCache  = "fleet_namespace_cache_"

	labelResource  = "resource"
	labelNamespace = "namespace"
	labelReason    = "reason"
)

var (
	nsMetricsOnce  sync.Once
	nsBudgetSource atomic.Pointer[budget.Registry]
	nsCacheSource  atomic.Value // cache.Cache carrying *cache.CacheT
)

// EnableNamespaceBudgetMetrics registers the per-namespace collectors with
// the process prometheus registry exactly once and binds them to the current
// budget registry / cache. Fleet.Run re-executions simply rebind the sources;
// collectors are never registered twice.
func EnableNamespaceBudgetMetrics(reg *budget.Registry, c cache.Cache) {
	nsMetricsOnce.Do(func() {
		registry.promReg.MustRegister(
			namespaceBudgetCollector{},
			namespaceCacheCollector{},
		)
	})
	if reg != nil {
		nsBudgetSource.Store(reg)
	}
	if c != nil {
		nsCacheSource.Store(c)
	}
}

type namespaceBudgetCollector struct{}

func (namespaceBudgetCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range nsBudgetDescs {
		ch <- d
	}
}

var nsBudgetDescs = map[string]*prometheus.Desc{
	"enabled":       prometheus.NewDesc(nsMetricBudget+"enabled", "1 when namespace fair-share budgets are enabled", nil, nil),
	"capacity":      prometheus.NewDesc(nsMetricBudget+"capacity", "Configured global capacity of the resource class", []string{labelResource}, nil),
	"active_owners": prometheus.NewDesc(nsMetricBudget+"active_owners", "Number of tracked active namespace owners on the resource", []string{labelResource}, nil),
	"in_use_total":  prometheus.NewDesc(nsMetricBudget+"in_use_total", "Total weighted units currently in use across owners on the resource", []string{labelResource}, nil),
	"in_use":        prometheus.NewDesc(nsMetricBudget+"in_use", "Weighted units currently in use by the namespace owner on the resource", []string{labelResource, labelNamespace}, nil),
	"guarantee":     prometheus.NewDesc(nsMetricBudget+"guarantee", "Guaranteed share of capacity currently assigned to the namespace owner", []string{labelResource, labelNamespace}, nil),
	"borrowed":      prometheus.NewDesc(nsMetricBudget+"borrowed", "Weighted units currently used beyond the owner's guarantee", []string{labelResource, labelNamespace}, nil),
	"admitted":      prometheus.NewDesc(nsMetricBudget+"admitted_total", "Cumulative admissions granted to the namespace owner", []string{labelResource, labelNamespace}, nil),
	"rejected":      prometheus.NewDesc(nsMetricBudget+"rejected_total", "Cumulative admissions rejected for the namespace owner, by reason", []string{labelResource, labelNamespace, labelReason}, nil),
	"failures":      prometheus.NewDesc(nsMetricBudget+"failures_total", "Consecutive operation failures recorded for the namespace owner", []string{labelResource, labelNamespace}, nil),
	"backoff":       prometheus.NewDesc(nsMetricBudget+"backoff_seconds", "Seconds remaining until the namespace owner's failure backoff clears", []string{labelResource, labelNamespace}, nil),
}

func (namespaceBudgetCollector) Collect(ch chan<- prometheus.Metric) {
	reg := nsBudgetSource.Load()
	if reg == nil {
		return
	}
	snap := reg.Snapshot()
	if snap.Enabled {
		ch <- prometheus.MustNewConstMetric(nsBudgetDescs["enabled"], prometheus.GaugeValue, 1)
	} else {
		ch <- prometheus.MustNewConstMetric(nsBudgetDescs["enabled"], prometheus.GaugeValue, 0)
	}

	for _, rs := range snap.Resource {
		res := string(rs.Resource)
		ch <- prometheus.MustNewConstMetric(nsBudgetDescs["capacity"], prometheus.GaugeValue, float64(rs.Capacity), res)
		ch <- prometheus.MustNewConstMetric(nsBudgetDescs["active_owners"], prometheus.GaugeValue, float64(rs.ActiveOwners), res)
		ch <- prometheus.MustNewConstMetric(nsBudgetDescs["in_use_total"], prometheus.GaugeValue, float64(rs.InUse), res)
		for _, os := range rs.Owners {
			ns := os.Owner
			ch <- prometheus.MustNewConstMetric(nsBudgetDescs["in_use"], prometheus.GaugeValue, float64(os.InUse), res, ns)
			ch <- prometheus.MustNewConstMetric(nsBudgetDescs["guarantee"], prometheus.GaugeValue, float64(os.Guarantee), res, ns)
			ch <- prometheus.MustNewConstMetric(nsBudgetDescs["borrowed"], prometheus.GaugeValue, float64(os.Borrowed), res, ns)
			ch <- prometheus.MustNewConstMetric(nsBudgetDescs["admitted"], prometheus.CounterValue, float64(os.Admitted), res, ns)
			ch <- prometheus.MustNewConstMetric(nsBudgetDescs["failures"], prometheus.CounterValue, float64(os.Failures), res, ns)
			for reason, n := range os.Rejected {
				ch <- prometheus.MustNewConstMetric(nsBudgetDescs["rejected"], prometheus.CounterValue, float64(n), res, ns, string(reason))
			}
			if os.BackoffUntil != nil {
				if secs := time.Until(*os.BackoffUntil).Seconds(); secs > 0 {
					ch <- prometheus.MustNewConstMetric(nsBudgetDescs["backoff"], prometheus.GaugeValue, secs, res, ns)
				}
			}
		}
	}
}

type namespaceCacheCollector struct{}

func (namespaceCacheCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range nsCacheDescs {
		ch <- d
	}
}

var nsCacheDescs = map[string]*prometheus.Desc{
	"sets":          prometheus.NewDesc(nsMetricCache+"sets_total", "Cumulative attributed entries admitted to the cache per namespace owner", []string{labelNamespace}, nil),
	"rejectedSets":  prometheus.NewDesc(nsMetricCache+"rejected_total", "Cumulative attributed cache sets rejected per namespace owner", []string{labelNamespace}, nil),
	"evicted":       prometheus.NewDesc(nsMetricCache+"evicted_total", "Cumulative entries evicted (policy or TTL) per namespace owner", []string{labelNamespace}, nil),
	"evictedCost":   prometheus.NewDesc(nsMetricCache+"evicted_cost_bytes_total", "Cumulative byte cost of entries evicted (policy or TTL) per namespace owner", []string{labelNamespace}, nil),
	"reclaimed":     prometheus.NewDesc(nsMetricCache+"reclaimed_total", "Cumulative entries reclaimed from the owner to honor another owner's guaranteed share", []string{labelNamespace}, nil),
	"reclaimedCost": prometheus.NewDesc(nsMetricCache+"reclaimed_cost_bytes_total", "Cumulative byte cost reclaimed from the owner to honor another owner's guaranteed share", []string{labelNamespace}, nil),
}

type cacheStatsProvider interface {
	BudgetCacheStats() cache.FairStatsSnapshot
}

func (namespaceCacheCollector) Collect(ch chan<- prometheus.Metric) {
	src, ok := nsCacheSource.Load().(cache.Cache)
	if !ok || src == nil {
		return
	}
	provider, ok := src.(cacheStatsProvider)
	if !ok {
		return
	}
	snap := provider.BudgetCacheStats()
	for owner, s := range snap.Owners {
		ns := string(owner)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["sets"], prometheus.CounterValue, float64(s.Sets), ns)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["rejectedSets"], prometheus.CounterValue, float64(s.Rejected), ns)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["evicted"], prometheus.CounterValue, float64(s.Evicted), ns)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["evictedCost"], prometheus.CounterValue, float64(s.EvictedCost), ns)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["reclaimed"], prometheus.CounterValue, float64(s.Reclaimed), ns)
		ch <- prometheus.MustNewConstMetric(nsCacheDescs["reclaimedCost"], prometheus.CounterValue, float64(s.ReclaimedCost), ns)
	}
}
