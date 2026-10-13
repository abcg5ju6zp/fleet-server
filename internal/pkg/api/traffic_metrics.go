// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elastic/elastic-agent-libs/monitoring"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

// TrafficStageStatus is the debug/metrics view of one class gate.
type TrafficStageStatus struct {
	Stage    string           `json:"stage"`
	Enabled  bool             `json:"enabled"`
	Snapshot traffic.Snapshot `json:"snapshot"`
	Stats    traffic.Stats    `json:"stats"`
}

// TrafficReloadStatus reports the last traffic_priority reload outcome.
type TrafficReloadStatus struct {
	OK      bool      `json:"ok"`
	At      time.Time `json:"at"`
	Error   string    `json:"error,omitempty"`
	Version int64     `json:"version"`
}

// TrafficStatus is the full internal debug snapshot.
type TrafficStatus struct {
	Enabled bool                  `json:"enabled"`
	Stages  []TrafficStageStatus  `json:"stages"`
	Bulk    bulk.BulkTrafficStats `json:"bulk"`
	Reload  TrafficReloadStatus   `json:"reload"`
}

// TrafficStatusProvider is implemented by the fleet server wiring.
type TrafficStatusProvider interface {
	TrafficStatus() TrafficStatus
}

var (
	trafficInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_inflight",
		Help:      "In-flight permits held at each traffic stage, by class.",
	}, []string{"stage", "class"})
	trafficWaiting = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_waiting",
		Help:      "Requests waiting for a permit at each stage, by class.",
	}, []string{"stage", "class"})
	trafficBorrowed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_borrowed",
		Help:      "Permits borrowed from the other class share, by class.",
	}, []string{"stage", "class"})
	trafficAdmitted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_admitted_total",
		Help:      "Total permits admitted by stage, class and mode.",
	}, []string{"stage", "class", "mode"})
	trafficRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_rejected_total",
		Help:      "Total rejected/timed-out/canceled waiters by stage, class and reason.",
	}, []string{"stage", "class", "reason"})
	trafficWaitSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_wait_seconds",
		Help:      "Time spent waiting for a permit, by stage and class.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"stage", "class"})
	trafficBulkQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_bulk_queue_depth",
		Help:      "Items queued in bulk engine queues, by queue and class.",
	}, []string{"queue", "class"})
	trafficReloadOK = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_config_last_reload_successful",
		Help:      "1 if the last traffic_priority reload succeeded, 0 otherwise.",
	})
	trafficReloadTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_config_last_reload_timestamp_seconds",
		Help:      "Unix timestamp of the last traffic_priority reload attempt.",
	})
	trafficBulkDispatchRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_bulk_dispatch_rejected_total",
		Help:      "Total background bulk dispatches rejected above the pending-dispatch cap.",
	})
	trafficBulkQueueBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "http_server",
		Name:      "traffic_priority_bulk_queue_bytes",
		Help:      "Bytes queued in bulk engine queues.",
	}, []string{"queue"})
)

// trafficLibbeatReg mirrors scalar totals into the libbeat /stats tree.
var trafficLibbeatReg *monitoring.Registry

func init() {
	registry.promReg.MustRegister(
		trafficInflight,
		trafficWaiting,
		trafficBorrowed,
		trafficAdmitted,
		trafficRejected,
		trafficWaitSeconds,
		trafficBulkQueueDepth,
		trafficBulkQueueBytes,
		trafficBulkDispatchRejected,
		trafficReloadOK,
		trafficReloadTimestamp,
	)
	trafficLibbeatReg = registry.registry.GetOrCreateRegistry("traffic_priority")
}

// gateMetricsObserver adapts gate lifecycle events to labeled Prometheus
// metrics and scalar libbeat gauges.
type gateMetricsObserver struct {
	stage          string
	inflight       [2]*monitoring.Uint
	waiting        [2]*monitoring.Uint
	borrowed       [2]*monitoring.Uint
	rejectedTO     [2]*monitoring.Uint
	rejectedCap    [2]*monitoring.Uint
	rejectedCancel [2]*monitoring.Uint
}

func newGateMetricsObserver(stage string) *gateMetricsObserver {
	o := &gateMetricsObserver{stage: stage}
	for _, class := range []traffic.Class{traffic.Control, traffic.Background} {
		reg := trafficLibbeatReg.GetOrCreateRegistry(stage).GetOrCreateRegistry(class.String())
		o.inflight[class] = monitoring.NewUint(reg, "inflight")
		o.waiting[class] = monitoring.NewUint(reg, "waiting")
		o.borrowed[class] = monitoring.NewUint(reg, "borrowed")
		o.rejectedTO[class] = monitoring.NewUint(reg, "rejected_timeout")
		o.rejectedCap[class] = monitoring.NewUint(reg, "rejected_waiters")
		o.rejectedCancel[class] = monitoring.NewUint(reg, "rejected_canceled")
	}
	return o
}

func admitMode(class traffic.Class, borrowed bool) string {
	switch {
	case borrowed:
		return "borrowed"
	case class == traffic.Background:
		return "floor"
	default:
		return "reserved"
	}
}

func (o *gateMetricsObserver) Admitted(class traffic.Class, borrowed bool, waited time.Duration) {
	labels := prometheus.Labels{"stage": o.stage, "class": class.String()}
	trafficInflight.With(labels).Inc()
	trafficAdmitted.With(prometheus.Labels{
		"stage": o.stage, "class": class.String(), "mode": admitMode(class, borrowed),
	}).Inc()
	trafficWaitSeconds.With(labels).Observe(waited.Seconds())
	if borrowed {
		trafficBorrowed.With(labels).Inc()
		o.borrowed[class].Inc()
	}
	o.inflight[class].Inc()
}

func (o *gateMetricsObserver) Released(class traffic.Class, borrowed bool) {
	labels := prometheus.Labels{"stage": o.stage, "class": class.String()}
	trafficInflight.With(labels).Dec()
	if borrowed {
		trafficBorrowed.With(labels).Dec()
		if o.borrowed[class].Get() > 0 {
			o.borrowed[class].Sub(1)
		}
	}
	if o.inflight[class].Get() > 0 {
		o.inflight[class].Sub(1)
	}
}

func (o *gateMetricsObserver) Rejected(class traffic.Class, reason string) {
	trafficRejected.With(prometheus.Labels{
		"stage": o.stage, "class": class.String(), "reason": reason,
	}).Inc()
	switch reason {
	case traffic.ReasonTimeout:
		o.rejectedTO[class].Inc()
	case traffic.ReasonWaiters:
		o.rejectedCap[class].Inc()
	default:
		o.rejectedCancel[class].Inc()
	}
}

func (o *gateMetricsObserver) Queued(class traffic.Class, delta int) {
	trafficWaiting.With(prometheus.Labels{
		"stage": o.stage, "class": class.String(),
	}).Add(float64(delta))
	g := o.waiting[class]
	if delta > 0 {
		g.Add(uint64(delta))
	} else if g.Get() >= uint64(-delta) {
		g.Sub(uint64(-delta))
	}
}

// IncBackgroundDispatchRejections increments the bulk dispatch rejection
// counter; wired to the bulker via a callback to avoid an import cycle.
func IncBackgroundDispatchRejections() {
	trafficBulkDispatchRejected.Inc()
}

// BindTrafficMetrics installs metrics observers on the per-stage gates.
// Stages: "entry", "bulk_flush", "es".
func BindTrafficMetrics(stages map[string]*traffic.Gate) {
	for stage, gate := range stages {
		if gate != nil {
			gate.SetObserver(newGateMetricsObserver(stage))
		}
	}
}

// RunTrafficMetricsSampler periodically publishes gauges that only exist as
// point-in-time snapshots: bulk queue depths and the reload outcome.
func RunTrafficMetricsSampler(ctx context.Context, interval time.Duration, provider TrafficStatusProvider) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sample := func() {
		status := provider.TrafficStatus()
		for _, q := range status.Bulk.Queues {
			bg := q.Items - q.ControlItems
			if bg < 0 {
				bg = 0
			}
			trafficBulkQueueDepth.With(prometheus.Labels{"queue": q.Queue, "class": "control"}).
				Set(float64(q.ControlItems))
			trafficBulkQueueDepth.With(prometheus.Labels{"queue": q.Queue, "class": "background"}).
				Set(float64(bg))
			trafficBulkQueueBytes.With(prometheus.Labels{"queue": q.Queue}).
				Set(float64(q.Bytes))
		}
		if status.Reload.OK {
			trafficReloadOK.Set(1)
		} else {
			trafficReloadOK.Set(0)
		}
		if !status.Reload.At.IsZero() {
			trafficReloadTimestamp.Set(float64(status.Reload.At.UnixNano()) / 1e9)
		}
	}
	sample()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample()
		}
	}
}

// trafficPriorityHandler serves the internal debug snapshot. It must only
// be mounted on the local metrics listener, never on agent-facing listeners.
func trafficPriorityHandler(provider TrafficStatusProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		status := provider.TrafficStatus()
		enc := json.NewEncoder(w)
		if err := enc.Encode(status); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
