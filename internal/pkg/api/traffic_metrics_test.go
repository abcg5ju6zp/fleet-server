// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

type fakeTrafficProvider struct {
	status TrafficStatus
}

func (f *fakeTrafficProvider) TrafficStatus() TrafficStatus { return f.status }

func TestGateMetricsObserver(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 4, ReservedControl: 2, BackgroundWaitTimeout: time.Second,
	})
	require.NoError(t, err)
	gate.SetObserver(newGateMetricsObserver("unittest"))

	inflight := trafficInflight.WithLabelValues("unittest", "background")
	waiting := trafficWaiting.WithLabelValues("unittest", "background")
	borrowed := trafficBorrowed.WithLabelValues("unittest", "background")
	admFloor := trafficAdmitted.WithLabelValues("unittest", "background", "floor")
	admBorrow := trafficAdmitted.WithLabelValues("unittest", "background", "borrowed")

	before := testutil.ToFloat64(inflight)
	beforeWait := testutil.ToFloat64(waiting)
	beforeBorrow := testutil.ToFloat64(borrowed)
	beforeFloor := testutil.ToFloat64(admFloor)

	var tickets []*traffic.Ticket
	for range 3 {
		tk, err := gate.Acquire(context.Background(), traffic.Background)
		require.NoError(t, err)
		tickets = append(tickets, tk)
	}
	assert.Greater(t, testutil.ToFloat64(inflight), before)
	assert.Equal(t, beforeWait, testutil.ToFloat64(waiting), "immediate admits leave no waiting gauge")
	assert.Greater(t, testutil.ToFloat64(admBorrow), 0.0, "third background admit borrows the reserve")
	assert.Greater(t, testutil.ToFloat64(admFloor), beforeFloor)

	for _, tk := range tickets {
		tk.Release()
	}
	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(inflight) == before
	}, time.Second, time.Millisecond)
	assert.Equal(t, beforeBorrow, testutil.ToFloat64(borrowed), "borrowed gauge returns to baseline")
}

func TestRunTrafficMetricsSampler(t *testing.T) {
	prov := &fakeTrafficProvider{status: TrafficStatus{
		Enabled: true,
		Bulk: bulk.BulkTrafficStats{
			Queues: []bulk.QueueTrafficStat{
				{Queue: "search", Items: 5, Bytes: 100, ControlItems: 2},
				{Queue: "bulk", Items: 3, Bytes: 60, ControlItems: 3},
			},
		},
		Reload: TrafficReloadStatus{OK: true, At: time.Now()},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunTrafficMetricsSampler(ctx, 10*time.Millisecond, prov)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(trafficBulkQueueDepth.WithLabelValues("search", "control")) == 2 &&
			testutil.ToFloat64(trafficBulkQueueDepth.WithLabelValues("search", "background")) == 3 &&
			testutil.ToFloat64(trafficBulkQueueDepth.WithLabelValues("bulk", "background")) == 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, 1.0, testutil.ToFloat64(trafficReloadOK))
	assert.Greater(t, testutil.ToFloat64(trafficReloadTimestamp), 0.0)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop")
	}
}

func TestTrafficPriorityHandler(t *testing.T) {
	prov := &fakeTrafficProvider{status: TrafficStatus{
		Enabled: true,
		Stages: []TrafficStageStatus{
			{
				Stage:    "entry",
				Enabled:  true,
				Snapshot: traffic.Snapshot{Enabled: true, Capacity: 10, ReservedControl: 4},
				Stats: traffic.Stats{
					Enabled:            true,
					Capacity:           10,
					ReservedControl:    4,
					ReservedBackground: 6,
					Background:         traffic.ClassStats{InFlight: 2, Borrowed: 1, Waiting: 1},
				},
			},
		},
		Reload: TrafficReloadStatus{OK: true},
	}}

	h := trafficPriorityHandler(prov)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/traffic-priority", nil)
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var got TrafficStatus
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.True(t, got.Enabled)
	require.Len(t, got.Stages, 1)
	assert.Equal(t, "entry", got.Stages[0].Stage)
	assert.Equal(t, int64(2), got.Stages[0].Stats.Background.InFlight)
	assert.True(t, got.Reload.OK)
}
