// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package es

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

type recordingTransport struct {
	calls     atomic.Int64
	classes   []string
	inFlight  atomic.Int64
	maxFlight atomic.Int64
	block     chan struct{}
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := traffic.FromContext(req.Context()).String()
	r.classes = append(r.classes, c)
	n := r.inFlight.Add(1)
	for {
		old := r.maxFlight.Load()
		if n <= old || r.maxFlight.CompareAndSwap(old, n) {
			break
		}
	}
	if r.block != nil {
		select {
		case <-r.block:
		case <-req.Context().Done():
			r.inFlight.Add(-1)
			return nil, req.Context().Err()
		}
	}
	r.inFlight.Add(-1)
	r.calls.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}, nil
}

func newReq(ctx context.Context) *http.Request {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9200/_bulk", strings.NewReader(`{}`))
	if err != nil {
		panic(err)
	}
	return req
}

func TestTrafficRoundTripperPerAttemptAcquire(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 4, ReservedControl: 2, BackgroundWaitTimeout: time.Second,
	})
	require.NoError(t, err)

	inner := &recordingTransport{}
	rt := &trafficRoundTripper{next: inner, gate: gate}

	// Simulate three transport attempts (initial + two elastictransport
	// retries) on the same background request.
	ctx := traffic.WithClass(context.Background(), traffic.Background)
	for range 3 {
		resp, err := rt.RoundTrip(newReq(ctx))
		require.NoError(t, err)
		_ = resp.Body.Close()
	}

	require.Equal(t, int64(3), inner.calls.Load())
	assert.Equal(t, []string{"background", "background", "background"}, inner.classes,
		"class must be retained across retry attempts")
	assert.Equal(t, int64(0), inner.inFlight.Load())

	st := gate.Stats()
	assert.Equal(t, int64(3), st.Background.AdmittedFloor+st.Background.AdmittedBorrowed)
	assert.Zero(t, st.Background.InFlight, "permit released per attempt, including during backoff")
}

func TestTrafficRoundTripperDefaultClassControl(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 2, ReservedControl: 1})
	require.NoError(t, err)

	inner := &recordingTransport{}
	rt := &trafficRoundTripper{next: inner, gate: gate}

	_, err = rt.RoundTrip(newReq(context.Background()))
	require.NoError(t, err)
	assert.Equal(t, "control", inner.classes[0])
}

func TestTrafficRoundTripperRejectsSaturatedBackground(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 2, ReservedControl: 1, BackgroundWaitTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	block := make(chan struct{})
	inner := &recurringTransport{block: block}
	rt := &trafficRoundTripper{next: inner, gate: gate}

	ctx := traffic.WithClass(context.Background(), traffic.Background)

	// Saturate both permits with long-running background attempts.
	for range 2 {
		go func() { _, _ = rt.RoundTrip(newReq(ctx)) }()
	}
	require.Eventually(t, func() bool {
		return gate.Stats().Background.InFlight == 2
	}, time.Second, time.Millisecond)

	start := time.Now()
	_, err = rt.RoundTrip(newReq(ctx))
	require.ErrorIs(t, err, traffic.ErrCapacityTimeout)
	assert.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond)
	assert.Equal(t, 2, inner.callsAfterReject(), "inner transport must not be called on rejection")

	// A control request is next in line: it waits until background releases,
	// then is admitted ahead of any future background work.
	ctrlErr := make(chan error, 1)
	go func() {
		resp, e := rt.RoundTrip(newReq(context.Background()))
		if e == nil {
			_ = resp.Body.Close()
		}
		ctrlErr <- e
	}()
	require.Eventually(t, func() bool { return gate.Stats().Control.Waiting == 1 }, time.Second, time.Millisecond)

	close(block)
	select {
	case e := <-ctrlErr:
		require.NoError(t, e)
	case <-time.After(2 * time.Second):
		t.Fatal("control not admitted after background released")
	}
}

func TestTrafficRoundTripperContextCancel(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 1, ReservedControl: 1,
	})
	require.NoError(t, err)

	block := make(chan struct{})
	rt := &trafficRoundTripper{next: &recurringTransport{block: block}, gate: gate}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = rt.RoundTrip(newReq(ctx)) }()
	require.Eventually(t, func() bool {
		return gate.Stats().Control.InFlight == 1
	}, time.Second, time.Millisecond)

	waitCtx, waitCancel := context.WithCancel(traffic.WithClass(context.Background(), traffic.Control))
	errCh := make(chan error, 1)
	go func() { _, e := rt.RoundTrip(newReq(waitCtx)); errCh <- e }()
	require.Eventually(t, func() bool { return gate.Stats().Control.Waiting == 1 }, time.Second, time.Millisecond)

	waitCancel()
	select {
	case e := <-errCh:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waiting request did not return after cancel")
	}
	assert.Zero(t, gate.Stats().Control.Waiting)

	cancel()
	close(block)
}

func TestWithTrafficGateNilIsPassthrough(t *testing.T) {
	opt := WithTrafficGate(nil)
	inner := &recurringTransport{block: make(chan struct{})}
	wrapped := opt.rtWrap(inner)
	assert.Same(t, inner, wrapped, "nil gate leaves the transport untouched")
}

func TestTrafficGateChainsWithInstrumentation(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 2, ReservedControl: 1})
	require.NoError(t, err)

	inner := &recurringTransport{block: make(chan struct{})}
	gateRT := WithTrafficGate(gate).rtWrap(inner)
	// APM instrumentation wraps the gate wrapper, matching NewClient order.
	chained := InstrumentRoundTripper().rtWrap(gateRT)
	assert.NotSame(t, gateRT, chained)

	done := make(chan struct{})
	go func() {
		_, e := chained.RoundTrip(newReq(context.Background()))
		assert.NoError(t, e)
		close(done)
	}()
	require.Eventually(t, func() bool { return gate.Stats().Control.InFlight == 1 }, time.Second, time.Millisecond)
	close(inner.block)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("chained request did not finish")
	}
	assert.Equal(t, int64(1), inner.calls.Load())
}

// recurringTransport blocks until block is closed for every call.
type recurringTransport struct {
	block     chan struct{}
	calls     atomic.Int64
	rejectHit atomic.Int64
}

func (r *recurringTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	select {
	case <-r.block:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		Body:       io.NopCloser(strings.NewReader(`{}`)),
	}, nil
}

func (r *recurringTransport) callsAfterReject() int { return int(r.calls.Load()) }
