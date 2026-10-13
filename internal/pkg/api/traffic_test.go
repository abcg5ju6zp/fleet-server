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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

func TestOperationClass(t *testing.T) {
	control := []string{"checkin", "acks", "opamp", "enroll", "status", "getPGPKey", "audit-unenroll"}
	background := []string{"uploadBegin", "uploadChunk", "uploadComplete", "deliverFile", "artifact"}
	unmanaged := []string{"", "something-else"}

	for _, op := range control {
		c, managed := operationClass(op)
		assert.Equal(t, traffic.Control, c, op)
		assert.True(t, managed, op)
	}
	for _, op := range background {
		c, managed := operationClass(op)
		assert.Equal(t, traffic.Background, c, op)
		assert.True(t, managed, op)
	}
	for _, op := range unmanaged {
		_, managed := operationClass(op)
		assert.False(t, managed, op)
	}
}

func TestTrafficAdmissionPassthrough(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
	})

	// nil gate
	req := httptest.NewRequest(http.MethodPost, "/api/fleet/artifacts/x/y", nil)
	rec := httptest.NewRecorder()
	trafficAdmission(nil, inner).ServeHTTP(rec, req)
	require.True(t, called)
	require.Equal(t, http.StatusAccepted, rec.Code)

	// disabled gate
	gate, err := traffic.NewGate(nil)
	require.NoError(t, err)
	called = false
	rec = httptest.NewRecorder()
	trafficAdmission(gate, inner).ServeHTTP(rec, req)
	require.True(t, called)
	require.Equal(t, http.StatusAccepted, rec.Code)

	// unknown path under enabled gate
	require.NoError(t, gate.Apply(&traffic.Snapshot{Enabled: true, Capacity: 1, ReservedControl: 1}))
	called = false
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/unknown", nil)
	trafficAdmission(gate, inner).ServeHTTP(rec, req)
	require.True(t, called)
}

func TestTrafficAdmissionTagsContextAndReleases(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2})
	require.NoError(t, err)

	seen := traffic.Control
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = traffic.FromContext(r.Context())
	})

	for _, tc := range []struct {
		path  string
		class traffic.Class
	}{
		{"/api/fleet/agents/id/checkin", traffic.Control},
		{"/api/fleet/agents/id/acks", traffic.Control},
		{"/v1/opamp", traffic.Control},
		{"/api/fleet/artifacts/x/y", traffic.Background},
		{"/api/fleet/uploads", traffic.Background},
		{"/api/fleet/file/x", traffic.Background},
	} {
		seen = traffic.Control
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, tc.path, nil)
		trafficAdmission(gate, inner).ServeHTTP(rec, req)
		assert.Equal(t, tc.class, seen, tc.path)
		require.Equal(t, http.StatusOK, rec.Code, tc.path)
	}

	st := gate.Stats()
	assert.Zero(t, st.Control.InFlight)
	assert.Zero(t, st.Background.InFlight, "ticket released when the handler returns")
}

func TestTrafficAdmissionBackground429Body(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 2, ReservedControl: 1, BackgroundWaitTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	release := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(trafficAdmission(gate, inner))
	defer srv.Close()

	// Saturate the gate with two in-flight background requests.
	for range 2 {
		go func() { _, _ = srv.Client().Post(srv.URL+"/api/fleet/artifacts/x/y", "application/json", nil) }()
	}
	require.Eventually(t, func() bool {
		return gate.Stats().Background.InFlight == 2
	}, time.Second, time.Millisecond)

	// Third background waits and is rejected with the standard 429 body.
	resp, err := srv.Client().Post(srv.URL+"/api/fleet/uploads", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)

	var body struct {
		StatusCode int    `json:"statusCode"`
		Error      string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, http.StatusTooManyRequests, body.StatusCode)
	assert.Equal(t, "TrafficLimit", body.Error)

	// A control request queues and is admitted as soon as a permit frees.
	ctlDone := make(chan int, 1)
	go func() {
		r, e := srv.Client().Get(srv.URL + "/api/fleet/agents/id/checkin")
		if e != nil {
			ctlDone <- 0
			return
		}
		_ = r.Body.Close()
		ctlDone <- r.StatusCode
	}()
	require.Eventually(t, func() bool { return gate.Stats().Control.Waiting == 1 }, time.Second, time.Millisecond)

	close(release)
	select {
	case code := <-ctlDone:
		assert.Equal(t, http.StatusNoContent, code)
	case <-time.After(2 * time.Second):
		t.Fatal("control request not admitted after background released")
	}
}

func TestTrafficAdmissionCancelWhileWaiting(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled: true, Capacity: 1, ReservedControl: 1, BackgroundWaitTimeout: 0,
	})
	require.NoError(t, err)

	release := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(trafficAdmission(gate, inner))
	defer srv.Close()

	go func() { _, _ = srv.Client().Post(srv.URL+"/api/fleet/artifacts/x/y", "application/json", nil) }()
	require.Eventually(t, func() bool { return gate.Stats().Background.InFlight == 1 }, time.Second, time.Millisecond)

	innerCalled := make(chan struct{}, 1)
	probe := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		innerCalled <- struct{}{}
	})
	h := trafficAdmission(gate, probe)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/api/fleet/uploads", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	select {
	case <-innerCalled:
		t.Fatal("inner handler must not be called for a rejected request")
	default:
	}
	// A client that already left gets no response body, only the default
	// recorder status; no 429 is written to a canceled connection.
	assert.Equal(t, http.StatusOK, rec.Code)

	close(release)
}
