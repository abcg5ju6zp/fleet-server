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

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/limit"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

func budgetTestRegistry(t *testing.T, capacity int64) *budget.Registry {
	t.Helper()
	cfg := budget.Config{
		Enabled:          true,
		IdleTTL:          time.Minute,
		ReclaimTTL:       time.Second,
		MaxTrackedOwners: 10,
		Backoff: budget.BackoffConfig{
			Initial:          time.Second,
			Max:              time.Second,
			FailureThreshold: 2,
		},
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCheckin: {
				Enabled:      true,
				Capacity:     capacity,
				GuaranteeMin: 1,
				GuaranteeMax: capacity,
				BorrowMax:    capacity,
			},
		},
	}
	return budget.New(cfg)
}

func TestAcquireTenantBudget429BodyMatchesLegacy(t *testing.T) {
	r := budgetTestRegistry(t, 1)

	lease, err := r.TryAcquire(budget.ResourceCheckin, "space-a")
	require.NoError(t, err)
	defer lease.Release()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/fleet/agents/id/checkin", nil)
	w := httptest.NewRecorder()
	_, l, admitted := acquireTenantBudget(req, w, r, budget.ResourceCheckin, tenant.Owner("space-b"))
	require.False(t, admitted)
	assert.Nil(t, l)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "application/json; charset=utf-8", w.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	_, hasRetryAfter := w.Header()["Retry-After"]
	assert.False(t, hasRetryAfter, "capacity rejections do not send Retry-After")

	// Body must be byte-identical to the legacy max-limit 429 response.
	legacy := httptest.NewRecorder()
	require.NoError(t, limit.WriteError(&zerolog.Logger{}, legacy, limit.ErrMaxLimit))
	var legacyBody, gotBody map[string]any
	require.NoError(t, json.Unmarshal(legacy.Body.Bytes(), &legacyBody))
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &gotBody))
	assert.Equal(t, legacyBody, gotBody)
}

func TestAcquireTenantBudgetBackoffSendsRetryAfter(t *testing.T) {
	r := budgetTestRegistry(t, 4)
	r.ReportFailure(budget.ResourceCheckin, "space-a")
	r.ReportFailure(budget.ResourceCheckin, "space-a")

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/fleet/agents/id/checkin", nil)
	w := httptest.NewRecorder()
	_, _, admitted := acquireTenantBudget(req, w, r, budget.ResourceCheckin, tenant.Owner("space-a"))
	require.False(t, admitted)
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
}

func TestAcquireTenantBudgetNilRegistryIsNoop(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/fleet/agents/upgrades/x/pgp-public-key", nil)
	w := httptest.NewRecorder()
	ctx, lease, admitted := acquireTenantBudget(req, w, nil, budget.ResourcePGP, tenant.Default)
	require.True(t, admitted)
	assert.NotNil(t, lease)
	assert.Equal(t, tenant.Default, lease.Owner())
	assert.Equal(t, budget.ResourcePGP, lease.Resource())
	lease.Release()
	assert.Equal(t, http.StatusOK, w.Code)
	_, ok := tenant.OwnerFromContext(ctx)
	assert.True(t, ok)
}

// TestTwoStageAdmissionReleasesGlobalSlotOnReject proves that a rejected
// second stage never retains the route limiter's global slot: the
// emulated middleware (global acquire -> second-stage helper -> deferred
// global release) admits a subsequent request immediately after the
// rejected response returns.
func TestTwoStageAdmissionReleasesGlobalSlotOnReject(t *testing.T) {
	r := budgetTestRegistry(t, 1)
	lim := limit.NewLimiter(&config.Limit{Max: 1})

	srv := httptest.NewServer(lim.Wrap("checkin", nil, zerolog.DebugLevel)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, lease, admitted := acquireTenantBudget(req, w, r, budget.ResourceCheckin, "space-a")
		if !admitted {
			return
		}
		defer lease.Release()
		w.WriteHeader(http.StatusOK)
	})))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Second stage rejection (namespace bucket full from the first
	// request's held slot) must not keep the global slot: this request
	// succeeds against the legacy limiter because the prior handler exited.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp2, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode, "global slot released after second-stage rejection")
}

// TestLongPollLeaseReleasedOnDisconnect covers the check-in lease lifecycle:
// held for the whole blocking poll and returned on every exit, including a
// canceled request context.
func TestLongPollLeaseReleasedOnDisconnect(t *testing.T) {
	r := budgetTestRegistry(t, 4)
	entered := make(chan struct{})
	release := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, lease, admitted := acquireTenantBudget(req, w, r, budget.ResourceCheckin, "space-a")
		if !admitted {
			return
		}
		defer lease.Release()
		close(entered)
		select {
		case <-release:
		case <-req.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		ctx, cancel := context.WithCancel(req.Context())
		// Cancel from a separate goroutine once the long poll is in
		// flight; RoundTrip only returns once the handler observes the
		// cancellation, so canceling after it blocks would deadlock.
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()
		req = req.WithContext(ctx)
		resp, err := srv.Client().Transport.RoundTrip(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	<-entered
	require.Eventually(t, func() bool {
		for _, rs := range r.Snapshot().Resource {
			for _, os := range rs.Owners {
				if os.Owner == "space-a" {
					return os.InUse == 0
				}
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "lease returned after client disconnect")
}
