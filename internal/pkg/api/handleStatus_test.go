// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build !integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elastic/elastic-agent-client/v7/pkg/client"
	"github.com/elastic/elastic-agent-client/v7/pkg/proto"
	"github.com/elastic/fleet-server/v7/internal/pkg/apikey"
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	fbuild "github.com/elastic/fleet-server/v7/internal/pkg/build"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	testlog "github.com/elastic/fleet-server/v7/internal/pkg/testing/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withAuthFunc(authfn AuthFunc) OptFunc {
	return func(st *StatusT) {
		if authfn != nil {
			st.authfn = authfn
		}
	}
}

type mockStatusPolicyMonitor struct {
	state client.UnitState
}

func (pm *mockStatusPolicyMonitor) Run(ctx context.Context) error {
	return nil
}

func (pm *mockStatusPolicyMonitor) State() client.UnitState {
	return pm.state
}

func TestHandleStatus(t *testing.T) {
	ctx := context.Background()

	cfg := &config.Server{}
	cfg.InitDefaults()
	c, err := cache.New(config.Cache{NumCounters: 100, MaxCost: 100000})
	require.NoError(t, err)

	authfnOk := func(r *http.Request) (*apikey.APIKey, error) {
		return nil, nil
	}
	authfnFail := func(r *http.Request) (*apikey.APIKey, error) {
		return nil, apikey.ErrNoAuthHeader
	}

	tests := []struct {
		Name   string
		AuthFn AuthFunc
		Authed bool
	}{
		{
			Name:   "authenticated",
			AuthFn: authfnOk,
			Authed: true,
		},
		{
			Name:   "non authenticated",
			AuthFn: authfnFail,
		},
	}

	// Test table, with inner loop on all available statuses
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			for k, v := range proto.State_name {
				t.Run(v, func(t *testing.T) {
					logger := testlog.SetLogger(t)
					ctx = logger.WithContext(ctx)
					state := client.UnitState(k)
					r := apiServer{
						st: NewStatusT(cfg, nil, c, withAuthFunc(tc.AuthFn), WithSelfMonitor(&mockStatusPolicyMonitor{state}), WithBuildInfo(fbuild.Info{
							Version:   "8.1.0",
							Commit:    "4eff928",
							BuildTime: time.Now(),
						})),
					}

					hr := Handler(&r)

					w := httptest.NewRecorder()
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/api/status", nil)
					hr.ServeHTTP(w, req)

					expectedCode := http.StatusServiceUnavailable
					if state == client.UnitStateHealthy {
						expectedCode = http.StatusOK
					}

					assert.Equal(t, expectedCode, w.Code)

					var res StatusAPIResponse
					err := json.Unmarshal(w.Body.Bytes(), &res)
					require.NoError(t, err)

					assert.Equal(t, "fleet-server", res.Name)
					assert.Equal(t, state.String(), string(res.Status))
					// Expect extended version information if authenticated
					if tc.Authed {
						require.NotNil(t, res.Version)
						assert.Equal(t, r.st.bi.Version, *res.Version.Number)
						assert.Equal(t, r.st.bi.Commit, *res.Version.BuildHash)
						assert.Equal(t, r.st.bi.BuildTime.Format(time.RFC3339), *res.Version.BuildTime)
					} else {
						require.Nil(t, res.Version)
					}
				})
			}
		})
	}
}

func statusRegistry(enabled bool) *budget.Registry {
	cfg := budget.Config{
		Enabled:          enabled,
		IdleTTL:          time.Hour,
		ReclaimTTL:       time.Minute,
		MaxTrackedOwners: 10,
	}
	// Mirrors production wiring: no resources are registered while the
	// master switch is off.
	if enabled {
		cfg.Resources = map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCheckin: {Enabled: true, Capacity: 4, GuaranteeMin: 1, GuaranteeMax: 4, BorrowMax: 4},
		}
	}
	return budget.New(cfg)
}

// TR-13.1: authenticated responses carry the budget section and its values
// match the registry snapshot.
func TestHandleStatusBudgetSectionAuthenticated(t *testing.T) {
	ctx := testlog.SetLogger(t).WithContext(context.Background())
	cfg := &config.Server{}
	cfg.InitDefaults()
	c, err := cache.New(config.Cache{NumCounters: 100, MaxCost: 100000})
	require.NoError(t, err)

	reg := statusRegistry(true)
	lease, err := reg.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.NoError(t, err)
	defer lease.Release()
	// Rejected admission shows up in the section too.
	_, err = reg.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.NoError(t, err)
	_, err = reg.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.NoError(t, err)
	_, err = reg.TryAcquire(budget.ResourceCheckin, "ns-a")
	require.NoError(t, err)
	_, err = reg.TryAcquire(budget.ResourceCheckin, "ns-b")
	require.Error(t, err)

	st := NewStatusT(cfg, nil, c,
		withAuthFunc(func(*http.Request) (*apikey.APIKey, error) { return nil, nil }),
		WithSelfMonitor(&mockStatusPolicyMonitor{state: client.UnitStateHealthy}),
		WithStatusBudgets(reg),
	)
	r := apiServer{st: st}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/api/status", nil)
	Handler(&r).ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	section, ok := raw["namespace_budgets"]
	require.True(t, ok, "authenticated status must include the budget section")

	var snap budget.Snapshot
	require.NoError(t, json.Unmarshal(section, &snap))
	assert.True(t, snap.Enabled)
	require.Len(t, snap.Resource, 1)
	rs := snap.Resource[0]
	assert.Equal(t, budget.ResourceCheckin, rs.Resource)
	assert.Equal(t, int64(4), rs.Capacity)
	assert.Equal(t, int64(4), rs.InUse)

	var a budget.OwnerSnapshot
	for _, os := range rs.Owners {
		if os.Owner == "ns-a" {
			a = os
		}
	}
	assert.Equal(t, int64(4), a.InUse)
	assert.Equal(t, int64(0), a.Rejected[budget.ReasonMax]) // ns-a admits never rejected
	var bRejects int64
	for _, os := range rs.Owners {
		if os.Owner == "ns-b" {
			bRejects = os.Rejected[budget.ReasonMax]
		}
	}
	assert.Equal(t, int64(1), bRejects)
}

// TR-13.1: the unauthenticated response is unchanged and exposes no budget
// detail.
func TestHandleStatusBudgetSectionUnauthenticatedHidden(t *testing.T) {
	ctx := testlog.SetLogger(t).WithContext(context.Background())
	cfg := &config.Server{}
	cfg.InitDefaults()
	c, err := cache.New(config.Cache{NumCounters: 100, MaxCost: 100000})
	require.NoError(t, err)

	reg := statusRegistry(true)
	st := NewStatusT(cfg, nil, c,
		withAuthFunc(func(*http.Request) (*apikey.APIKey, error) { return nil, apikey.ErrNoAuthHeader }),
		WithSelfMonitor(&mockStatusPolicyMonitor{state: client.UnitStateHealthy}),
		WithStatusBudgets(reg),
	)
	r := apiServer{st: st}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/api/status", nil)
	Handler(&r).ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	_, hasBudgets := raw["namespace_budgets"]
	assert.False(t, hasBudgets)
	_, hasVersion := raw["version"]
	assert.False(t, hasVersion)
}

// When the feature switch is off the authenticated section reports the
// disabled baseline and no managed resources.
func TestHandleStatusBudgetSectionDisabled(t *testing.T) {
	ctx := testlog.SetLogger(t).WithContext(context.Background())
	cfg := &config.Server{}
	cfg.InitDefaults()
	c, err := cache.New(config.Cache{NumCounters: 100, MaxCost: 100000})
	require.NoError(t, err)

	reg := statusRegistry(false)
	st := NewStatusT(cfg, nil, c,
		withAuthFunc(func(*http.Request) (*apikey.APIKey, error) { return nil, nil }),
		WithSelfMonitor(&mockStatusPolicyMonitor{state: client.UnitStateHealthy}),
		WithStatusBudgets(reg),
	)
	r := apiServer{st: st}
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/api/status", nil)
	Handler(&r).ServeHTTP(w, req)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	section, ok := raw["namespace_budgets"]
	require.True(t, ok)
	var snap budget.Snapshot
	require.NoError(t, json.Unmarshal(section, &snap))
	assert.False(t, snap.Enabled)
	assert.Empty(t, snap.Resource)
}
