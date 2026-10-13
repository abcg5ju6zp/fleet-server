// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.elastic.co/apm/v2"

	"github.com/elastic/elastic-agent-client/v7/pkg/client"

	"github.com/elastic/fleet-server/v7/internal/pkg/apikey"
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/build"
	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/logger"
	"github.com/elastic/fleet-server/v7/internal/pkg/policy"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"

	"github.com/rs/zerolog"
)

const (
	kStatusMod = "status"
)

// statusResponse extends the generated status body for authenticated callers
// with the explainable namespace-budget section. The generated type is not
// hand-edited; the extra field is added only at the response boundary.
type statusResponse struct {
	StatusAPIResponse
	NamespaceBudgets *budget.Snapshot `json:"namespace_budgets,omitempty"`
}

type AuthFunc func(*http.Request) (*apikey.APIKey, error)

type StatusT struct {
	cfg     *config.Server
	bulk    bulk.Bulk
	cache   cache.Cache
	sm      policy.SelfMonitor
	bi      build.Info
	authfn  AuthFunc
	budgets *budget.Registry
}

type OptFunc func(*StatusT)

func WithSelfMonitor(sm policy.SelfMonitor) OptFunc {
	return func(st *StatusT) {
		st.sm = sm
	}
}

func WithBuildInfo(bi build.Info) OptFunc {
	return func(st *StatusT) {
		st.bi = bi
	}
}

func NewStatusT(cfg *config.Server, bulker bulk.Bulk, cache cache.Cache, opts ...OptFunc) *StatusT {
	st := &StatusT{
		cfg:   cfg,
		bulk:  bulker,
		cache: cache,
	}
	st.authfn = st.authenticate

	for _, opt := range opts {
		opt(st)
	}
	return st
}

func (st StatusT) authenticate(r *http.Request) (*apikey.APIKey, error) {
	// This authenticates that the provided API key exists and is enabled.
	// WARNING: This does not validate that the api key is valid for the Fleet Domain.
	// An additional check must be executed to validate it is not a random api key.
	// This check is sufficient for the purposes of this API
	return authAPIKey(r, st.bulk, st.cache)
}

func (st StatusT) handleStatus(zlog zerolog.Logger, r *http.Request, w http.ResponseWriter) error {
	authed := true
	if _, aerr := st.authfn(r); aerr != nil {
		zlog.Debug().Err(aerr).Msg("unauthenticated status request, return short status response only")
		authed = false
	}

	// Authenticated status requests share the identity-less default bucket;
	// unauthenticated ones stay outside namespace accounting entirely.
	if authed {
		var lease budget.Lease
		var admitted bool
		if _, lease, admitted = acquireTenantBudget(r, w, st.budgets, budget.ResourceStatus, tenant.Default); !admitted {
			return nil
		}
		defer lease.Release()
	}

	span, ctx := apm.StartSpan(r.Context(), "getState", "process")
	state := st.sm.State()
	out := statusResponse{
		StatusAPIResponse: StatusAPIResponse{
			Name:   build.ServiceName,
			Status: StatusResponseStatus(state.String()), // TODO try to make the oapi codegen less verbose here
		},
	}

	if authed {
		sSpan, _ := apm.StartSpan(ctx, "getVersion", "process")
		bt := st.bi.BuildTime.Format(time.RFC3339)
		out.Version = &StatusResponseVersion{
			Number:    &st.bi.Version,
			BuildHash: &st.bi.Commit,
			BuildTime: &bt,
		}
		sSpan.End()

		// The explainable budget section is authenticated-only; the
		// unauthenticated short response stays byte-identical. A disabled
		// registry still reports its effective state (enabled=false).
		if st.budgets != nil {
			snap := st.budgets.Snapshot()
			out.NamespaceBudgets = &snap
		}
	}
	span.End()

	span, _ = apm.StartSpan(r.Context(), "response", "write")
	defer span.End()

	// If the request context has been cancelled, such as the case when the server is stopping we should return a 503
	// Note that the API server uses Shutdown, so no new requests should be accepted and this edge case will be rare.
	if errors.Is(r.Context().Err(), context.Canceled) {
		state = client.UnitStateStopping
	}

	data, err := json.Marshal(&out)
	if err != nil {
		return err
	}

	code := http.StatusServiceUnavailable
	if state == client.UnitStateHealthy {
		code = http.StatusOK
	}
	w.WriteHeader(code)

	ts, ok := logger.CtxStartTime(r.Context())
	nWritten, err := w.Write(data)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			e := zlog.Error().Err(err).Int(ECSHTTPResponseCode, code)
			if ok {
				e = e.Int64(ECSEventDuration, time.Since(ts).Nanoseconds())
			}
			e.Msg("fail status")
		}
	}

	cntStatus.bodyOut.Add(uint64(nWritten)) //nolint:gosec // disable G115
	e := zlog.Debug().Int(ECSHTTPResponseBodyBytes, nWritten)
	if ok {
		e = e.Int64(ECSEventDuration, time.Since(ts).Nanoseconds())
	}
	e.Msg("ok status")

	return nil
}
