// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/rs/zerolog/hlog"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/limit"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// acquireTenantBudget is the second admission stage, executed only after
// authorization succeeded and the resource owner is therefore known. It
// stamps the owner into the request context and claims one slot in the
// owner's namespace budget for the resource class.
//
// The route limiter's global slot (first stage) is released on its own
// when the handler returns, so a rejected second stage never holds a
// global slot beyond the response write. A successful claim returns a
// lease the caller must defer-release; the lease keeps the owner captured
// at admission for the whole request, including long polls.
//
// Rejections reuse the legacy 429 body verbatim and may add Retry-After.
func acquireTenantBudget(r *http.Request, w http.ResponseWriter, budgets *budget.Registry, res budget.Resource, owner tenant.Owner) (context.Context, budget.Lease, bool) {
	ctx := tenant.WithOwner(r.Context(), owner)
	if budgets == nil {
		return ctx, budget.NopLease(owner, res), true
	}
	lease, err := budgets.TryAcquire(res, owner)
	if err != nil {
		writeBudgetRejection(r, w, res, owner, err)
		return ctx, nil, false
	}
	return ctx, lease, true
}

// requestOwner returns the owner stamped at second-stage admission, or the
// shared default bucket for call sites without one.
func requestOwner(r *http.Request) tenant.Owner {
	return ctxOwner(r.Context())
}

// ctxOwner returns the owner stamped into ctx at second-stage admission, or
// the shared default bucket for call sites without one.
func ctxOwner(ctx context.Context) tenant.Owner {
	if owner, ok := tenant.OwnerFromContext(ctx); ok {
		return owner
	}
	return tenant.Default
}

func writeBudgetRejection(r *http.Request, w http.ResponseWriter, res budget.Resource, owner tenant.Owner, err error) {
	zlog := hlog.FromRequest(r)
	rej, ok := budget.As(err)

	fields := map[string]any{
		labelResource: string(res),
		"owner":       string(owner),
	}
	if ok {
		fields["reason"] = string(rej.Reason)
		if rej.RetryAfter > 0 {
			seconds := int((rej.RetryAfter + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", itoa(seconds))
		}
	}
	zlog.Warn().Fields(fields).Err(err).Msg("namespace budget rejected")

	limErr := limit.ErrMaxLimit
	if ok && rej.Reason == budget.ReasonBackoff {
		limErr = limit.ErrRateLimit
	}
	if wErr := limit.WriteError(hlog.FromRequest(r), w, limErr); wErr != nil {
		hlog.FromRequest(r).Error().Err(wErr).Msg("fail writing budget rejection error response")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
