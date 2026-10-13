// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rs/zerolog/hlog"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

// operationClass maps an operation (as returned by pathToOperation) to its
// traffic class. The second result is false for operations not managed by
// the traffic gate (they keep today's behavior exactly).
func operationClass(operation string) (traffic.Class, bool) {
	switch operation {
	case "checkin", "acks", "opamp", "enroll", "status", "getPGPKey", "audit-unenroll":
		return traffic.Control, true
	case "uploadBegin", "uploadChunk", "uploadComplete", "deliverFile", "artifact":
		return traffic.Background, true
	default:
		return traffic.Control, false
	}
}

// trafficAdmission is the outermost middleware: it admits classified
// requests through the shared capacity gate before the connection throttle
// and per-route limiters run, so a waiting background request never holds a
// connection throttle slot or a route permit. The admitted request class is
// also propagated through the request context to bulk and the ES client.
//
// A nil/disabled gate and unmanaged paths are pure pass-through.
func trafficAdmission(gate *traffic.Gate, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gate == nil || !gate.Enabled() {
			next.ServeHTTP(w, r)
			return
		}

		class, managed := operationClass(pathToOperation(r.URL.Path))
		if !managed {
			next.ServeHTTP(w, r)
			return
		}

		ticket, err := gate.Acquire(r.Context(), class)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Client went away while queued: nothing to write back.
				return
			}
			hlog.FromRequest(r).Warn().
				Str("class", class.String()).
				Err(err).
				Msg("traffic gate rejected request")
			writeTrafficError(w, err)
			return
		}
		defer ticket.Release()

		ctx := traffic.WithClass(r.Context(), class)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// writeTrafficError responds with the same 429 envelope shape as the limit
// package so clients see one consistent overload contract.
func writeTrafficError(w http.ResponseWriter, err error) {
	resp := struct {
		Status  int    `json:"statusCode"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}{
		Status:  http.StatusTooManyRequests,
		Error:   "TrafficLimit",
		Message: "exceeded traffic capacity",
	}
	if errors.Is(err, traffic.ErrCapacityTimeout) {
		resp.Message = "timed out waiting for traffic capacity"
	}
	p, mErr := json.Marshal(&resp)
	if mErr != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(p)
}
