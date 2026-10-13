// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package es

import (
	"net/http"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

// WithTrafficGate wraps the client transport with a class-aware capacity
// gate. A permit is acquired per HTTP attempt from the class carried by the
// request context (defaulting to control), so elastictransport retries keep
// their class across attempts and release the permit during backoff.
//
// A nil gate is a no-op.
func WithTrafficGate(gate *traffic.Gate) ConfigOption {
	return ConfigOption{
		rtWrap: func(rt http.RoundTripper) http.RoundTripper {
			if gate == nil {
				return rt
			}
			return &trafficRoundTripper{next: rt, gate: gate}
		},
	}
}

type trafficRoundTripper struct {
	next  http.RoundTripper
	gate  *traffic.Gate
}

func (t *trafficRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	class := traffic.FromContext(req.Context())
	ticket, err := t.gate.Acquire(req.Context(), class)
	if err != nil {
		return nil, err
	}
	defer ticket.Release()
	return t.next.RoundTrip(req)
}
