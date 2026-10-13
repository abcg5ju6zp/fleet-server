// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package traffic implements traffic classification and class-aware capacity
// reservation used to protect control-plane traffic (agent check-ins, action
// acknowledgements, OpAMP heartbeats) from background traffic (diagnostic
// file uploads, file downloads, artifact fetches) during overload.
package traffic

import (
	"context"
)

// Class is the traffic class attached to a request and to the Elasticsearch
// work it causes.
type Class uint8

const (
	// Control is the zero value so that any context without an explicit class
	// is treated as control traffic. Control traffic owns the reserved share
	// of every gate.
	Control Class = iota
	// Background is background traffic (diagnostic uploads, file downloads,
	// artifact fetches). It may borrow idle control capacity, but stops
	// borrowing as soon as a control request is waiting.
	Background
)

// String returns the metrics/label safe name of the class.
func (c Class) String() string {
	switch c {
	case Control:
		return "control"
	case Background:
		return "background"
	default:
		return "unknown"
	}
}

type ctxClassKey struct{}

// WithClass returns a context carrying the given class.
func WithClass(ctx context.Context, c Class) context.Context {
	return context.WithValue(ctx, ctxClassKey{}, c)
}

// FromContext returns the class carried by ctx. It defaults to Control so
// that un-annotated server-side work (monitors, GC, bulk flushes) is never
// starved.
func FromContext(ctx context.Context) Class {
	if ctx == nil {
		return Control
	}
	if c, ok := ctx.Value(ctxClassKey{}).(Class); ok {
		return c
	}
	return Control
}
