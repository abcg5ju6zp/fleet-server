// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package traffic

import (
	"context"
	"testing"
)

// BenchmarkGateAcquireDisabled measures the no-op fast path when the
// feature is off: it must be an allocation-free trivial return.
func BenchmarkGateAcquireDisabled(b *testing.B) {
	g, _ := NewGate(nil)
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		tk, err := g.Acquire(ctx, Background)
		if err != nil {
			b.Fatal(err)
		}
		tk.Release()
	}
}

// BenchmarkGateAcquireUncontended measures immediate admit/release when
// capacity is free.
func BenchmarkGateAcquireUncontended(b *testing.B) {
	g, _ := NewGate(&Snapshot{
		Enabled:               true,
		Capacity:              1024,
		ReservedControl:       256,
		BackgroundWaitTimeout: 0,
	})
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		tk, err := g.Acquire(ctx, Background)
		if err != nil {
			b.Fatal(err)
		}
		tk.Release()
	}
}
