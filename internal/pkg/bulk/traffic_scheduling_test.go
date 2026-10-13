// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

// sequencedTransport blocks every request until its release channel is
// closed and records the ES endpoint kind ("bulk" vs "msearch") in arrival
// order. It returns valid single-item responses matching one-block flushes.
type sequencedTransport struct {
	mu       sync.Mutex
	arrivals []string
	release  chan struct{}
}

func newSequencedTransport() *sequencedTransport {
	return &sequencedTransport{release: make(chan struct{}, 8)}
}

func (s *sequencedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	kind := "msearch"
	if strings.Contains(req.URL.Path, "_bulk") {
		kind = "bulk"
	}
	s.mu.Lock()
	s.arrivals = append(s.arrivals, kind)
	s.mu.Unlock()

	select {
	case <-s.release:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("X-Elastic-Product", "Elasticsearch")
	body := `{"took":1,"responses":[{"status":200,"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}]}`
	if kind == "bulk" {
		body = `{"took":1,"errors":false,"items":[{"create":{"_index":"test","_id":"1","_version":1,"result":"created","status":201}}]}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (s *sequencedTransport) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.arrivals))
	copy(out, s.arrivals)
	return out
}

func newTrafficBulker(t *testing.T, gate *traffic.Gate, tr http.RoundTripper, opts ...BulkOpt) *Bulker {
	t.Helper()
	all := []BulkOpt{
		WithFlushInterval(20 * time.Millisecond),
		WithFlushThresholdCount(1_000_000),
		WithMaxPending(1),
		WithTrafficScheduling(gate, 0, 40*time.Millisecond),
	}
	all = append(all, opts...)
	b := newTestBulkerWithTransport(t, tr, all...)
	return b
}

func runBulker(t *testing.T, b *Bulker) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = b.Run(ctx) }()
	return ctx, cancel
}

// TestControlFlushPreemptsDeferredBackground verifies that with the single
// flush permit held by a background flush, a control flush acquires it as
// soon as it frees, while a second background queue stays deferred.
func TestControlFlushPreemptsDeferredBackground(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled:               true,
		Capacity:              1,
		ReservedControl:       1,
		BackgroundWaitTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	tr := newSequencedTransport()
	b := newTrafficBulker(t, gate, tr)
	ctx, cancel := runBulker(t, b)
	defer cancel()

	bgCtx := traffic.WithClass(ctx, traffic.Background)

	errCh := make(chan error, 3)
	go func() { _, e := b.Search(bgCtx, "idx", []byte(`{}`)); errCh <- e }()

	// First background flush starts and holds the only permit.
	require.Eventually(t, func() bool {
		return len(tr.snapshot()) == 1
	}, time.Second, time.Millisecond)

	go func() { _, e := b.Search(bgCtx, "idx", []byte(`{}`)); errCh <- e }()
	go func() { _, e := b.Create(ctx, "test", "1", []byte(`{"a":1}`)); errCh <- e }()

	// Release the background flush: the control flush must be next.
	tr.release <- struct{}{}
	require.Eventually(t, func() bool {
		return len(tr.snapshot()) == 2
	}, time.Second, time.Millisecond)

	got := tr.snapshot()
	assert.Equal(t, []string{"msearch", "bulk"}, got, "control flush must preempt deferred background")

	// Release the control flush: queued background finally runs.
	tr.release <- struct{}{}
	require.Eventually(t, func() bool {
		return len(tr.snapshot()) == 3
	}, time.Second, time.Millisecond)
	assert.Equal(t, "msearch", tr.snapshot()[2])
	tr.release <- struct{}{}

	for range 3 {
		select {
		case e := <-errCh:
			assert.NoError(t, e)
		case <-time.After(2 * time.Second):
			t.Fatal("callers did not finish")
		}
	}
}

// TestClassifiedDispatchRoutesByClass verifies per-class ingress routing.
func TestClassifiedDispatchRoutesByClass(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2})
	require.NoError(t, err)
	b := newTrafficBulker(t, gate, newSequencedTransport())
	require.True(t, b.trafficEnabled())

	bg := b.newBlk(ActionSearch, optionsT{trafficClassSet: true, trafficClass: traffic.Background})
	ctrl := b.newBlk(ActionCreate, optionsT{})

	go func() { b.dispatch(context.Background(), bg) }()
	select {
	case got := <-b.chBackground:
		assert.Same(t, bg, got)
		got.ch <- respT{}
	case <-time.After(time.Second):
		t.Fatal("background block not routed to chBackground")
	}

	go func() { b.dispatch(context.Background(), ctrl) }()
	select {
	case got := <-b.chControl:
		assert.Same(t, ctrl, got)
		got.ch <- respT{}
	case <-time.After(time.Second):
		t.Fatal("control block not routed to chControl")
	}
}

// TestBackgroundDispatchLimit rejects only background when its dispatch cap
// is exceeded; control dispatches proceed.
func TestBackgroundDispatchLimit(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2})
	require.NoError(t, err)
	b := newTrafficBulker(t, gate, newSequencedTransport(),
		WithTrafficScheduling(gate, 1, time.Second))
	rejections := 0
	b.SetBackgroundDispatchRejectedCallback(func() { rejections++ })

	first := b.newBlk(ActionSearch, optionsT{trafficClassSet: true, trafficClass: traffic.Background})
	go func() { b.dispatch(context.Background(), first) }()
	select {
	case <-b.chBackground:
	case <-time.After(time.Second):
		t.Fatal("first dispatch did not enter channel")
	}
	// First dispatch is now pending its response and holds the cap.

	second := b.newBlk(ActionSearch, optionsT{trafficClassSet: true, trafficClass: traffic.Background})
	resp := b.dispatch(context.Background(), second)
	require.ErrorIs(t, resp.err, ErrTooManyBulkDispatches)
	assert.Equal(t, int64(1), b.rejectedBgDispatches.Load())
	assert.Equal(t, 1, rejections, "reject callback must fire exactly once")

	// Control is not counted against the background cap.
	ctrl := b.newBlk(ActionCreate, optionsT{})
	go func() { b.dispatch(context.Background(), ctrl) }()
	select {
	case got := <-b.chControl:
		got.ch <- respT{}
	case <-time.After(time.Second):
		t.Fatal("control dispatch blocked by background cap")
	}

	// Release the first pending dispatch.
	first.ch <- respT{}
}

// TestClassifiedDispatchCancelWhileQueued releases a block that never made
// it into a full class channel.
func TestClassifiedDispatchCancelWhileQueued(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2})
	require.NoError(t, err)
	b := newTrafficBulker(t, gate, newSequencedTransport(),
		WithBlockQueueSize(1))

	// Fill the background channel; the Run loop is not started.
	b.chBackground <- &bulkT{ch: make(chan respT, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	blk := b.newBlk(ActionSearch, optionsT{trafficClassSet: true, trafficClass: traffic.Background})
	_, _ = blk.buf.WriteString(`{"index":"test"}`)

	cancel()
	resp := b.dispatch(ctx, blk)
	require.ErrorIs(t, resp.err, context.Canceled)

	reused, ok := b.blkPool.Get().(*bulkT)
	require.True(t, ok)
	require.Zero(t, reused.buf.Len(), "aborted block returned to pool")
}

// TestDisabledGateUsesLegacyChannel verifies off-mode routing is unchanged.
func TestDisabledGateUsesLegacyChannel(t *testing.T) {
	gate, err := traffic.NewGate(nil) // disabled
	require.NoError(t, err)
	b := newTrafficBulker(t, gate, newSequencedTransport())
	assert.False(t, b.trafficEnabled())

	blk := b.newBlk(ActionSearch, optionsT{trafficClassSet: true, trafficClass: traffic.Background})
	go func() { b.dispatch(context.Background(), blk) }()
	select {
	case got := <-b.ch:
		assert.Same(t, blk, got)
		got.ch <- respT{}
	case <-b.chControl:
		t.Fatal("disabled gate must use the legacy channel")
	case <-b.chBackground:
		t.Fatal("disabled gate must use the legacy channel")
	case <-time.After(time.Second):
		t.Fatal("dispatch did not reach the legacy channel")
	}
}

// TestForceFlushGateTimeoutKeepsRunAlive covers the regression where a
// forced background flush, unable to get a permit because long control
// flushes hold the capacity, received a gate timeout and killed the whole
// Run loop. A gate-side timeout must only defer the background queue.
func TestForceFlushGateTimeoutKeepsRunAlive(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled:               true,
		Capacity:              1,
		ReservedControl:       1,
		BackgroundWaitTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	tr := newSequencedTransport()
	b := newTrafficBulker(t, gate, tr,
		WithFlushInterval(15*time.Millisecond),
		WithTrafficScheduling(gate, 0, 30*time.Millisecond))
	ctx, cancel := runBulker(t, b)
	defer cancel()

	bgCtx := traffic.WithClass(ctx, traffic.Background)
	errCh := make(chan error, 3)
	go func() { _, e := b.Search(bgCtx, "idx", []byte(`{}`)); errCh <- e }()

	// Wait for the first flush to start, then send the second background
	// block so it lands in a separate queue batch (not merged into the
	// in-flight msearch).
	require.Eventually(t, func() bool { return len(tr.snapshot()) == 1 }, time.Second, time.Millisecond)
	go func() { _, e := b.Search(bgCtx, "idx", []byte(`{}`)); errCh <- e }()

	// Let several force-deadlines expire while the permit is held by the
	// long first flush.
	time.Sleep(400 * time.Millisecond)

	// Control request: Run must still be alive and must be blocked acquiring
	// the flush permit for the control queue.
	go func() { _, e := b.Create(ctx, "test", "1", []byte(`{"a":1}`)); errCh <- e }()
	require.Eventually(t, func() bool { return gate.Stats().Control.Waiting == 1 },
		2*time.Second, time.Millisecond, "Run loop must still schedule control work")

	tr.release <- struct{}{} // first msearch completes; control acquires next
	tr.release <- struct{}{} // bulk control flush
	require.Eventually(t, func() bool {
		got := tr.snapshot()
		return len(got) == 3 && got[1] == "bulk"
	}, 2*time.Second, time.Millisecond)
	tr.release <- struct{}{} // deferred background msearch

	for range 3 {
		select {
		case e := <-errCh:
			assert.NoError(t, e)
		case <-time.After(3 * time.Second):
			t.Fatal("callers did not finish: Run loop likely exited")
		}
	}
}

// TestDisableFlushesDeferredBackgroundQueue ensures an enable->disable
// toggle does not strand a queue that was deferred only because it lacked a
// background permit.
func TestDisableFlushesDeferredBackgroundQueue(t *testing.T) {
	gate, err := traffic.NewGate(&traffic.Snapshot{
		Enabled:               true,
		Capacity:              1,
		ReservedControl:       1,
		BackgroundWaitTimeout: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	tr := newSequencedTransport()
	// Flush delay larger than the test window: while enabled the second
	// queue must remain deferred.
	b := newTrafficBulker(t, gate, tr,
		WithFlushInterval(15*time.Millisecond),
		WithTrafficScheduling(gate, 0, time.Hour))
	ctx, cancel := runBulker(t, b)
	defer cancel()

	bgCtx := traffic.WithClass(ctx, traffic.Background)
	errCh := make(chan error, 2)
	go func() { _, e := b.Search(bgCtx, "idx", []byte(`{}`)); errCh <- e }()
	require.Eventually(t, func() bool { return len(tr.snapshot()) == 1 }, time.Second, time.Millisecond)

	// Refresh create goes to kQueueRefreshBulk; no permit left -> retained.
	go func() { _, e := b.Create(bgCtx, "test", "2", []byte(`{"a":1}`), WithRefresh()); errCh <- e }()
	time.Sleep(100 * time.Millisecond)
	require.Len(t, tr.snapshot(), 1, "deferred queue must not flush while enabled")

	// Turning the gate off must drain the retained queue on the next cycle.
	require.NoError(t, gate.Apply(&traffic.Snapshot{Enabled: false}))
	require.Eventually(t, func() bool {
		got := tr.snapshot()
		return len(got) == 2 && got[1] == "bulk"
	}, 2*time.Second, time.Millisecond, "retained queue stranded after disable")

	tr.release <- struct{}{}
	tr.release <- struct{}{}
	for range 2 {
		select {
		case e := <-errCh:
			assert.NoError(t, e)
		case <-time.After(2 * time.Second):
			t.Fatal("callers did not finish")
		}
	}
}

func TestReconfigureTrafficSettings(t *testing.T) {
	gate, err := traffic.NewGate(nil)
	require.NoError(t, err)
	b := newTrafficBulker(t, gate, newSequencedTransport(),
		WithTrafficScheduling(gate, 7, 100*time.Millisecond))

	assert.Equal(t, int64(7), b.bgDispatchLimit())
	assert.Equal(t, 100*time.Millisecond, b.bgFlushDelay())

	b.ReconfigureTraffic(3, 250*time.Millisecond)
	assert.Equal(t, int64(3), b.bgDispatchLimit())
	assert.Equal(t, 250*time.Millisecond, b.bgFlushDelay())

	// Zero values fall back to bulker options.
	b.ReconfigureTraffic(0, 0)
	assert.Equal(t, b.opts.maxPendingBulkDispatches, b.bgDispatchLimit())
	assert.Equal(t, 2*b.opts.flushInterval, b.bgFlushDelay())
}
