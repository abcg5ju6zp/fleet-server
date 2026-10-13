// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package traffic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func enabledSnapshot() *Snapshot {
	return &Snapshot{
		Enabled:             true,
		Capacity:            4,
		ReservedControl:     2,
		BackgroundWaitTimeout: time.Second,
	}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !fn() {
		t.Fatal("condition not met before timeout")
	}
}

func TestClassFromContext(t *testing.T) {
	assert.Equal(t, "control", Control.String())
	assert.Equal(t, "background", Background.String())

	assert.Equal(t, Control, FromContext(context.Background()))
	assert.Equal(t, Background, FromContext(WithClass(context.Background(), Background)))
	assert.Equal(t, Control, FromContext(nil))
}

func TestSnapshotValidate(t *testing.T) {
	require.NoError(t, (&Snapshot{}).Validate()) // disabled is always valid

	cases := []struct {
		name string
		snap Snapshot
	}{
		{"zero capacity", Snapshot{Enabled: true}},
		{"zero reserve", Snapshot{Enabled: true, Capacity: 4}},
		{"reserve exceeds capacity", Snapshot{Enabled: true, Capacity: 4, ReservedControl: 5}},
		{"negative timeout", Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2, BackgroundWaitTimeout: -1}},
		{"negative waiters", Snapshot{Enabled: true, Capacity: 4, ReservedControl: 2, MaxBackgroundWaiters: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.snap.Validate())
		})
	}

	good := Snapshot{Enabled: true, Capacity: 4, ReservedControl: 4}
	require.NoError(t, good.Validate())
	assert.Equal(t, int64(0), good.ReservedBackground())
}

func TestGateDisabledIsNoop(t *testing.T) {
	g, err := NewGate(nil)
	require.NoError(t, err)
	assert.False(t, g.Enabled())

	for range 10 {
		tk, err := g.Acquire(context.Background(), Background)
		require.NoError(t, err)
		assert.False(t, tk.Borrowed())
		defer tk.Release()
	}
	tk, ok := g.TryAcquire(Control)
	require.True(t, ok)
	tk.Release()

	st := g.Stats()
	assert.False(t, st.Enabled)
	assert.Equal(t, int64(0), st.Background.InFlight)
}

func TestBackgroundUsesFloorThenBorrows(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	t1 := mustAcquire(t, g, context.Background(), Background)
	t2 := mustAcquire(t, g, context.Background(), Background)
	assert.False(t, t1.Borrowed())
	assert.False(t, t2.Borrowed())

	t3 := mustAcquire(t, g, context.Background(), Background)
	t4 := mustAcquire(t, g, context.Background(), Background)
	assert.True(t, t3.Borrowed())
	assert.True(t, t4.Borrowed())

	st := g.Stats()
	assert.Equal(t, int64(4), st.Background.InFlight)
	assert.Equal(t, int64(2), st.Background.Borrowed)
	assert.Equal(t, int64(2), st.Background.AdmittedFloor)
	assert.Equal(t, int64(2), st.Background.AdmittedBorrowed)

	t1.Release()
	t2.Release()
	t3.Release()
	t4.Release()
	assert.Zero(t, g.Stats().Background.InFlight)
}

func TestControlAdmittedBeforeWaitingBackgroundOnRelease(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	// Background saturates all four permits (2 floor + 2 borrowed).
	var saturated [4]*Ticket
	for i := range saturated {
		saturated[i] = mustAcquire(t, g, context.Background(), Background)
	}

	bgDone := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), Background)
		if err != nil {
			panic(err)
		}
		bgDone <- tk
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	ctlDone := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), Control)
		if err != nil {
			panic(err)
		}
		ctlDone <- tk
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Control.Waiting == 1 })

	// Free one permit: control must win it ahead of the waiting background.
	saturated[0].Release()
	select {
	case tk := <-ctlDone:
		assert.False(t, tk.Borrowed(), "control reclaims its own reserve")
		tk.Release()
	case <-bgDone:
		t.Fatal("background borrowed permit was granted while control was waiting")
	case <-time.After(time.Second):
		t.Fatal("control was not admitted after a permit was released")
	}

	// Second release admits the waiting background.
	saturated[1].Release()
	select {
	case tk := <-bgDone:
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("waiting background was not admitted after a second release")
	}

	for _, tk := range saturated[2:] {
		tk.Release()
	}
}

func TestBackgroundBorrowFreezesWhileControlWaits(t *testing.T) {
	snap := enabledSnapshot()
	snap.BackgroundWaitTimeout = 0 // only context bounds waits here
	g, err := NewGate(snap)
	require.NoError(t, err)

	// Control holds its two reserve permits; background fills the floor.
	var ctl [2]*Ticket
	for i := range ctl {
		ctl[i] = mustAcquire(t, g, context.Background(), Control)
	}
	var bg [2]*Ticket
	for i := range bg {
		bg[i] = mustAcquire(t, g, context.Background(), Background)
	}

	// A third background cannot borrow reserve (full) and queues.
	bgDone := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), Background)
		if err == nil {
			bgDone <- tk
		}
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	// Borrowed background count must not grow while control demand exists.
	assert.Equal(t, int64(0), g.Stats().Background.Borrowed)

	// Release a control permit: the queued background may now borrow it
	// (no control waiters at that point).
	ctl[0].Release()
	select {
	case tk := <-bgDone:
		assert.True(t, tk.Borrowed())
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("background did not get the freed control permit")
	}

	ctl[1].Release()
	bg[0].Release()
	bg[1].Release()
}

func TestControlCannotStealFloorFromWaitingBackground(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	// Control bursts into idle floor and saturates the gate.
	var ctl [4]*Ticket
	for i := range ctl {
		ctl[i] = mustAcquire(t, g, context.Background(), Control)
	}
	st := g.Stats()
	assert.Equal(t, int64(2), st.Control.Borrowed)

	// Background demand arrives and queues.
	bgDone := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), Background)
		if err != nil {
			panic(err)
		}
		bgDone <- tk
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	// A control release goes to the waiting background because the freed
	// permit belongs to the floor share (control already owns its reserve).
	ctl[3].Release()
	select {
	case tk := <-bgDone:
		assert.False(t, tk.Borrowed())
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("waiting background was not given a floor permit")
	}
	assert.Equal(t, int64(0), g.Stats().Background.Borrowed)

	for i := 0; i < 3; i++ {
		ctl[i].Release()
	}
}

func TestBackgroundWaitTimeout(t *testing.T) {
	snap := enabledSnapshot()
	snap.BackgroundWaitTimeout = 20 * time.Millisecond
	g, err := NewGate(snap)
	require.NoError(t, err)

	var hold [4]*Ticket
	for i := range hold {
		hold[i] = mustAcquire(t, g, context.Background(), Background)
	}

	start := time.Now()
	_, err = g.Acquire(context.Background(), Background)
	require.ErrorIs(t, err, ErrCapacityTimeout)
	assert.GreaterOrEqual(t, time.Since(start), 19*time.Millisecond)

	st := g.Stats()
	assert.Equal(t, int64(1), st.Background.RejectedTimeout)
	assert.Equal(t, 0, st.Background.Waiting)

	for _, tk := range hold {
		tk.Release()
	}
}

func TestContextCancelWhileWaitingReleasesSlot(t *testing.T) {
	snap := enabledSnapshot()
	snap.BackgroundWaitTimeout = 0
	g, err := NewGate(snap)
	require.NoError(t, err)

	var hold [4]*Ticket
	for i := range hold {
		hold[i] = mustAcquire(t, g, context.Background(), Background)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := g.Acquire(ctx, Background)
		errCh <- err
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled acquire did not return")
	}
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 0 })
	assert.Equal(t, int64(1), g.Stats().Background.RejectedCanceled)

	// A control request must now behave as if the canceled waiter never
	// existed: it queues until a permit frees.
	ctlDone := make(chan *Ticket, 1)
	go func() {
		tk, err := g.Acquire(context.Background(), Control)
		if err != nil {
			panic(err)
		}
		ctlDone <- tk
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Control.Waiting == 1 })
	hold[0].Release()
	select {
	case tk := <-ctlDone:
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("control not admitted after canceled background waiter")
	}

	for _, tk := range hold[1:] {
		tk.Release()
	}
}

func TestMaxBackgroundWaitersRejectsImmediately(t *testing.T) {
	snap := enabledSnapshot()
	snap.MaxBackgroundWaiters = 1
	g, err := NewGate(snap)
	require.NoError(t, err)

	var hold [4]*Ticket
	for i := range hold {
		hold[i] = mustAcquire(t, g, context.Background(), Background)
	}

	release := make(chan struct{})
	go func() {
		_, _ = g.Acquire(bgCtx{done: release}, Background)
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	_, err = g.Acquire(context.Background(), Background)
	require.ErrorIs(t, err, ErrCapacityRejected)
	assert.Equal(t, int64(1), g.Stats().Background.RejectedWaiters)

	close(release)
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 0 })
	for _, tk := range hold {
		tk.Release()
	}
}

func TestTryAcquire(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	for range 4 {
		_, ok := g.TryAcquire(Background)
		require.True(t, ok)
	}
	_, ok := g.TryAcquire(Background)
	assert.False(t, ok, "saturated gate refuses without queueing")
	_, ok = g.TryAcquire(Control)
	assert.False(t, ok)

	g.Stats() // no waiting entries must have been retained
	assert.Equal(t, 0, g.Stats().Background.Waiting)
}

func TestApplySwapKeepsInFlightTicketsValid(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	t1 := mustAcquire(t, g, context.Background(), Background)
	t2 := mustAcquire(t, g, context.Background(), Control)
	assert.Equal(t, int64(4), g.CurrentSnapshot().Capacity)

	// Invalid snapshot must be rejected and leave rules untouched.
	require.Error(t, g.Apply(&Snapshot{Enabled: true, Capacity: 1, ReservedControl: 2}))
	assert.Equal(t, int64(4), g.CurrentSnapshot().Capacity)

	// Valid swap: old tickets remain functional and release cleanly.
	require.NoError(t, g.Apply(&Snapshot{Enabled: true, Capacity: 10, ReservedControl: 6, BackgroundWaitTimeout: time.Second}))
	assert.Equal(t, int64(10), g.CurrentSnapshot().Capacity)

	t1.Release()
	t2.Release()
	st := g.Stats()
	assert.Zero(t, st.Control.InFlight)
	assert.Zero(t, st.Background.InFlight)

	// New admissions follow the new reserve (6).
	var ctl [6]*Ticket
	for i := range ctl {
		ctl[i] = mustAcquire(t, g, context.Background(), Control)
	}
	defer func() {
		for _, tk := range ctl {
			tk.Release()
		}
	}()
	assert.Equal(t, int64(6), g.Stats().Control.InFlight)
}

func TestApplyDisabledUnblocksWaiters(t *testing.T) {
	g, err := NewGate(enabledSnapshot())
	require.NoError(t, err)

	var hold [4]*Ticket
	for i := range hold {
		hold[i] = mustAcquire(t, g, context.Background(), Background)
	}

	done := make(chan *Ticket, 2)
	go func() {
		tk, err := g.Acquire(context.Background(), Control)
		if err != nil {
			panic(err)
		}
		done <- tk
	}()
	waitFor(t, time.Second, func() bool { return g.Stats().Control.Waiting == 1 })

	require.NoError(t, g.Apply(&Snapshot{Enabled: false}))
	select {
	case tk := <-done:
		tk.Release()
	case <-time.After(time.Second):
		t.Fatal("disabling the gate did not unblock waiters")
	}

	// Disabled gate admits without counting.
	tk, err := g.Acquire(context.Background(), Background)
	require.NoError(t, err)
	tk.Release()
	assert.Zero(t, g.Stats().Background.Waiting)

	for _, h := range hold {
		h.Release()
	}
}

func TestGateStress(t *testing.T) {
	snap := &Snapshot{
		Enabled:               true,
		Capacity:              16,
		ReservedControl:       8,
		BackgroundWaitTimeout: 50 * time.Millisecond,
		MaxBackgroundWaiters:  64,
	}
	g, err := NewGate(snap)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, class := range []Class{Control, Background} {
		for range 8 {
			wg.Add(1)
			go func(class Class) {
				defer wg.Done()
				for ctx.Err() == nil {
					tk, err := g.Acquire(ctx, class)
					if errors.Is(err, ErrCapacityTimeout) || errors.Is(err, ErrCapacityRejected) {
						continue
					}
					if err != nil {
						return
					}
					time.Sleep(time.Millisecond)
					tk.Release()
				}
			}(class)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				i++
				var next *Snapshot
				if i%2 == 0 {
					next = &Snapshot{Enabled: true, Capacity: 16, ReservedControl: 8, BackgroundWaitTimeout: 50 * time.Millisecond, MaxBackgroundWaiters: 64}
				} else {
					next = &Snapshot{Enabled: true, Capacity: 24, ReservedControl: 10, BackgroundWaitTimeout: 30 * time.Millisecond, MaxBackgroundWaiters: 32}
				}
				_ = g.Apply(next)
			}
		}
	}()
	wg.Wait()

	// Give in-flight tickets a moment to be released after ctx timeout.
	waitFor(t, time.Second, func() bool {
		st := g.Stats()
		return st.Control.InFlight == 0 && st.Background.InFlight == 0
	})
	st := g.Stats()
	assert.Zero(t, st.Control.InFlight)
	assert.Zero(t, st.Background.InFlight)
	assert.Zero(t, st.Control.Waiting)
	assert.Zero(t, st.Background.Waiting)
}

func mustAcquire(t *testing.T, g *Gate, ctx context.Context, class Class) *Ticket {
	t.Helper()
	tk, err := g.Acquire(ctx, class)
	require.NoError(t, err)
	return tk
}

// bgCtx is a context that is only ever canceled by test cleanup; it exists
// to keep a waiter parked in TestMaxBackgroundWaiters.
type bgCtx struct {
	done <-chan struct{}
}

func (c bgCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c bgCtx) Done() <-chan struct{}       { return c.done }
func (c bgCtx) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}
func (c bgCtx) Value(any) any { return nil }
