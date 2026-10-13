// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package traffic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recObserver struct {
	mu         sync.Mutex
	queued     int
	admits     []admitEvent
	releases   int
	rejections []string
}

type admitEvent struct {
	class    Class
	borrowed bool
}

func (r *recObserver) Admitted(c Class, borrowed bool, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admits = append(r.admits, admitEvent{c, borrowed})
}

func (r *recObserver) Released(c Class, borrowed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = c
	_ = borrowed
	r.releases++
}

func (r *recObserver) Rejected(c Class, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = c
	r.rejections = append(r.rejections, reason)
}

func (r *recObserver) Queued(_ Class, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queued += delta
}

func (r *recObserver) snapshot() (queued int, admits []admitEvent, releases int, rejections []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]admitEvent, len(r.admits))
	copy(out, r.admits)
	rj := make([]string, len(r.rejections))
	copy(rj, r.rejections)
	return r.queued, out, r.releases, rj
}

func TestGateObserverEvents(t *testing.T) {
	g, err := NewGate(&Snapshot{
		Enabled:               true,
		Capacity:              2,
		ReservedControl:       1,
		BackgroundWaitTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	obs := &recObserver{}
	g.SetObserver(obs)

	t1 := mustAcquire(t, g, context.Background(), Background) // floor
	t2 := mustAcquire(t, g, context.Background(), Background) // borrowed

	// Third background times out while the gate is saturated.
	_, err = g.Acquire(context.Background(), Background)
	require.ErrorIs(t, err, ErrCapacityTimeout)

	// Free a permit: a control request is admitted from the reserve.
	t1.Release()
	c1 := mustAcquire(t, g, context.Background(), Control)
	c1.Release()
	t2.Release()

	queued, admits, releases, rejections := obs.snapshot()
	assert.Equal(t, 0, queued, "queue events balanced")
	require.Len(t, admits, 3)
	assert.Equal(t, admitEvent{Background, false}, admits[0])
	assert.Equal(t, admitEvent{Background, true}, admits[1])
	assert.Equal(t, admitEvent{Control, false}, admits[2])
	assert.Equal(t, 3, releases)
	assert.Equal(t, []string{ReasonTimeout}, rejections)
}

func TestGateObserverMaxWaitersRejectEvent(t *testing.T) {
	g, err := NewGate(&Snapshot{
		Enabled: true, Capacity: 2, ReservedControl: 1,
		BackgroundWaitTimeout: 0, MaxBackgroundWaiters: 1,
	})
	require.NoError(t, err)
	obs := &recObserver{}
	g.SetObserver(obs)

	t1 := mustAcquire(t, g, context.Background(), Background)
	t2 := mustAcquire(t, g, context.Background(), Background)

	park := make(chan struct{})
	go func() { _, _ = g.Acquire(bgCtx{done: park}, Background) }()
	waitFor(t, time.Second, func() bool { return g.Stats().Background.Waiting == 1 })

	_, err = g.Acquire(context.Background(), Background)
	require.ErrorIs(t, err, ErrCapacityRejected)
	_, _, _, rejections := obs.snapshot()
	assert.Contains(t, rejections, ReasonWaiters)
	queued, _, _, _ := obs.snapshot()
	assert.Equal(t, 1, queued, "only the parked waiter is queued, the rejected request never was")

	t1.Release()
	t2.Release()
	close(park)
	waitFor(t, time.Second, func() bool {
		q, _, _, _ := obs.snapshot()
		return q == 0
	})
}
