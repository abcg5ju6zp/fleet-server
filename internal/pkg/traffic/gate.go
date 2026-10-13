// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package traffic

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrCapacityTimeout is returned to background requests that wait longer
	// than the configured background wait timeout.
	ErrCapacityTimeout = errors.New("traffic gate: timed out waiting for capacity")
	// ErrCapacityRejected is returned when a request is rejected immediately,
	// e.g. the background waiter queue exceeds its configured maximum.
	ErrCapacityRejected = errors.New("traffic gate: request rejected by capacity gate")
)

// admitMode classifies how a permit was granted.
type admitMode uint8

const (
	modeReserved admitMode = iota // control using its own reserve
	modeFloor                     // background using its own floor
	modeBorrowed                  // a class using the other class's idle share
)

// Reject reasons indexed in Gate.rejected.
const (
	rejTimeout = iota
	rejWaiters
	rejCanceled
)

// Ticket represents one acquired permit. Release must be called once. A ticket
// keeps the class and mode it was admitted with for its whole life:
// configuration swaps never reclassify or revoke admitted work.
type Ticket struct {
	gate     *Gate
	class    Class
	borrowed bool
	once     sync.Once
}

// Class returns the class the ticket was acquired for.
func (t *Ticket) Class() Class { return t.class }

// Borrowed reports whether the ticket uses the other class's reserved share.
func (t *Ticket) Borrowed() bool { return t.borrowed }

// Release returns the permit to the gate. It is safe to call more than once;
// subsequent calls are no-ops. On a disabled-gate no-op ticket Release is a
// no-op as well.
func (t *Ticket) Release() {
	t.once.Do(func() {
		if t.gate != nil {
			t.gate.release(t)
		}
	})
}

type waiter struct {
	class Class
	ch    chan struct{}
	at    time.Time
	grant *Ticket
	elem  *list.Element
}

// disabledTicket is returned by every Acquire/TryAcquire when the gate is
// off, keeping the off-mode fast path allocation free.
var disabledTicket = &Ticket{}

// Observer receives gate lifecycle events. Implementations must be cheap and
// must not call back into the gate (events are emitted under the gate lock).
type Observer interface {
	// Admitted is called when a waiter receives a permit. waited is the time
	// spent in the FIFO queue (zero for an immediate admit).
	Admitted(class Class, borrowed bool, waited time.Duration)
	// Released is called when a ticket is returned.
	Released(class Class, borrowed bool)
	// Rejected is called when a waiter gives up (timeout, waiter cap or
	// context cancellation).
	Rejected(class Class, reason string)
	// Queued is called when a waiter enters (+1) or leaves without a grant
	// (-1).
	Queued(class Class, delta int)
}

// Gate is a priority capacity gate with a reserved control share R and a
// background floor S = C - R.
//
// Admission (evaluated under one lock by pumpLocked):
//   - control takes a free reserve permit first;
//   - background takes a free floor permit first, even while control is
//     waiting (the floor share is owned by background);
//   - either class may borrow the other's idle share only when the other
//     class has no waiters;
//   - after any release, waiting control requests are considered before
//     background borrowing, so a saturated background fleet cannot hold
//     control requests above their bounded wait once permits free up.
//
// All state transitions happen under a single mutex; waiters park on their
// own channel so cancellation and timeouts never busy wait.
type Gate struct {
	mu       sync.Mutex
	snap     *Snapshot
	inflight [2]int64
	borrowed [2]int64

	queues [2]*list.List

	admitted [2][3]int64 // class x admitMode
	rejected [2][3]int64 // class x rejectReason
	waitCnt  [2]int64
	waitNs   [2]int64

	observer Observer
}

// NewGate creates a gate from an initial snapshot. A nil or disabled snapshot
// produces a gate whose Acquire/TryAcquire are no-ops until Apply enables it.
func NewGate(snap *Snapshot) (*Gate, error) {
	if err := snap.Validate(); err != nil {
		return nil, err
	}
	g := &Gate{}
	g.queues[Control] = list.New()
	g.queues[Background] = list.New()
	if snap != nil {
		g.snap = cloneSnapshot(snap)
	}
	return g, nil
}

func cloneSnapshot(s *Snapshot) *Snapshot {
	c := *s
	return &c
}

// SetObserver installs the metrics observer. It is intended for wiring at
// startup; observers must not call back into the gate.
func (g *Gate) SetObserver(o Observer) {
	g.mu.Lock()
	g.observer = o
	g.mu.Unlock()
}

func (g *Gate) emitAdmit(w *waiter, borrowed bool) {
	if g.observer != nil {
		g.observer.Queued(w.class, -1)
		g.observer.Admitted(w.class, borrowed, time.Since(w.at))
	}
}

func (g *Gate) emitReject(class Class, reason string) {
	if g.observer != nil {
		g.observer.Queued(class, -1)
		g.observer.Rejected(class, reason)
	}
}

// Reject reason constants used by Observer.Rejected.
const (
	ReasonTimeout  = "timeout"
	ReasonWaiters  = "waiters"
	ReasonCanceled = "canceled"
)

// CurrentSnapshot returns a copy of the rules currently in force.
func (g *Gate) CurrentSnapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.snap == nil {
		return Snapshot{}
	}
	return *g.snap
}

// Enabled reports whether the gate is currently enforcing an enabled
// snapshot.
func (g *Gate) Enabled() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.snap != nil && g.snap.Enabled
}

// Apply atomically replaces the gate rules. Already admitted tickets run to
// completion under their acquired class/mode; new admissions use the new
// snapshot. An invalid snapshot is rejected and the previous rules stay in
// force. Applying a disabled snapshot unblocks every queued waiter so that
// turning the feature off never stalls in-flight requests.
func (g *Gate) Apply(snap *Snapshot) error {
	if snap == nil {
		snap = &Snapshot{Enabled: false}
	}
	if err := snap.Validate(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	g.snap = cloneSnapshot(snap)

	if !snap.Enabled {
		// Unblock anything queued under the previous enabled snapshot.
		g.grantAllLocked()
		return nil
	}
	g.pumpLocked()
	return nil
}

// Acquire obtains a permit for class. Control requests queue with priority
// and are bounded only by ctx. Background requests additionally honor the
// snapshot's wait timeout and waiter cap. A disabled gate returns a no-op
// ticket.
func (g *Gate) Acquire(ctx context.Context, class Class) (*Ticket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	g.mu.Lock()
	if g.snap == nil || !g.snap.Enabled {
		g.mu.Unlock()
		return disabledTicket, nil
	}
	snap := g.snap

	if class == Background && snap.MaxBackgroundWaiters > 0 && g.queues[Background].Len() >= snap.MaxBackgroundWaiters {
		g.rejected[class][rejWaiters]++
		if g.observer != nil {
			g.observer.Rejected(class, ReasonWaiters)
		}
		g.mu.Unlock()
		return nil, ErrCapacityRejected
	}

	w := &waiter{class: class, ch: make(chan struct{}), at: time.Now()}
	w.elem = g.queues[class].PushBack(w)
	if g.observer != nil {
		g.observer.Queued(class, 1)
	}
	g.pumpLocked()
	granted := w.grant != nil
	waitTimeout := snap.BackgroundWaitTimeout
	g.mu.Unlock()

	if granted {
		return w.grant, nil
	}

	var timerC <-chan time.Time
	var timer *time.Timer
	if class == Background && waitTimeout > 0 {
		timer = time.NewTimer(waitTimeout)
		timerC = timer.C
	}

	for {
		select {
		case <-w.ch:
			return w.grant, nil
		case <-ctx.Done():
			g.mu.Lock()
			if t := w.grant; t != nil {
				// Granted concurrently; honor the ticket.
				g.mu.Unlock()
				return t, nil
			}
			g.removeWaiterLocked(w)
			g.rejected[class][rejCanceled]++
			g.emitReject(class, ReasonCanceled)
			g.pumpLocked()
			g.mu.Unlock()
			return nil, ctx.Err()
		case <-timerC:
			g.mu.Lock()
			if t := w.grant; t != nil {
				g.mu.Unlock()
				return t, nil
			}
			g.removeWaiterLocked(w)
			g.rejected[class][rejTimeout]++
			g.emitReject(class, ReasonTimeout)
			g.pumpLocked()
			g.mu.Unlock()
			if timer != nil {
				timer.Stop()
			}
			return nil, ErrCapacityTimeout
		}
	}
}

// TryAcquire obtains a permit without queueing. It returns false if the
// request cannot be admitted immediately. It is used by the single-goroutine
// bulk scheduler to defer background-only flushes instead of stalling the
// control queue.
func (g *Gate) TryAcquire(class Class) (*Ticket, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.snap == nil || !g.snap.Enabled {
		return disabledTicket, true
	}
	w := &waiter{class: class, ch: make(chan struct{}), at: time.Now()}
	w.elem = g.queues[class].PushBack(w)
	if g.observer != nil {
		g.observer.Queued(class, 1)
	}
	g.pumpLocked()
	if t := w.grant; t != nil {
		return t, true
	}
	g.removeWaiterLocked(w)
	if g.observer != nil {
		g.observer.Queued(class, -1)
	}
	return nil, false
}

// Stats is a point-in-time observability snapshot of a gate.
type Stats struct {
	Enabled            bool
	Capacity           int64
	ReservedControl    int64
	ReservedBackground int64
	Control            ClassStats
	Background         ClassStats
}

// ClassStats holds per-class counters and gauges.
type ClassStats struct {
	InFlight         int64
	Borrowed         int64
	Waiting          int
	AdmittedReserved int64
	AdmittedFloor    int64
	AdmittedBorrowed int64
	RejectedTimeout  int64
	RejectedWaiters  int64
	RejectedCanceled int64
	WaitCount        int64
	WaitTotal        time.Duration
}

// Stats returns a consistent snapshot of the gate's counters.
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()

	st := Stats{}
	if g.snap != nil {
		st.Enabled = g.snap.Enabled
		st.Capacity = g.snap.Capacity
		st.ReservedControl = g.snap.ReservedControl
		st.ReservedBackground = g.snap.ReservedBackground()
	}
	for _, c := range []Class{Control, Background} {
		cs := ClassStats{
			InFlight:         g.inflight[c],
			Borrowed:         g.borrowed[c],
			Waiting:          g.queues[c].Len(),
			AdmittedReserved: g.admitted[c][modeReserved],
			AdmittedFloor:    g.admitted[c][modeFloor],
			AdmittedBorrowed: g.admitted[c][modeBorrowed],
			RejectedTimeout:  g.rejected[c][rejTimeout],
			RejectedWaiters:  g.rejected[c][rejWaiters],
			RejectedCanceled: g.rejected[c][rejCanceled],
			WaitCount:        g.waitCnt[c],
			WaitTotal:        time.Duration(g.waitNs[c]),
		}
		if c == Control {
			st.Control = cs
		} else {
			st.Background = cs
		}
	}
	return st
}

func (g *Gate) release(t *Ticket) {
	g.mu.Lock()
	g.inflight[t.class]--
	if t.borrowed {
		g.borrowed[t.class]--
	}
	if g.observer != nil {
		g.observer.Released(t.class, t.borrowed)
	}
	g.pumpLocked()
	g.mu.Unlock()
}

// removeWaiterLocked drops a not-yet-granted waiter from its queue.
func (g *Gate) removeWaiterLocked(w *waiter) {
	if w.elem != nil {
		g.queues[w.class].Remove(w.elem)
		w.elem = nil
	}
}

// grantLocked admits the waiter with the given mode.
func (g *Gate) grantLocked(w *waiter, mode admitMode) {
	g.removeWaiterLocked(w)

	borrowed := mode == modeBorrowed
	t := &Ticket{gate: g, class: w.class, borrowed: borrowed}
	w.grant = t

	g.inflight[w.class]++
	if borrowed {
		g.borrowed[w.class]++
	}
	g.admitted[w.class][mode]++
	g.waitCnt[w.class]++
	g.waitNs[w.class] += int64(time.Since(w.at))

	if g.observer != nil {
		g.emitAdmit(w, borrowed)
	}

	close(w.ch)
}

// grantAllLocked admits every queued waiter, bypassing capacity. It is used
// when the gate is disabled while requests are waiting.
func (g *Gate) grantAllLocked() {
	for _, c := range []Class{Control, Background} {
		for {
			front := g.queues[c].Front()
			if front == nil {
				break
			}
			w := front.Value.(*waiter)
			if c == Control {
				g.grantLocked(w, modeReserved)
			} else {
				g.grantLocked(w, modeFloor)
			}
		}
	}
}

// pumpLocked grants queued requests while rules permit. It is the single
// place encoding admission policy and must be called on every state change
// (enqueue, release, apply) while holding g.mu.
func (g *Gate) pumpLocked() {
	snap := g.snap
	if snap == nil || !snap.Enabled {
		return
	}
	capC := snap.Capacity
	r := snap.ReservedControl
	floor := snap.ReservedBackground()

	for {
		total := g.inflight[Control] + g.inflight[Background]
		controlHead := g.queueHead(Control)
		backgroundHead := g.queueHead(Background)

		switch {
		// 1. Control claims its own reserve.
		case controlHead != nil && total < capC && g.inflight[Control] < r:
			g.grantLocked(controlHead, modeReserved)
		// 2. Background claims its own floor, even with control waiting:
		// the floor share is owned by background and is what prevents a
		// continuous control stream from monopolizing shared capacity.
		case backgroundHead != nil && total < capC && g.inflight[Background] < floor:
			g.grantLocked(backgroundHead, modeFloor)
		// 3. Control borrows idle floor only when no background is queued.
		case controlHead != nil && backgroundHead == nil && total < capC:
			g.grantLocked(controlHead, modeBorrowed)
		// 4. Background borrows idle reserve only when no control is queued.
		case backgroundHead != nil && controlHead == nil && total < capC:
			g.grantLocked(backgroundHead, modeBorrowed)
		default:
			return
		}
	}
}

func (g *Gate) queueHead(c Class) *waiter {
	e := g.queues[c].Front()
	if e == nil {
		return nil
	}
	return e.Value.(*waiter)
}
