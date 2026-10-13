// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package traffic

import (
	"errors"
	"time"
)

// Snapshot is an immutable set of gate rules. A gate applies each snapshot
// atomically: already admitted tickets keep their class and mode until they
// are released, while new admissions use the newest snapshot.
type Snapshot struct {
	// Enabled turns gating on. A disabled gate admits everything through
	// its no-op fast path.
	Enabled bool

	// Capacity is the total number of concurrent permits.
	Capacity int64

	// ReservedControl is the share owned by control traffic (R). Background
	// traffic may borrow unused reserved permits, but new borrowing stops as
	// soon as a control request is waiting.
	ReservedControl int64

	// BackgroundWaitTimeout bounds how long a background request waits for a
	// permit before it is rejected. Zero means the caller context is the only
	// bound. Control requests are never timed out by the gate.
	BackgroundWaitTimeout time.Duration

	// MaxBackgroundWaiters, when positive, rejects a new background request
	// immediately once the background wait queue reaches this length. Zero
	// means unbounded.
	MaxBackgroundWaiters int
}

// Validate checks the snapshot. Disabled snapshots are always valid so that
// turning the feature off can never fail a reload.
func (s *Snapshot) Validate() error {
	if s == nil || !s.Enabled {
		return nil
	}
	var errs []error
	if s.Capacity <= 0 {
		errs = append(errs, errors.New("capacity must be positive when the gate is enabled"))
	}
	if s.ReservedControl <= 0 {
		errs = append(errs, errors.New("control reservation must be positive when the gate is enabled"))
	}
	if s.Capacity > 0 && s.ReservedControl > s.Capacity {
		errs = append(errs, errors.New("control reservation must not exceed capacity"))
	}
	if s.BackgroundWaitTimeout < 0 {
		errs = append(errs, errors.New("background wait timeout must not be negative"))
	}
	if s.MaxBackgroundWaiters < 0 {
		errs = append(errs, errors.New("max background waiters must not be negative"))
	}
	return errors.Join(errs...)
}

// ReservedBackground returns the background floor share (C - R).
func (s *Snapshot) ReservedBackground() int64 {
	if s == nil || s.Capacity == 0 {
		return 0
	}
	r := s.Capacity - s.ReservedControl
	if r < 0 {
		return 0
	}
	return r
}
