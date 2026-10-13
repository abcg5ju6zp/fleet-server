// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import (
	"errors"
	"time"
)

// Reason classifies a budget rejection. The existing route rate limiter keeps
// owning the "rate" reason; the budget kernel rejects for concurrency
// (max/borrow) and per-owner backoff reasons.
type Reason string

const (
	// ReasonMax means the global resource capacity is exhausted.
	ReasonMax Reason = "max"
	// ReasonBorrow means the owner is beyond its guaranteed share and the
	// idle share it tried to borrow is reserved by a contending owner.
	ReasonBorrow Reason = "borrow"
	// ReasonBackoff means the owner is in its failure cooldown window.
	ReasonBackoff Reason = "backoff"
)

// RejectError is returned by a failed, non-blocking admission. It carries the
// machine-readable reason and the suggested delay before the owner retries.
type RejectError struct {
	Reason     Reason
	RetryAfter time.Duration
}

// Error implements error.
func (e *RejectError) Error() string {
	switch e.Reason {
	case ReasonBackoff:
		return "namespace budget: owner is backed off"
	case ReasonBorrow:
		return "namespace budget: guaranteed share contention, borrow rejected"
	default:
		return "namespace budget: global capacity exhausted"
	}
}

// Is lets errors.Is match rejections regardless of their payload.
func (e *RejectError) Is(target error) bool {
	var other *RejectError
	if errors.As(target, &other) {
		return other.Reason == "" || other.Reason == e.Reason
	}
	return false
}

// As extracts any *RejectError from err.
func As(err error) (*RejectError, bool) {
	var rej *RejectError
	if errors.As(err, &rej) {
		return rej, true
	}
	return nil, false
}
