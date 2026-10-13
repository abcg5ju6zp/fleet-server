// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
)

// WithBudgets injects the process-wide namespace budget registry into the
// API server, shared by both listeners.
func WithBudgets(b *budget.Registry) APIOpt {
	return func(a *apiServer) {
		a.budgets = b
	}
}

// WithCheckinBudgets injects the budget registry into the check-in handler.
func WithCheckinBudgets(b *budget.Registry) CheckinOption {
	return func(ct *CheckinT) {
		ct.budgets = b
	}
}

// WithEnrollerBudgets injects the budget registry into the enroll handler.
func WithEnrollerBudgets(b *budget.Registry) func(*EnrollerT) {
	return func(et *EnrollerT) {
		et.budgets = b
	}
}

// WithArtifactBudgets injects the budget registry into the artifact handler.
func WithArtifactBudgets(b *budget.Registry) func(*ArtifactT) {
	return func(at *ArtifactT) {
		at.budgets = b
	}
}

// WithAckBudgets injects the budget registry into the ack handler.
func WithAckBudgets(b *budget.Registry) func(*AckT) {
	return func(ack *AckT) {
		ack.budgets = b
	}
}

// WithStatusBudgets injects the budget registry into the status handler.
func WithStatusBudgets(b *budget.Registry) OptFunc {
	return func(st *StatusT) {
		st.budgets = b
	}
}

// WithOpAMPBudgets injects the budget registry into the OpAMP handler.
func WithOpAMPBudgets(b *budget.Registry) func(*OpAMPT) {
	return func(oa *OpAMPT) {
		oa.budgets = b
	}
}

// WithUploadBudgets injects the budget registry into the upload handler.
func WithUploadBudgets(b *budget.Registry) func(*UploadT) {
	return func(ut *UploadT) {
		ut.budgets = b
	}
}

// WithFileDeliveryBudgets injects the budget registry into the file delivery
// handler.
func WithFileDeliveryBudgets(b *budget.Registry) func(*FileDeliveryT) {
	return func(ft *FileDeliveryT) {
		ft.budgets = b
	}
}

// WithPGPBudgets injects the budget registry into the PGP retrieval handler.
func WithPGPBudgets(b *budget.Registry) func(*PGPRetrieverT) {
	return func(pt *PGPRetrieverT) {
		pt.budgets = b
	}
}

// WithAuditBudgets injects the budget registry into the audit handler.
func WithAuditBudgets(b *budget.Registry) func(*AuditT) {
	return func(audit *AuditT) {
		audit.budgets = b
	}
}
