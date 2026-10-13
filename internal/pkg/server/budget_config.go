// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package server

import (
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
)

// dispatchQueueCapacity mirrors the unexported bulker entry-channel size
// (bulk.defaultBlockQueueSz); the production wiring never overrides it.
const dispatchQueueCapacity int64 = 32

// buildBudgetConfig translates server configuration into the resource-
// agnostic budget registry configuration. Resources whose global ceiling is
// configured as zero stay unmanaged, preserving the pre-existing
// "unlimited" semantics of each route limiter.
func buildBudgetConfig(cfg *config.Config) budget.Config {
	in := &cfg.Inputs[0]
	srv := &in.Server
	nb := srv.NamespaceBudgets

	out := budget.Config{
		Enabled:          nb.Enabled,
		IdleTTL:          nb.IdleTTL,
		ReclaimTTL:       nb.ReclaimTTL,
		MaxTrackedOwners: nb.MaxTrackedNamespaces,
		Backoff: budget.BackoffConfig{
			Initial:          nb.Backoff.Initial,
			Max:              nb.Backoff.Max,
			FailureThreshold: nb.Backoff.FailureThreshold,
		},
		Resources: map[budget.Resource]budget.ResourceConfig{},
		Overrides: map[string]budget.OwnerOverride{},
	}
	for name, ov := range nb.Overrides {
		out.Overrides[name] = budget.OwnerOverride{
			Weight:    ov.Weight,
			Guarantee: ov.Guarantee,
		}
	}

	ratio := nb.BorrowMaxRatio
	add := func(res budget.Resource, capacity int64) {
		if !nb.Enabled || capacity <= 0 {
			return
		}
		rc := budget.ResourceConfig{
			Enabled:      true,
			Capacity:     capacity,
			GuaranteeMin: nb.GuaranteeMin,
			GuaranteeMax: nb.GuaranteeMax,
			BorrowMax:    borrowSlots(capacity, ratio),
		}
		if tuned, ok := nb.Resources[string(res)]; ok {
			if tuned.Enabled != nil {
				rc.Enabled = *tuned.Enabled
			}
			if tuned.GuaranteeMin > 0 {
				rc.GuaranteeMin = tuned.GuaranteeMin
			}
			if tuned.GuaranteeMax > 0 {
				rc.GuaranteeMax = tuned.GuaranteeMax
			}
			if tuned.BorrowRatio > 0 {
				rc.BorrowMax = borrowSlots(capacity, tuned.BorrowRatio)
			}
		}
		out.Resources[res] = rc
	}

	lim := srv.Limits
	add(budget.ResourceCheckin, lim.CheckinLimit.Max)
	add(budget.ResourceAck, lim.AckLimit.Max)
	add(budget.ResourceArtifact, lim.ArtifactLimit.Max)
	add(budget.ResourceEnroll, lim.EnrollLimit.Max)
	add(budget.ResourceStatus, lim.StatusLimit.Max)
	add(budget.ResourceOpAMP, lim.OpAMPLimit.Max)
	add(budget.ResourceUploadBegin, lim.UploadStartLimit.Max)
	add(budget.ResourceUploadChunk, lim.UploadChunkLimit.Max)
	add(budget.ResourceUploadComplete, lim.UploadEndLimit.Max)
	add(budget.ResourceFileDelivery, lim.DeliverFileLimit.Max)
	add(budget.ResourcePGP, lim.GetPGPKey.Max)
	add(budget.ResourceAudit, lim.AuditUnenrollLimit.Max)

	add(budget.ResourceBulkDispatch, dispatchQueueCapacity)
	add(budget.ResourceBulkFlush, int64(srv.Bulk.FlushMaxPending))
	add(budget.ResourcePolicyDispatch, int64(srv.Bulk.FlushMaxPending))
	add(budget.ResourceActionDispatch, int64(srv.Bulk.FlushMaxPending))
	add(budget.ResourceActionFanout, int64(srv.Bulk.FlushMaxPending))
	add(budget.ResourceCacheCost, in.Cache.MaxCost)

	return out.Defaulted()
}

func borrowSlots(capacity int64, ratio float64) int64 {
	if ratio <= 0 {
		return 0
	}
	if ratio >= 1 {
		return capacity
	}
	slots := int64(float64(capacity)*ratio + 0.5)
	if slots < 0 {
		slots = 0
	}
	if slots > capacity {
		slots = capacity
	}
	return slots
}
