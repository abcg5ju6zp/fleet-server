// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import "time"

// Resource identifies a managed resource class. Budgets are independent per
// resource: exhausting check-in concurrency does not spend action dispatch
// budget.
type Resource string

const (
	ResourceCheckin        Resource = "checkin"
	ResourceAck            Resource = "ack"
	ResourceArtifact       Resource = "artifact"
	ResourceEnroll         Resource = "enroll"
	ResourceStatus         Resource = "status"
	ResourceOpAMP          Resource = "opamp"
	ResourceUploadBegin    Resource = "upload_begin"
	ResourceUploadChunk    Resource = "upload_chunk"
	ResourceUploadComplete Resource = "upload_complete"
	ResourceFileDelivery   Resource = "file_delivery"
	ResourcePGP            Resource = "pgp"
	ResourceAudit          Resource = "audit_unenroll"
	ResourceBulkDispatch   Resource = "bulk_dispatch"
	ResourceBulkFlush      Resource = "bulk_flush"
	ResourcePolicyDispatch Resource = "policy_dispatch"
	ResourceActionDispatch Resource = "action_dispatch"
	ResourceActionFanout   Resource = "action_fanout"
	ResourceCacheCost      Resource = "cache_cost"
)

// BackoffConfig drives the per-owner failure cooldown. After FailureThreshold
// consecutive reported failures (within the owner's active period), new
// admissions are rejected until the cooldown expires; cooldown grows
// exponentially up to Max.
type BackoffConfig struct {
	Initial          time.Duration `config:"initial"`
	Max              time.Duration `config:"max"`
	FailureThreshold int           `config:"failure_threshold"`
}

// ResourceConfig is the budget configuration for one resource class.
type ResourceConfig struct {
	// Enabled toggles namespace accounting for this resource. When false the
	// resource is unmanaged: every admission succeeds and nothing is tracked.
	Enabled bool `config:"enabled"`
	// Capacity is the global ceiling shared by all owners. Zero disables
	// accounting for the resource (equivalent to Enabled=false).
	Capacity int64 `config:"capacity"`
	// GuaranteeMin/GuaranteeMax clamp each owner's dynamically computed
	// guaranteed share of Capacity.
	GuaranteeMin int64 `config:"guarantee_min"`
	GuaranteeMax int64 `config:"guarantee_max"`
	// BorrowMax is the number of slots beyond its guarantee a single owner
	// may use while other owners are idle. Defaults to Capacity so that a
	// single active owner can use the whole resource.
	BorrowMax int64 `config:"borrow_max"`
}

// OwnerOverride pins the scheduling weight and optionally a fixed guaranteed
// share for one owner. It is intended for small, operator-known namespaces.
type OwnerOverride struct {
	Weight    float64 `config:"weight"`
	Guarantee int64   `config:"guarantee"`
}

// Config configures a Registry.
type Config struct {
	// Enabled is the master kill switch.
	Enabled bool `config:"enabled"`
	// IdleTTL removes zero-use owners not seen for that long, shrinking no
	// one's computed guarantee.
	IdleTTL time.Duration `config:"idle_ttl"`
	// ReclaimTTL bounds how long a contention marker (set when an owner is
	// rejected while below its guarantee) blocks borrow admissions of other
	// owners even if the contending owner goes away without a successful
	// admission.
	ReclaimTTL time.Duration `config:"reclaim_ttl"`
	// MaxTrackedOwners bounds registry cardinality; owners created beyond the
	// cap share the aggregate OtherOwner bucket.
	MaxTrackedOwners int `config:"max_tracked_namespaces"`
	// Backoff applies to every managed resource unless it gets a specific
	// value below.
	Backoff BackoffConfig `config:"backoff"`
	// Resources holds per-resource configuration keyed by Resource.
	Resources map[Resource]ResourceConfig `config:"resources"`
	// Overrides holds per-owner scheduling overrides keyed by owner.
	Overrides map[string]OwnerOverride `config:"overrides"`
}

const (
	defaultIdleTTL          = 10 * time.Minute
	defaultReclaimTTL       = 30 * time.Second
	defaultMaxTrackedOwners = 1000

	defaultBackoffInitial   = time.Second
	defaultBackoffMax       = 30 * time.Second
	defaultBackoffThreshold = 5
)

// Defaulted returns a copy of cfg with the zero-valued knobs filled with
// built-in defaults. Resource entries are filled with defaults individually;
// missing resources stay absent (unmanaged) until the wiring layer registers
// them.
func (cfg Config) Defaulted() Config {
	out := cfg
	if out.IdleTTL <= 0 {
		out.IdleTTL = defaultIdleTTL
	}
	if out.ReclaimTTL <= 0 {
		out.ReclaimTTL = defaultReclaimTTL
	}
	if out.MaxTrackedOwners <= 0 {
		out.MaxTrackedOwners = defaultMaxTrackedOwners
	}
	if out.Backoff.Initial <= 0 {
		out.Backoff.Initial = defaultBackoffInitial
	}
	if out.Backoff.Max <= 0 {
		out.Backoff.Max = defaultBackoffMax
	}
	if out.Backoff.FailureThreshold <= 0 {
		out.Backoff.FailureThreshold = defaultBackoffThreshold
	}
	if out.Resources != nil {
		resources := make(map[Resource]ResourceConfig, len(cfg.Resources))
		for k, v := range cfg.Resources {
			if v.BorrowMax < 0 {
				v.BorrowMax = 0
			}
			if v.BorrowMax == 0 {
				v.BorrowMax = v.Capacity
			}
			if v.GuaranteeMax == 0 {
				v.GuaranteeMax = v.Capacity
			}
			resources[k] = v
		}
		out.Resources = resources
	}
	return out
}

// managed reports whether the resource participates in namespace accounting.
func (rc ResourceConfig) managed(globalEnabled bool) bool {
	return globalEnabled && rc.Enabled && rc.Capacity > 0
}
