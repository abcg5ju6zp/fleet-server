// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package config

import (
	"fmt"
	"time"
)

// NamespaceBackoff is the per-namespace failure cooldown configuration.
type NamespaceBackoff struct {
	Initial          time.Duration `config:"initial"`
	Max              time.Duration `config:"max"`
	FailureThreshold int           `config:"failure_threshold"`
}

// InitDefaults initializes the defaults for the configuration.
func (c *NamespaceBackoff) InitDefaults() {
	c.Initial = time.Second
	c.Max = 30 * time.Second
	c.FailureThreshold = 5
}

// NamespaceBudgetOverride pins scheduling weight or a fixed guaranteed
// share for one namespace; it is intended for small, operator-known
// deployments.
type NamespaceBudgetOverride struct {
	Weight    float64 `config:"weight"`
	Guarantee int64   `config:"guarantee"`
}

// NamespaceResourceBudget is the per-resource-class budget tuning. Zero
// values fall back to global defaults / derived capacity.
type NamespaceResourceBudget struct {
	// Enabled lets operators turn namespace accounting off for one resource.
	Enabled *bool    `config:"enabled"`
	GuaranteeMin int64   `config:"guarantee_min"`
	GuaranteeMax int64   `config:"guarantee_max"`
	BorrowRatio  float64 `config:"borrow_max_ratio"`
}

// ServerNamespaceBudgets configures namespace fair-share budgets. When
// enabled, each managed resource keeps a global ceiling (the existing
// limits) plus per-namespace guaranteed shares with work-conserving
// borrow that is reclaimed under contention.
type ServerNamespaceBudgets struct {
	// Enabled is the master switch. A single active namespace observes
	// identical behavior with the switch on or off.
	Enabled bool `config:"enabled"`

	// GuaranteeMin/GuaranteeMax clamp dynamically computed shares in
	// absolute slots for resources that do not override them.
	GuaranteeMin int64 `config:"guarantee_min"`
	GuaranteeMax int64 `config:"guarantee_max"`
	// BorrowMaxRatio bounds borrowed slots as a fraction of resource
	// capacity (0..1). Defaults to 1: an idle deployment lends all
	// unused share.
	BorrowMaxRatio float64 `config:"borrow_max_ratio"`

	// IdleTTL evicts zero-use namespaces so shares return to active ones.
	IdleTTL time.Duration `config:"idle_ttl"`
	// ReclaimTTL bounds how long a contention marker blocks borrowing
	// after the contending namespace was last seen.
	ReclaimTTL time.Duration `config:"reclaim_ttl"`
	// MaxTrackedNamespaces bounds metrics label cardinality; namespaces
	// beyond it share the aggregate _other bucket.
	MaxTrackedNamespaces int `config:"max_tracked_namespaces"`

	Backoff NamespaceBackoff `config:"backoff"`

	// Resources optionally tunes individual resource classes. Keys are
	// budget resource names (checkin, ack, bulk_dispatch, ...).
	Resources map[string]NamespaceResourceBudget `config:"resources"`
	// Overrides pins weight/guarantee for named namespaces.
	Overrides map[string]NamespaceBudgetOverride `config:"overrides"`
}

// InitDefaults initializes the defaults for the configuration.
func (c *ServerNamespaceBudgets) InitDefaults() {
	c.Enabled = true
	c.BorrowMaxRatio = 1
	c.IdleTTL = 10 * time.Minute
	c.ReclaimTTL = 30 * time.Second
	c.MaxTrackedNamespaces = 1000
	c.Backoff.InitDefaults()
}

// Validate ensures that the configuration is valid.
func (c *ServerNamespaceBudgets) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.GuaranteeMin < 0 || c.GuaranteeMax < 0 {
		return fmt.Errorf("namespace_budgets guarantees must not be negative, got min=%d max=%d", c.GuaranteeMin, c.GuaranteeMax)
	}
	if c.GuaranteeMax > 0 && c.GuaranteeMin > c.GuaranteeMax {
		return fmt.Errorf("namespace_budgets.guarantee_min (%d) must not exceed guarantee_max (%d)", c.GuaranteeMin, c.GuaranteeMax)
	}
	if c.BorrowMaxRatio < 0 || c.BorrowMaxRatio > 1 {
		return fmt.Errorf("namespace_budgets.borrow_max_ratio must be within [0,1], got %v", c.BorrowMaxRatio)
	}
	if c.IdleTTL < 0 || c.ReclaimTTL < 0 {
		return fmt.Errorf("namespace_budgets TTLs must not be negative, got idle=%s reclaim=%s", c.IdleTTL, c.ReclaimTTL)
	}
	if c.MaxTrackedNamespaces < 0 {
		return fmt.Errorf("namespace_budgets.max_tracked_namespaces must not be negative, got %d", c.MaxTrackedNamespaces)
	}
	if c.Backoff.Initial < 0 || c.Backoff.Max < 0 || c.Backoff.FailureThreshold < 0 {
		return fmt.Errorf("namespace_budgets.backoff values must not be negative")
	}
	if c.Backoff.Max > 0 && c.Backoff.Initial > c.Backoff.Max {
		return fmt.Errorf("namespace_budgets.backoff.initial (%s) must not exceed max (%s)", c.Backoff.Initial, c.Backoff.Max)
	}
	for name, r := range c.Resources {
		if r.GuaranteeMin < 0 || r.GuaranteeMax < 0 {
			return fmt.Errorf("namespace_budgets.resources.%s guarantees must not be negative", name)
		}
		if r.GuaranteeMax > 0 && r.GuaranteeMin > r.GuaranteeMax {
			return fmt.Errorf("namespace_budgets.resources.%s.guarantee_min (%d) must not exceed guarantee_max (%d)", name, r.GuaranteeMin, r.GuaranteeMax)
		}
		if r.BorrowRatio < 0 || r.BorrowRatio > 1 {
			return fmt.Errorf("namespace_budgets.resources.%s.borrow_max_ratio must be within [0,1], got %v", name, r.BorrowRatio)
		}
	}
	for name, o := range c.Overrides {
		if o.Weight < 0 {
			return fmt.Errorf("namespace_budgets.overrides.%s.weight must not be negative, got %v", name, o.Weight)
		}
		if o.Guarantee < 0 {
			return fmt.Errorf("namespace_budgets.overrides.%s.guarantee must not be negative, got %d", name, o.Guarantee)
		}
	}
	return nil
}
