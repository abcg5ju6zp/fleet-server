// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testResourceCheckin = "checkin"
	testOverrideSpaceA  = "space-a"
)

func TestNamespaceBudgetsValidate(t *testing.T) {
	type test struct {
		name string
		cfg  ServerNamespaceBudgets
		ok   bool
	}
	enabled := true
	tests := []test{
		{"disabled is always valid", ServerNamespaceBudgets{Enabled: false}, true},
		{"zero enabled uses defaults at wiring", func() ServerNamespaceBudgets {
			c := ServerNamespaceBudgets{}
			c.InitDefaults()
			return c
		}(), true},
		{"negative guarantee", ServerNamespaceBudgets{Enabled: enabled, GuaranteeMin: -1}, false},
		{"min exceeds max", ServerNamespaceBudgets{Enabled: enabled, GuaranteeMin: 5, GuaranteeMax: 2}, false},
		{"ratio above one", ServerNamespaceBudgets{Enabled: enabled, BorrowMaxRatio: 1.2}, false},
		{"negative ratio", ServerNamespaceBudgets{Enabled: enabled, BorrowMaxRatio: -0.1}, false},
		{"negative ttl", ServerNamespaceBudgets{Enabled: enabled, IdleTTL: -time.Second}, false},
		{"backoff initial exceeds max", ServerNamespaceBudgets{
			Enabled: enabled,
			Backoff: NamespaceBackoff{Initial: 2 * time.Second, Max: time.Second},
		}, false},
		{"resource min exceeds max", ServerNamespaceBudgets{
			Enabled: enabled,
			Resources: map[string]NamespaceResourceBudget{
				testResourceCheckin: {GuaranteeMin: 9, GuaranteeMax: 3},
			},
		}, false},
		{"resource bad ratio", ServerNamespaceBudgets{
			Enabled:   enabled,
			Resources: map[string]NamespaceResourceBudget{testResourceCheckin: {BorrowRatio: 2}},
		}, false},
		{"negative override weight", ServerNamespaceBudgets{
			Enabled:   enabled,
			Overrides: map[string]NamespaceBudgetOverride{testOverrideSpaceA: {Weight: -1}},
		}, false},
		{"valid tuned config", ServerNamespaceBudgets{
			Enabled:        enabled,
			GuaranteeMin:   1,
			GuaranteeMax:   100,
			BorrowMaxRatio: 0.8,
			IdleTTL:        time.Minute,
			ReclaimTTL:     5 * time.Second,
			Backoff:        NamespaceBackoff{Initial: time.Second, Max: 10 * time.Second, FailureThreshold: 3},
			Resources: map[string]NamespaceResourceBudget{
				testResourceCheckin: {GuaranteeMin: 1, GuaranteeMax: 50, BorrowRatio: 0.5},
			},
			Overrides: map[string]NamespaceBudgetOverride{
				testOverrideSpaceA: {Weight: 3, Guarantee: 10},
			},
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "namespace_budgets")
			}
		})
	}
}

func TestNamespaceBudgetsInitDefaults(t *testing.T) {
	c := ServerNamespaceBudgets{}
	c.InitDefaults()
	assert.True(t, c.Enabled, "budgets default to enabled")
	assert.Equal(t, 1.0, c.BorrowMaxRatio)
	assert.Equal(t, 10*time.Minute, c.IdleTTL)
	assert.Equal(t, 30*time.Second, c.ReclaimTTL)
	assert.Equal(t, 1000, c.MaxTrackedNamespaces)
	assert.Equal(t, time.Second, c.Backoff.Initial)
	assert.Equal(t, 30*time.Second, c.Backoff.Max)
	assert.Equal(t, 5, c.Backoff.FailureThreshold)
}

func TestInputValidateIncludesNamespaceBudgets(t *testing.T) {
	in := Input{Type: "fleet-server"}
	require.NoError(t, in.Validate())

	in.Server.NamespaceBudgets = ServerNamespaceBudgets{Enabled: true, BorrowMaxRatio: 2}
	require.Error(t, in.Validate())
}
