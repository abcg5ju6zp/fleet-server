// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package checkin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// TestUniformCheckinOwner covers best-effort timestamp writer
// attribution: single-owner batches carry a WithOwner option; mixed and
// empty batches stay on the shared default bucket.
func TestUniformCheckinOwner(t *testing.T) {
	now := time.Now()
	mk := func(owners ...tenant.Owner) map[string]pendingT {
		out := make(map[string]pendingT, len(owners))
		for i, o := range owners {
			out[string(rune('a'+i))] = pendingT{ts: now.Format(time.RFC3339), owner: o}
		}
		return out
	}

	owner, ok := uniformCheckinOwner(mk("space-a", "space-a"))
	require.True(t, ok)
	assert.Equal(t, tenant.Owner("space-a"), owner)

	_, ok = uniformCheckinOwner(mk("space-a", "space-b"))
	assert.False(t, ok, "mixed owner batch stays unattributed")

	owner, ok = uniformCheckinOwner(map[string]pendingT{})
	require.True(t, ok)
	assert.Equal(t, tenant.Default, owner, "empty batch stays on default")
}
