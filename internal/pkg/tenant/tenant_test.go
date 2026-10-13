// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package tenant

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonical(t *testing.T) {
	cases := []struct {
		name       string
		namespaces []string
		want       Owner
	}{
		{"nil", nil, Default},
		{"empty", []string{}, Default},
		{"only blanks", []string{"  ", ""}, Default},
		{"single", []string{"prod"}, Owner("prod")},
		{"trimmed, sorted and deduped", []string{"b", "a", "a", " a "}, Owner("a|b")},
		{"inner space sanitized", []string{"a b", "a", "b"}, Owner("a|a_b|b")},
		{"empty element falls back to default", []string{"", " "}, Default},
		{"uppercase normalized", []string{"Prod", "DEV"}, Owner("dev|prod")},
		{"unsafe chars replaced", []string{"a/b", "c.d"}, Owner("a_b|c_d")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, Canonical(tc.namespaces))
		})
	}
}

func TestCanonicalStable(t *testing.T) {
	assert.Equal(t, Canonical([]string{"z", "a", "m"}), Canonical([]string{"m", "z", "a"}))
}

func TestOwnerFromContext(t *testing.T) {
	ctx := context.Background()

	_, ok := OwnerFromContext(ctx)
	assert.False(t, ok)

	ctx = WithOwner(ctx, "space-a")
	owner, ok := OwnerFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, Owner("space-a"), owner)

	// An empty owner is normalized to the default bucket.
	owner, ok = OwnerFromContext(WithOwner(context.Background(), ""))
	require.True(t, ok)
	assert.Equal(t, Default, owner)
}

func TestPrincipalSnapshot(t *testing.T) {
	p := NewPrincipal(Canonical([]string{"b", "a"}))
	assert.Equal(t, Owner("a|b"), p.Owner())
	assert.WithinDuration(t, time.Now(), p.AcquiredAt(), time.Second)

	d := NewPrincipal("")
	assert.Equal(t, Default, d.Owner())
	assert.True(t, d.Owner().IsDefault())
}
