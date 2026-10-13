// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package tenant provides the resource-ownership identity used for namespace
// scoped budgets and fair scheduling.
//
// The owner of a request is derived exclusively from an already authenticated
// identity (an enrolled agent's namespaces or an enrollment key's namespaces).
// Requests without a usable namespace are attributed to the shared Default
// owner so that pre-existing, namespace-less endpoints keep working.
package tenant

import (
	"context"
	"sort"
	"strings"
	"time"
)

const (
	// Default is the owner used for legacy agents with no namespaces, static
	// policy token enrollments and unauthenticated / identity-less endpoints.
	Default Owner = "default"

	// separator joins the sorted, de-duplicated namespace list of a
	// multi-namespace agent into a single stable owner key.
	separator = "|"

	// maxComponentLen bounds the length of a single namespace component when
	// sanitizing untrusted input for use as a metrics label or log field.
	maxComponentLen = 128
)

// Owner is the canonical resource-ownership key. It is immutable once derived.
type Owner string

// String returns the owner string.
func (o Owner) String() string {
	return string(o)
}

// IsDefault reports whether the owner is the shared default bucket.
func (o Owner) IsDefault() bool {
	return o == Default
}

// Canonical derives a stable owner key from the authenticated identity's
// namespace list. The list is trimmed, sanitized, sorted and de-duplicated;
// an empty result is attributed to Default.
//
// An agent that belongs to multiple namespaces is charged to a single
// combined owner key (decision D1 in the namespace budget specification).
func Canonical(namespaces []string) Owner {
	cleaned := make([]string, 0, len(namespaces))
	seen := make(map[string]struct{}, len(namespaces))
	for _, ns := range namespaces {
		c := sanitize(ns)
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		cleaned = append(cleaned, c)
	}
	if len(cleaned) == 0 {
		return Default
	}
	sort.Strings(cleaned)
	return Owner(strings.Join(cleaned, separator))
}

// sanitize trims and replaces any character that is unsafe in a metrics label
// value or log field, bounding the component length.
func sanitize(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range ns {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-' || r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
		if b.Len() >= maxComponentLen {
			break
		}
	}
	return b.String()
}

type ctxKey struct{}

// WithOwner returns a context carrying the authenticated request owner.
// It must only be called after authorization succeeds; handlers must not
// derive the owner from unauthenticated input.
func WithOwner(ctx context.Context, owner Owner) context.Context {
	if owner == "" {
		owner = Default
	}
	return context.WithValue(ctx, ctxKey{}, owner)
}

// OwnerFromContext returns the owner attributed to the request. The boolean is
// false when no authenticated owner was attached.
func OwnerFromContext(ctx context.Context) (Owner, bool) {
	owner, ok := ctx.Value(ctxKey{}).(Owner)
	if !ok || owner == "" {
		return "", false
	}
	return owner, true
}

// Principal is the immutable ownership snapshot captured at admission time.
// Requests keep this identity for their whole lifetime, including after the
// originating client disconnects or the agent document changes.
type Principal struct {
	owner      Owner
	acquiredAt time.Time
}

// NewPrincipal snapshots an owner at the current time.
func NewPrincipal(owner Owner) Principal {
	if owner == "" {
		owner = Default
	}
	return Principal{owner: owner, acquiredAt: time.Now().UTC()}
}

// Owner returns the snapshotted owner.
func (p Principal) Owner() Owner {
	if p.owner == "" {
		return Default
	}
	return p.owner
}

// AcquiredAt returns when the principal was captured.
func (p Principal) AcquiredAt() time.Time {
	return p.acquiredAt
}
