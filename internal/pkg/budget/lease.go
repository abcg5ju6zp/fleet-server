// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package budget

import (
	"sync"
	"sync/atomic"

	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

// Lease is an admitted resource reservation. Release must be called exactly
// once for every successful admission; it is safe to call more than once.
type Lease interface {
	// Release returns the claimed slots to the owner's budget.
	Release()
	// Owner returns the owner captured at admission time; it never changes.
	Owner() tenant.Owner
	// Resource returns the resource class.
	Resource() Resource
}

type lease struct {
	once      sync.Once
	released  atomic.Bool
	registry  *Registry
	resource  Resource
	owner     tenant.Owner
	amount    int64
	untracked bool
}

// Release implements Lease.
func (l *lease) Release() {
	l.once.Do(func() {
		l.released.Store(true)
		if !l.untracked {
			l.registry.release(l.resource, l.owner, l.amount)
		}
	})
}

// Owner implements Lease.
func (l *lease) Owner() tenant.Owner { return l.owner }

// Resource implements Lease.
func (l *lease) Resource() Resource { return l.resource }

// noopLease is returned for unmanaged resources.
type noopLease struct {
	owner    tenant.Owner
	resource Resource
}

func (l noopLease) Release()            {}
func (l noopLease) Owner() tenant.Owner { return l.owner }
func (l noopLease) Resource() Resource  { return l.resource }

// NopLease returns a lease that does nothing; used by call sites that
// always run with a budget registry while a resource is unmanaged.
func NopLease(owner tenant.Owner, res Resource) Lease {
	return noopLease{owner: owner, resource: res}
}
