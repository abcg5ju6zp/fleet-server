// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build !integration

package cache

import (
	"github.com/dgraph-io/ristretto/v2"

	"github.com/elastic/fleet-server/v7/internal/pkg/config"
)

// fairEvictionAccounting reports whether the underlying cache emits
// exit callbacks, which the per-owner cost accounting relies on.
const fairEvictionAccounting = true

func newCache(cfg config.Cache, exit onExit) (Cacher, error) {
	rcfg := &ristretto.Config[string, any]{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: 64,
		OnExit: func(val any) {
			if exit != nil {
				exit(val)
			}
		},
	}

	return ristretto.NewCache(rcfg)
}
