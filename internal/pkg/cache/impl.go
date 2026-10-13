// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cache

import (
	"time"
)

type Cacher interface {
	Get(key string) (any, bool)
	Set(key string, value any, cost int64) bool
	SetWithTTL(key string, value any, cost int64, ttl time.Duration) bool
	Del(key string)
	Close()
}

// onExit is called with the stored value of an entry when it leaves the
// cache for any reason (eviction, TTL expiry, explicit delete, admission
// rejection or replacement). The fair cache uses it to return the entry's
// weighted cost lease to its owner. It must never block.
type onExit func(value any)
