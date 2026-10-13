// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package cache implements an in-memory cache used to track API keys, actions, and artifacts.
//
// When wired to a namespace budget registry, entries attributed to an
// authenticated owner hold their byte cost in that owner's guaranteed
// cache-cost share; entries without a usable owner (pre-authorization API
// key validation, PGP keys) land in the shared default shard. The owner
// parameters on the attributed setters are the ownership captured at
// admission time and must not be derived from unauthenticated input.

package cache

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/elastic/fleet-server/v7/internal/pkg/apikey"
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

type Cache interface {
	Reconfigure(config.Cache) error

	SetAction(action model.Action, owner tenant.Owner)
	GetAction(id string) (model.Action, bool)

	SetAPIKey(key APIKey, enabled bool)
	ValidAPIKey(key APIKey) bool

	SetEnrollmentAPIKey(id string, key model.EnrollmentAPIKey, cost int64, owner tenant.Owner)
	GetEnrollmentAPIKey(id string) (model.EnrollmentAPIKey, bool)

	SetArtifact(artifact model.Artifact, owner tenant.Owner)
	GetArtifact(ident, sha2 string) (model.Artifact, bool)

	SetUpload(id string, info file.Info, owner tenant.Owner)
	GetUpload(id string) (file.Info, bool)

	SetPGPKey(id string, p []byte)
	GetPGPKey(id string) ([]byte, bool)
}

type APIKey = apikey.APIKey
type SecurityInfo = apikey.SecurityInfo

type Option func(c *CacheT)

func WithLog(log *zerolog.Logger) Option {
	return func(c *CacheT) {
		c.log = log
	}
}

// WithBudgets enables per-owner guaranteed cache-cost shares.
func WithBudgets(budgets *budget.Registry) Option {
	return func(c *CacheT) {
		c.budgets = budgets
	}
}

type CacheT struct {
	cache   Cacher
	fair    *fairCache
	log     *zerolog.Logger
	cfg     config.Cache
	budgets *budget.Registry
	mut     sync.RWMutex
}

type actionCache struct {
	actionID   string
	actionType string
}

// New creates a new cache.
func New(cfg config.Cache, opts ...Option) (*CacheT, error) {
	log := zerolog.Nop()
	c := CacheT{
		cfg: cfg,
		log: &log,
	}

	for _, opt := range opts {
		opt(&c)
	}

	fair := newFairCache(c.budgets, nil)
	cache, err := newCache(cfg, fair.onExit)
	if err != nil {
		return nil, err
	}
	fair.bindCacher(cache)
	c.cache = cache
	c.fair = fair

	return &c, nil
}

// Reconfigure will drop cache
func (c *CacheT) Reconfigure(cfg config.Cache) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	// Release every owner cost lease and forget ownership metadata before
	// the old cache is closed; the rebuilt cache starts from the same
	// empty baseline as the pre-existing behavior.
	c.fair.reset()

	cache, err := newCache(cfg, c.fair.onExit)
	if err != nil {
		return err
	}

	// Close down previous cache
	c.cache.Close()

	// And assign new one
	c.cfg = cfg
	c.fair.bindCacher(cache)
	c.cache = cache
	return nil
}

// BudgetCacheStats returns the point-in-time per-owner cache outcome
// counters used by the metrics layer.
func (c *CacheT) BudgetCacheStats() FairStatsSnapshot {
	c.mut.RLock()
	defer c.mut.RUnlock()
	return c.fair.statsSnapshot()
}

// SetAction sets an action in the cache.
//
// This will only cache the action ID and action Type. So `GetAction` will only
// return a `model.Action` with `ActionId` and `Type` set.
func (c *CacheT) SetAction(action model.Action, owner tenant.Owner) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "action:" + action.ActionID
	v := actionCache{
		actionID:   action.ActionID,
		actionType: action.Type,
	}
	cost := int64(len(action.ActionID) + len(action.Type))
	ttl := c.cfg.ActionTTL
	ok := c.fair.set(scopedKey, v, cost, ttl, owner)
	c.log.Trace().
		Bool("ok", ok).
		Str("id", action.ActionID).
		Int64("cost", cost).
		Msg("Action cache SET")
}

// GetAction returns an action from the cache.
//
// This will only return a `model.Action` with the action ID and action Type set.
// This is because `SetAction` So `GetAction` will only cache the action ID and action Type.
func (c *CacheT) GetAction(id string) (model.Action, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "action:" + id
	if v, ok := c.cache.Get(scopedKey); ok {
		c.log.Trace().Str("id", id).Msg("Action cache HIT")
		action, ok := unwrap(v).(actionCache)
		if !ok {
			c.log.Error().Str("id", id).Msg("Action cache cast fail")
			return model.Action{}, false
		}
		return model.Action{
			ActionID: action.actionID,
			Type:     action.actionType,
		}, ok
	}

	c.log.Trace().Str("id", id).Msg("Action cache MISS")
	return model.Action{}, false
}

// SetAPIKey sets the API key in the cache.
//
// API keys are validated before the request owner is known, so these
// entries are always attributed to the shared default shard.
func (c *CacheT) SetAPIKey(key APIKey, enabled bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "api:" + key.ID

	// Use the valid key as the payload of the record;
	// If caller has marked key as not enabled, use empty string.
	val := key.Key
	if !enabled {
		val = ""
	}

	// If enabled, jitter allows us to randomize the expiration of the artifact
	// across time, which is helpful if a bunch of agents came on at the same time,
	// say during a network restoration. With some jitter, we avoid having to
	// revalidate the API Keys all the same time, which we know causes load on Elastic.
	ttl := c.cfg.APIKeyTTL
	if c.cfg.APIKeyJitter != 0 {
		jitter := time.Duration(rand.Int63n(int64(c.cfg.APIKeyJitter))) //nolint:gosec // used to generate a jitter offset value
		if jitter < ttl {
			ttl = ttl - jitter
		}
	}

	cost := int64(len(scopedKey) + len(val))
	ok := c.fair.set(scopedKey, val, cost, ttl, tenant.Default)
	c.log.Trace().
		Bool("ok", ok).
		Bool("enabled", enabled).
		Str("key", key.ID).
		Dur("ttl", ttl).
		Int64("cost", cost).
		Msg("ApiKey cache SET")
}

// ValidAPIKey returns true if the ApiKey is valid (aka. also present in cache).
func (c *CacheT) ValidAPIKey(key APIKey) bool {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "api:" + key.ID
	v, ok := c.cache.Get(scopedKey)
	if ok {
		switch unwrap(v) {
		case "":
			c.log.Trace().Str("id", key.ID).Msg("ApiKey cache HIT on disabled KEY")
			ok = false
		case key.Key:
			c.log.Trace().Str("id", key.ID).Msg("ApiKey cache HIT")
		default:
			c.log.Trace().Str("id", key.ID).Msg("ApiKey cache MISMATCH")
			ok = false
		}
	} else {
		c.log.Trace().Str("id", key.ID).Msg("ApiKey cache MISS")
	}
	return ok
}

// GetEnrollmentAPIKey returns the enrollment API key by ID.
func (c *CacheT) GetEnrollmentAPIKey(id string) (model.EnrollmentAPIKey, bool) { //nolint:dupl // similar getters to support strong typing
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "record:" + id
	if v, ok := c.cache.Get(scopedKey); ok {
		c.log.Trace().Str("id", id).Msg("Enrollment cache HIT")
		key, ok := unwrap(v).(model.EnrollmentAPIKey)

		if !ok {
			c.log.Error().Str("id", id).Msg("Enrollment cache cast fail")
			return model.EnrollmentAPIKey{}, false
		}
		return key, ok
	}

	c.log.Trace().Str("id", id).Msg("EnrollmentApiKey cache MISS")
	return model.EnrollmentAPIKey{}, false
}

// SetEnrollmentAPIKey adds the enrollment API key into the cache. The owner
// is derived from the just-validated enrollment key record namespaces.
func (c *CacheT) SetEnrollmentAPIKey(id string, key model.EnrollmentAPIKey, cost int64, owner tenant.Owner) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "record:" + id
	ttl := c.cfg.EnrollKeyTTL
	ok := c.fair.set(scopedKey, key, cost, ttl, owner)
	c.log.Trace().
		Bool("ok", ok).
		Str("id", id).
		Int64("cost", cost).
		Dur("ttl", ttl).
		Msg("EnrollmentApiKey cache SET")
}

func makeArtifactKey(ident, sha2 string) string {
	return fmt.Sprintf("artifact:%s:%s", ident, sha2)
}

func (c *CacheT) GetArtifact(ident, sha2 string) (model.Artifact, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := makeArtifactKey(ident, sha2)
	if v, ok := c.cache.Get(scopedKey); ok {
		c.log.Trace().Str("key", scopedKey).Msg("Artifact cache HIT")
		key, ok := unwrap(v).(model.Artifact)

		if !ok {
			c.log.Error().Str("sha2", sha2).Msg("Artifact cache cast fail")
			return model.Artifact{}, false
		}
		return key, ok
	}

	c.log.Trace().Str("key", scopedKey).Msg("Artifact cache MISS")
	return model.Artifact{}, false
}

// SetArtifact will set the cached artifact attributed to the authenticated
// owner's namespace share.
// TODO: strip body and spool to on disk cache if larger than a size threshold
func (c *CacheT) SetArtifact(artifact model.Artifact, owner tenant.Owner) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := makeArtifactKey(artifact.Identifier, artifact.DecodedSha256)
	cost := int64(len(artifact.Body))
	ttl := c.cfg.ArtifactTTL

	ok := c.fair.set(scopedKey, artifact, cost, ttl, owner)
	c.log.Trace().
		Bool("ok", ok).
		Str("key", scopedKey).
		Int64("cost", cost).
		Dur("ttl", ttl).
		Msg("Artifact cache SET")
}

func (c *CacheT) SetUpload(id string, info file.Info, owner tenant.Owner) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "upload:" + id
	ttl := 30 * time.Minute // @todo: add to configurable
	// cache cost for other entries use bytes as the unit. Add up the string lengths and the size of the int64s in the upload.Info struct, as a manual 'sizeof'
	cost := int64(len(info.ID) + len(info.DocID) + len(info.ActionID) + len(info.AgentID) + len(info.Source) + len(info.Status) + 8*4)
	ok := c.fair.set(scopedKey, info, cost, ttl, owner)
	c.log.Trace().
		Bool("ok", ok).
		Str("id", id).
		Int64("cost", cost).
		Dur("ttl", ttl).
		Msg("Upload info cache SET")
}
func (c *CacheT) GetUpload(id string) (file.Info, bool) { //nolint:dupl // a little repetition to support strong typing
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "upload:" + id
	if v, ok := c.cache.Get(scopedKey); ok {
		c.log.Trace().Str("id", id).Msg("upload info cache HIT")
		key, ok := unwrap(v).(file.Info)
		if !ok {
			c.log.Error().Str("id", id).Msg("upload info cache cast fail")
			return file.Info{}, false
		}
		return key, ok
	}

	c.log.Trace().Str("id", id).Msg("upload info cache MISS")
	return file.Info{}, false
}

// SetPGPKey caches a public key; the PGP endpoint is unauthenticated so
// entries land in the shared default shard.
func (c *CacheT) SetPGPKey(id string, p []byte) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "pgp:" + id
	ttl := 30 * time.Minute // @todo: add to configurable
	ok := c.fair.set(scopedKey, p, int64(len(p)), ttl, tenant.Default)
	c.log.Trace().
		Bool("ok", ok).
		Str("id", id).
		Int("cost", len(p)).
		Dur("ttl", ttl).
		Msg("PGP key cache SET")

}
func (c *CacheT) GetPGPKey(id string) ([]byte, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	scopedKey := "pgp:" + id
	if v, ok := c.cache.Get(scopedKey); ok {
		c.log.Trace().Str("id", id).Msg("PGP key cache HIT")
		key, ok := unwrap(v).([]byte)
		if !ok {
			c.log.Error().Str("id", id).Msg("upload info cache cast fail")
			return nil, false
		}
		return key, ok
	}
	return nil, false
}
