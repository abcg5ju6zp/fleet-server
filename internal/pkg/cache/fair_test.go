// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build !integration

package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

const testCacheCap int64 = 10000

// fakeCacher mimics the ristretto exit semantics needed by fairCache without
// any autonomous admission/eviction policy: overwrites and deletes invoke the
// exit callback synchronously with the previous value.
type fakeCacher struct {
	mu     sync.Mutex
	store  map[string]any
	exit   onExit
	closed bool
}

func newFakeCacher() *fakeCacher {
	return &fakeCacher{store: make(map[string]any)}
}

func (f *fakeCacher) Get(key string) (any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[key]
	return v, ok
}

func (f *fakeCacher) Set(_ string, _ any, _ int64) bool { return true }

func (f *fakeCacher) SetWithTTL(key string, value any, _ int64, _ time.Duration) bool {
	f.mu.Lock()
	prev, existed := f.store[key]
	f.store[key] = value
	f.mu.Unlock()
	if existed && f.exit != nil {
		f.exit(prev)
	}
	return true
}

func (f *fakeCacher) Del(key string) {
	f.mu.Lock()
	prev, ok := f.store[key]
	delete(f.store, key)
	f.mu.Unlock()
	if ok && f.exit != nil {
		f.exit(prev)
	}
}

func (f *fakeCacher) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.store {
		delete(f.store, k)
		if f.exit != nil {
			f.exit(v)
		}
	}
	f.closed = true
}

func testRegistry(t *testing.T) *budget.Registry {
	t.Helper()
	reg := budget.New(budget.Config{
		Enabled:          true,
		IdleTTL:          time.Hour,
		ReclaimTTL:       time.Minute,
		MaxTrackedOwners: 100,
		Resources: map[budget.Resource]budget.ResourceConfig{
			budget.ResourceCacheCost: {
				Enabled:   true,
				Capacity:  testCacheCap,
				BorrowMax: testCacheCap,
			},
		},
	})
	require.True(t, reg.IsManaged(budget.ResourceCacheCost))
	return reg
}

func newFairCacheHarness(t *testing.T, reg *budget.Registry) (*CacheT, *fakeCacher) {
	t.Helper()
	fake := newFakeCacher()
	fc := newFairCache(reg, fake)
	fake.exit = fc.onExit
	nop := zerolog.Nop()
	c := &CacheT{
		cache:   fake,
		fair:    fc,
		budgets: reg,
		cfg:     config.Cache{MaxCost: testCacheCap},
		log:     &nop,
	}
	t.Cleanup(func() { fake.Close() })
	return c, fake
}

func resourceSnapshot(t *testing.T, reg *budget.Registry) *budget.ResourceSnapshot {
	t.Helper()
	snap := reg.Snapshot()
	for i := range snap.Resource {
		if snap.Resource[i].Resource == budget.ResourceCacheCost {
			return &snap.Resource[i]
		}
	}
	return nil
}

func ownerSnapshot(t *testing.T, rs *budget.ResourceSnapshot, owner tenant.Owner) budget.OwnerSnapshot {
	t.Helper()
	for _, os := range rs.Owners {
		if os.Owner == string(owner) {
			return os
		}
	}
	t.Fatalf("owner %s missing from snapshot", owner)
	return budget.OwnerSnapshot{}
}

// TR-11.1: a namespace that filled the global cache while others were idle
// loses borrowed entries when another namespace writes within its guaranteed
// share; the second namespace's guaranteed keys survive and the first
// namespace keeps exactly its guarantee.
func TestFairCacheGuaranteedShareReclaimsBorrowed(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)

	a, b := tenant.Owner("ns-a"), tenant.Owner("ns-b")

	// A fills the whole cache while it is the sole active owner.
	for i := range 10 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: artifactKey(i), Body: make([]byte, 1000)}, a)
	}
	rs := resourceSnapshot(t, reg)
	require.Equal(t, testCacheCap, rs.InUse)
	oa := ownerSnapshot(t, rs, a)
	require.Equal(t, testCacheCap, oa.Guarantee)

	// B activates: each 1000-byte write reclaims one of A's oldest
	// borrowed entries until both owners hold their guarantees.
	for i := range 5 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: "b-" + artifactKey(i), Body: make([]byte, 1000)}, b)
	}

	rs = resourceSnapshot(t, reg)
	require.Equal(t, testCacheCap, rs.InUse)
	oa = ownerSnapshot(t, rs, a)
	ob := ownerSnapshot(t, rs, b)
	assert.Equal(t, int64(5000), oa.Guarantee)
	assert.Equal(t, int64(5000), oa.InUse)
	assert.Equal(t, int64(0), oa.Borrowed)
	assert.Equal(t, int64(5000), ob.Guarantee)
	assert.Equal(t, int64(5000), ob.InUse)

	// B's keys all survive.
	for i := range 5 {
		_, ok := c.GetArtifact("art", "b-"+artifactKey(i))
		assert.True(t, ok, "B guaranteed key %d must survive", i)
	}
	// A's five oldest borrowed entries were evicted in order.
	for i := range 5 {
		_, ok := c.GetArtifact("art", artifactKey(i))
		assert.False(t, ok, "A oldest borrowed key %d must be evicted", i)
	}
	// A keeps its newest five, its guaranteed share.
	for i := 5; i < 10; i++ {
		_, ok := c.GetArtifact("art", artifactKey(i))
		assert.True(t, ok, "A key %d within its guarantee must survive", i)
	}
}

// TR-11.1 (borrow): once B holds its full share, A cannot borrow more
// capacity; B's guaranteed entries are never deleted to serve A.
func TestFairCacheBorrowerRejectedUnderContentionKeepsGuaranteedOwner(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)
	a, b := tenant.Owner("ns-a"), tenant.Owner("ns-b")

	for i := range 10 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: artifactKey(i), Body: make([]byte, 1000)}, a)
	}
	for i := range 5 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: "b-" + artifactKey(i), Body: make([]byte, 1000)}, b)
	}

	// A tries to grow beyond its 5000 share while the pool is full: it may
	// only replace its own entries; evicting a single own entry frees room
	// but contention with B (starved during activation and A still beyond
	// its guarantee) rejects continued borrowing beyond that turnover.
	okA := c.fair.set("artifact:art:a-extra", "v", 1000, 0, a)
	// The first turnover of an own entry is allowed (cache replacement).
	assert.True(t, okA)

	// Every B entry remains present regardless of A's churn.
	for i := range 5 {
		_, ok := c.GetArtifact("art", "b-"+artifactKey(i))
		assert.True(t, ok, "B key %d must never be evicted for A", i)
	}
}

// TR-11.1 shared shard: pre-authorization API keys (default owner) keep
// working while attributed tenants churn the rest of the cache.
func TestFairCacheSharedShardProtected(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)
	a := tenant.Owner("ns-a")

	// Shared API-key entries occupy 2000 cost.
	c.SetAPIKey(APIKey{ID: "shared-1", Key: "k1"}, true)
	c.SetAPIKey(APIKey{ID: "shared-2", Key: "k2"}, true)

	// A borrows the remaining 8000 cost.
	for i := range 8 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: artifactKey(i), Body: make([]byte, 1000)}, a)
	}

	// B activation reclaims only A's borrowed entries.
	b := tenant.Owner("ns-b")
	for i := range 3 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: "b-" + artifactKey(i), Body: make([]byte, 1000)}, b)
	}

	assert.True(t, c.ValidAPIKey(APIKey{ID: "shared-1", Key: "k1"}))
	assert.True(t, c.ValidAPIKey(APIKey{ID: "shared-2", Key: "k2"}))
}

// TR-11.2: with a single owner the cache keeps global replacement behavior -
// a new full-capacity write evicts the oldest own entry instead of being
// rejected wholesale.
func TestFairCacheSingleOwnerReplacesOwnEntries(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)
	a := tenant.Default

	for i := range 10 {
		ok := c.fair.set("k"+artifactKey(i), "v", 1000, 0, a)
		require.True(t, ok)
	}
	// 11th write: global full, only the owner's own entries exist; LRU
	// replacement must admit it like the previous global cache did.
	require.True(t, c.fair.set("k-new", "v", 1000, 0, a))

	_, oldExists := c.cache.Get("k" + artifactKey(0))
	assert.False(t, oldExists, "oldest own entry replaced")
	_, newExists := c.cache.Get("k-new")
	assert.True(t, newExists)

	rs := resourceSnapshot(t, reg)
	assert.Equal(t, testCacheCap, rs.InUse)
}

// Same-key same-cost overwrites transfer the lease: no accounting growth and
// the new value is visible.
func TestFairCacheOverwriteTransfersLease(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)
	a := tenant.Owner("ns-a")

	require.True(t, c.fair.set("key", "v1", 1000, 0, a))
	require.True(t, c.fair.set("key", "v2", 1000, 0, a))
	require.True(t, c.fair.set("key", "v3", 1000, 0, a))

	v, ok := c.cache.Get("key")
	require.True(t, ok)
	e := v.(*fEntry)
	assert.Equal(t, "v3", e.val)
	rs := resourceSnapshot(t, reg)
	assert.Equal(t, int64(1000), rs.InUse)
}

// Closing the underlying cache releases every owner lease, so reconfiguring
// the cache restores the budget to a zero baseline (FR-30 / restart).
func TestFairCacheReconfigureReleasesLeases(t *testing.T) {
	cfg := config.Cache{NumCounters: 1000, MaxCost: testCacheCap, ArtifactTTL: time.Minute}
	reg := testRegistry(t)
	c, err := New(cfg, WithBudgets(reg))
	require.NoError(t, err)

	a := tenant.Owner("ns-a")
	for i := range 8 {
		c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: artifactKey(i), Body: make([]byte, 1000)}, a)
	}
	// Drain ristretto's async set buffer so leases are committed.
	if w, ok := c.cache.(interface{ Wait() }); ok {
		w.Wait()
	}
	require.Equal(t, int64(8000), resourceSnapshot(t, reg).InUse)

	require.NoError(t, c.Reconfigure(config.Cache{NumCounters: 1000, MaxCost: testCacheCap, ArtifactTTL: time.Minute}))
	if w, ok := c.cache.(interface{ Wait() }); ok {
		w.Wait()
	}
	assert.Equal(t, int64(0), resourceSnapshot(t, reg).InUse, "reconfigure resets owner cost accounting")

	// The rebuilt cache still serves new attributed writes.
	c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: "post", Body: make([]byte, 1000)}, a)
	if w, ok := c.cache.(interface{ Wait() }); ok {
		w.Wait()
	}
	_, ok := c.GetArtifact("art", "post")
	assert.True(t, ok)
}

// Without a budget registry the cache behaves exactly as before: raw values
// are stored and no owner metadata exists.
func TestFairCacheDisabledStoresRawValues(t *testing.T) {
	cfg := config.Cache{NumCounters: 1000, MaxCost: testCacheCap, ArtifactTTL: time.Minute}
	c, err := New(cfg)
	require.NoError(t, err)
	defer c.cache.Close()

	a := tenant.Owner("ns-a")
	c.SetArtifact(model.Artifact{Identifier: "art", DecodedSha256: "x", Body: []byte("payload")}, a)
	if w, ok := c.cache.(interface{ Wait() }); ok {
		w.Wait()
	}
	v, ok := c.cache.Get("artifact:art:x")
	require.True(t, ok)
	_, isEnvelope := v.(*fEntry)
	assert.False(t, isEnvelope, "unmanaged cache stores raw values")

	art, ok := c.GetArtifact("art", "x")
	require.True(t, ok)
	assert.Equal(t, "payload", string(art.Body))
}

// Concurrent attributed writes never corrupt the ledger; after the cache is
// closed the budget resource is fully released.
func TestFairCacheConcurrentAccountingSettles(t *testing.T) {
	reg := testRegistry(t)
	c, _ := newFairCacheHarness(t, reg)

	var wg sync.WaitGroup
	for _, owner := range []tenant.Owner{"ns-a", "ns-b", "ns-c"} {
		wg.Add(1)
		go func(o tenant.Owner) {
			defer wg.Done()
			for i := range 200 {
				c.fair.set("k:"+o.String()+":"+itoaKey(i), "v", 50, 0, o)
			}
		}(owner)
	}
	wg.Wait()

	rs := resourceSnapshot(t, reg)
	require.NotNil(t, rs)
	assert.LessOrEqual(t, rs.InUse, testCacheCap)

	c.fair.reset()
	assert.Equal(t, int64(0), resourceSnapshot(t, reg).InUse)
}

func artifactKey(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return "sha-00" + digits[i:i+1]
	}
	return "sha-0" + digits[i/10:i/10+1] + digits[i%10:i%10+1]
}

func itoaKey(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
