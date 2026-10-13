// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-ucfg"

	"github.com/elastic/fleet-server/v7/internal/pkg/apikey"
	"github.com/elastic/fleet-server/v7/internal/pkg/build"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/fleet-server/v7/internal/pkg/logger/ecs"
	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"

	"github.com/rs/zerolog"
	"go.elastic.co/apm/v2"
	"golang.org/x/sync/semaphore"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type APIKey = apikey.APIKey
type SecurityInfo = apikey.SecurityInfo
type APIKeyMetadata = apikey.APIKeyMetadata

var (
	ErrNoQuotes              = errors.New("quoted literal not supported")
	ErrTooManyBulkDispatches = errors.New("too many pending bulk dispatches")
	ErrEnrollDuplicate       = errors.New("enrollment deduplicated: retry")
)

type MultiOp struct {
	ID    string
	Index string
	Body  []byte
}

type Bulk interface {

	// Synchronous operations run in the bulk engine
	Create(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error)
	Read(ctx context.Context, index, id string, opts ...Opt) ([]byte, error)
	ReadRaw(ctx context.Context, index, id string, opts ...Opt) (*MgetResponseItem, error)
	Update(ctx context.Context, index, id string, body []byte, opts ...Opt) error
	Delete(ctx context.Context, index, id string, opts ...Opt) error
	Index(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error)
	Search(ctx context.Context, index string, body []byte, opts ...Opt) (*es.ResultT, error)
	HasTracer() bool
	StartTransaction(name, transactionType string) *apm.Transaction
	StartTransactionOptions(name, transactionType string, opts apm.TransactionOptions) *apm.Transaction

	// Multi Operation API's run in the bulk engine
	MCreate(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error)
	MIndex(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error)
	MUpdate(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error)
	MDelete(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error)

	// APIKey operations
	APIKeyCreate(ctx context.Context, name, ttl string, roles []byte, meta any) (*APIKey, error)
	APIKeyRead(ctx context.Context, id string, withOwner bool) (*APIKeyMetadata, error)
	APIKeyAuth(ctx context.Context, key APIKey) (*SecurityInfo, error)
	APIKeyInvalidate(ctx context.Context, ids ...string) error
	APIKeyUpdate(ctx context.Context, id, outputPolicyHash string, roles []byte) error

	// Accessor used to talk to elastic search direcly bypassing bulk engine
	Client() *elasticsearch.Client

	CreateAndGetBulker(ctx context.Context, zlog zerolog.Logger, outputName string, outputMap map[string]map[string]any) (Bulk, bool, error)
	GetBulker(outputName string) Bulk
	GetBulkerMap() map[string]Bulk
	CancelFn() context.CancelFunc
	RemoteOutputConfigChanged(zlog zerolog.Logger, name string, newCfg map[string]any) bool

	ReadSecrets(ctx context.Context, secretIds []string) (map[string]string, error)
	WriteSecret(ctx context.Context, value string) (string, error)
	DeleteSecret(ctx context.Context, secretID string) error
}

const kModBulk = "bulk"

var bulkSpanNames = func() [ActionFleetSearch + 1]string {
	var names [ActionFleetSearch + 1]string
	for i := range names {
		names[i] = "Bulker: " + actionT(i).String()
	}
	return names
}()

var flushQueueTxNames = func() [kNumQueues]string {
	var names [kNumQueues]string
	for i := range names {
		names[i] = "Flush queue " + queueT{ty: queueType(i)}.Type()
	}
	return names
}()

var flushSpanNames = func() [kNumQueues]string {
	var names [kNumQueues]string
	for i := range names {
		names[i] = "Flush: " + queueT{ty: queueType(i)}.Type()
	}
	return names
}()

type Bulker struct {
	es                    esapi.Transport
	ch                    chan *bulkT
	chControl             chan *bulkT
	chBackground          chan *bulkT
	opts                  bulkOptT
	blkPool               sync.Pool
	flushBufPool          sync.Pool
	apikeyLimit           *semaphore.Weighted
	readSecretsLimit      *semaphore.Weighted
	trafficGate           *traffic.Gate
	trafficSettings       atomic.Pointer[trafficScheduleCfg]
	tracer                *apm.Tracer
	cancelFn              context.CancelFunc
	pendingBulkDispatches atomic.Int64
	pendingBgDispatches   atomic.Int64
	rejectedBgDispatches  atomic.Int64
	queueSnapshot         atomic.Pointer[queueSnapshot]
	onBgDispatchRejected  func()

	remoteOutputConfigMap map[string]map[string]any
	bulkerMap             map[string]Bulk
	remoteOutputMutex     sync.RWMutex
}

// SetBackgroundDispatchRejectedCallback installs a metrics callback fired
// when a background dispatch is rejected above its cap.
func (b *Bulker) SetBackgroundDispatchRejectedCallback(fn func()) {
	b.onBgDispatchRejected = fn
}

// trafficScheduleCfg holds the hot-swappable bulk-specific traffic settings.
// Capacity rules themselves live in the shared traffic.Gate snapshot.
type trafficScheduleCfg struct {
	backgroundDispatchLimit int64
	backgroundFlushDelay    time.Duration
}

const (
	defaultFlushInterval                  = time.Second * 5
	defaultFlushThresholdCnt              = 32768
	defaultFlushThresholdSz               = 1024 * 1024 * 10
	defaultMaxPending                     = 32
	defaultBlockQueueSz                   = 32 // Small capacity to allow multiOp to spin fast
	defaultAPIKeyMaxParallel              = 32
	defaultMaxConcurrentSecretReads       = 32
	defaultApikeyMaxReqSize               = 100 * 1024 * 1024
	defaultFlushContextTimeout            = time.Minute * 1
	defaultMaxPendingBulkDispatches int64 = 0 // 0 means no limit

	// dispatchAbortDrainTimeout bounds how long the drain helper waits for
	// a late response from the Run loop on an abort from the second
	// select() of the dispatch method before giving up on freeing blk.
	// Three flush intervals give enough headroom for the blk to be picked
	// up, sent to ES, and responded to even under heavy load.
	dispatchAbortDrainTimeout = 3 * defaultFlushInterval
)

func NewBulker(es esapi.Transport, tracer *apm.Tracer, opts ...BulkOpt) *Bulker {

	bopts := parseBulkOpts(opts...)

	poolFunc := func() any {
		return &bulkT{ch: make(chan respT, 1)}
	}

	b := &Bulker{
		opts:                  bopts,
		es:                    es,
		ch:                    make(chan *bulkT, bopts.blockQueueSz),
		chControl:             make(chan *bulkT, bopts.blockQueueSz),
		chBackground:          make(chan *bulkT, bopts.blockQueueSz),
		trafficGate:           bopts.trafficGate,
		blkPool:               sync.Pool{New: poolFunc},
		flushBufPool:          sync.Pool{New: func() any { return new(bytes.Buffer) }},
		apikeyLimit:           semaphore.NewWeighted(int64(bopts.apikeyMaxParallel)),
		tracer:                tracer,
		remoteOutputConfigMap: make(map[string]map[string]any),
		// remote ES bulkers
		bulkerMap: make(map[string]Bulk),
	}
	// 0 means no limit; leave readSecretsLimit nil so ReadSecrets skips the semaphore.
	if bopts.maxConcurrentSecretReads > 0 {
		b.readSecretsLimit = semaphore.NewWeighted(int64(bopts.maxConcurrentSecretReads))
	}
	b.trafficSettings.Store(&trafficScheduleCfg{
		backgroundDispatchLimit: bopts.bgDispatchLimit,
		backgroundFlushDelay:    bopts.bgFlushDelay,
	})
	return b
}

// ReconfigureTraffic atomically updates the bulk-specific traffic settings.
// Capacity rules are swapped independently on the shared gate via Apply.
// It affects newly admitted work only; in-flight flushes finish under the
// rules they acquired.
func (b *Bulker) ReconfigureTraffic(backgroundDispatchLimit int64, backgroundFlushDelay time.Duration) {
	b.trafficSettings.Store(&trafficScheduleCfg{
		backgroundDispatchLimit: backgroundDispatchLimit,
		backgroundFlushDelay:    backgroundFlushDelay,
	})
}

func (b *Bulker) trafficEnabled() bool {
	return b.trafficGate != nil && b.trafficGate.Enabled()
}

func (b *Bulker) bgFlushDelay() time.Duration {
	if cfg := b.trafficSettings.Load(); cfg != nil && cfg.backgroundFlushDelay > 0 {
		return cfg.backgroundFlushDelay
	}
	return 2 * b.opts.flushInterval
}

func (b *Bulker) bgDispatchLimit() int64 {
	if cfg := b.trafficSettings.Load(); cfg != nil && cfg.backgroundDispatchLimit > 0 {
		return cfg.backgroundDispatchLimit
	}
	return b.opts.maxPendingBulkDispatches
}

// BackgroundDispatchLimit returns the currently effective background
// dispatch cap (0 means unlimited). It is used for observability/tests.
func (b *Bulker) BackgroundDispatchLimit() int64 {
	return b.bgDispatchLimit()
}

func (b *Bulker) GetBulker(outputName string) Bulk {
	b.remoteOutputMutex.RLock()
	defer b.remoteOutputMutex.RUnlock()
	return b.bulkerMap[outputName]
}

// GetBulkerMap returns a copy of the remote output bulkers
func (b *Bulker) GetBulkerMap() map[string]Bulk {
	mp := make(map[string]Bulk)
	b.remoteOutputMutex.RLock()
	maps.Copy(mp, b.bulkerMap)
	b.remoteOutputMutex.RUnlock()
	return mp
}

func (b *Bulker) CancelFn() context.CancelFunc {
	return b.cancelFn
}

func (b *Bulker) updateBulkerMap(outputName string, newBulker *Bulker) {
	// concurrency control of updating map
	b.remoteOutputMutex.Lock()
	defer b.remoteOutputMutex.Unlock()

	b.bulkerMap[outputName] = newBulker
}

// for remote ES output, create a new bulker in bulkerMap if does not exist
// if bulker exists for output, check if config changed
// if not changed, return the existing bulker
// if changed, stop the existing bulker and create a new one
func (b *Bulker) CreateAndGetBulker(ctx context.Context, zlog zerolog.Logger, outputName string, outputMap map[string]map[string]any) (Bulk, bool, error) {
	hasConfigChanged := b.hasChangedAndUpdateRemoteOutputConfig(zlog, outputName, outputMap[outputName])
	b.remoteOutputMutex.RLock()
	bulker := b.bulkerMap[outputName]
	b.remoteOutputMutex.RUnlock()
	if bulker != nil && !hasConfigChanged {
		return bulker, false, nil
	}
	if bulker != nil && hasConfigChanged {
		cancelFn := bulker.CancelFn()
		if cancelFn != nil {
			cancelFn()
		}
	}
	bulkCtx, bulkCancel := context.WithCancel(context.Background()) // background context used to allow bulker to flush on exit, exits when config changes or primary bulker exits.
	es, err := b.createRemoteEsClient(bulkCtx, outputName, outputMap)
	if err != nil {
		defer bulkCancel()
		return nil, hasConfigChanged, err
	}
	// Remote-output bulkers only share the ES transport gate; their flush
	// scheduling keeps its own legacy semaphore so remote flushes cannot
	// contend with the main engine's control reservations.
	newBulker := NewBulker(es, b.tracer)
	newBulker.cancelFn = bulkCancel

	b.updateBulkerMap(outputName, newBulker)

	errCh := make(chan error)
	go func() {
		runFunc := func() (err error) {
			zlog.Debug().Str(ecs.PolicyOutputName, outputName).Msg("Bulker started")
			return newBulker.Run(bulkCtx)
		}

		errCh <- runFunc()
	}()
	go func() {
		select {
		case err = <-errCh:
			zlog.Error().Err(err).Str(ecs.PolicyOutputName, outputName).Msg("Bulker error")
		case <-bulkCtx.Done():
			zlog.Debug().Str(ecs.PolicyOutputName, outputName).Msg("Bulk context done")
			err = bulkCtx.Err()
		}
	}()

	return newBulker, hasConfigChanged, nil
}

var newESClient = es.NewClient

func (b *Bulker) createRemoteEsClient(ctx context.Context, outputName string, outputMap map[string]map[string]any) (*elasticsearch.Client, error) {
	var esOutput config.Elasticsearch
	esConfig, err := ucfg.NewFrom(outputMap[outputName], config.DefaultOptions...)
	if err != nil {
		return nil, err
	}
	err = esConfig.Unpack(&esOutput)
	if err != nil {
		return nil, err
	}
	if len(esOutput.Hosts) == 0 {
		return nil, fmt.Errorf("failed to get hosts from output: %v", outputName)
	}
	if esOutput.ServiceToken == "" {
		return nil, fmt.Errorf("failed to get service token from output: %v", outputName)
	}
	cfg := config.Config{
		Output: config.Output{
			Elasticsearch: esOutput,
		},
	}
	es, err := newESClient(ctx, &cfg, false, append(elasticsearchOptions(
		true, b.opts.bi,
	), es.WithTrafficGate(b.trafficGate))...)
	if err != nil {
		return nil, err
	}
	return es, nil
}

func elasticsearchOptions(instumented bool, bi build.Info) []es.ConfigOption {
	options := []es.ConfigOption{es.WithUserAgent("Remote-Fleet-Server", bi)}
	if instumented {
		options = append(options, es.InstrumentRoundTripper())
	}
	return options
}

func (b *Bulker) Client() *elasticsearch.Client {
	client, ok := b.es.(*elasticsearch.Client)
	if !ok {
		panic("Client is not an elastic search pointer")
	}
	return client
}

func (b *Bulker) RemoteOutputConfigChanged(zlog zerolog.Logger, name string, newCfg map[string]any) bool {
	b.remoteOutputMutex.RLock()
	defer b.remoteOutputMutex.RUnlock()
	curCfg := b.remoteOutputConfigMap[name]

	// when output config first added, not reporting change
	hasChanged := curCfg != nil && !reflect.DeepEqual(curCfg, newCfg)
	return hasChanged
}

// check if remote output cfg changed
func (b *Bulker) hasChangedAndUpdateRemoteOutputConfig(zlog zerolog.Logger, name string, newCfg map[string]any) bool {
	hasChanged := b.RemoteOutputConfigChanged(zlog, name, newCfg)
	if hasChanged {
		zlog.Debug().Str("name", name).Msg("remote output configuration has changed")
	}

	newCfgCopy := make(map[string]any)
	maps.Copy(newCfgCopy, newCfg)
	b.remoteOutputMutex.Lock()
	b.remoteOutputConfigMap[name] = newCfgCopy
	b.remoteOutputMutex.Unlock()
	return hasChanged
}

// read secrets one by one as there is no bulk API yet to read them in one request
func (b *Bulker) ReadSecrets(ctx context.Context, secretIds []string) (map[string]string, error) {
	result := make(map[string]string)
	esClient := b.Client()
	for _, id := range secretIds {
		if b.readSecretsLimit != nil {
			if err := b.readSecretsLimit.Acquire(ctx, 1); err != nil {
				return nil, err
			}
		}
		val, err := ReadSecret(ctx, esClient, id)
		if b.readSecretsLimit != nil {
			b.readSecretsLimit.Release(1)
		}
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				zerolog.Ctx(ctx).Warn().Str("secret_id", id).Msg("secret not found; policy will load without it")
				continue
			}
			return nil, err
		}
		result[id] = val
	}
	return result, nil
}

func (b *Bulker) WriteSecret(ctx context.Context, value string) (string, error) {
	return WriteSecret(ctx, b.Client(), value)
}

func (b *Bulker) DeleteSecret(ctx context.Context, secretID string) error {
	return DeleteSecret(ctx, b.Client(), secretID)
}

// Stop timer, but don't stall on channel.
// API doesn't not seem to work as specified.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func blkToQueueType(blk *bulkT) queueType {
	queueIdx := kQueueBulk

	if blk.action == ActionSearch && blk.dedupeKey != "" {
		return kQueueEnrollSearch
	}

	forceRefresh := blk.flags.Has(flagRefresh)

	switch blk.action {
	case ActionSearch:
		queueIdx = kQueueSearch
	case ActionFleetSearch:
		queueIdx = kQueueFleetSearch
	case ActionRead:
		if forceRefresh {
			queueIdx = kQueueRefreshRead
		} else {
			queueIdx = kQueueRead
		}
	case ActionUpdateAPIKey:
		queueIdx = kQueueAPIKeyUpdate
	default:
		if forceRefresh {
			queueIdx = kQueueRefreshBulk
		}
	}

	return queueIdx
}

func (b *Bulker) Run(ctx context.Context) error {
	var err error

	zerolog.Ctx(ctx).Info().Interface("opts", &b.opts).Msg("Run bulker with options")

	// Create timer in stopped state
	timer := time.NewTimer(b.opts.flushInterval)
	stopTimer(timer)
	defer timer.Stop()

	// Separate timer and counter for the enrollment search queue so it can flush
	// independently at a shorter interval without affecting other queues.
	enrollTimer := time.NewTimer(b.opts.enrollFlushInterval)
	stopTimer(enrollTimer)
	defer enrollTimer.Stop()

	w := semaphore.NewWeighted(int64(b.opts.maxPending))

	var queues [kNumQueues]queueT

	var i queueType
	for ; i < kNumQueues; i++ {
		queues[i].ty = i
	}

	var itemCnt int
	var byteCnt int
	var enrollItemCnt int

	resetQueue := func(q *queueT) {
		// Reset local queue stored in array
		q.cnt = 0
		q.head = nil
		q.pending = 0
		q.controlCnt = 0
		q.bgDeferredAt = time.Time{}
	}

	doFlush := func() (bool, error) {
		enabled := b.trafficEnabled()
		// Pass 1: queues carrying control blocks are flushed first, in the
		// historical queue order. Mixed batches (control + background in
		// the same queue) are elevated to control; background work in them
		// hitchhikes rather than delaying a control flush. When the gate is
		// disabled every pending queue is handled here in historical order,
		// so a queue deferred under the enabled setting can never get
		// stranded by an enable->disable toggle.
		for i := range queues {
			if queueType(i) == kQueueEnrollSearch {
				continue // flushed independently by doFlushEnroll
			}
			q := &queues[i]
			if q.pending > 0 && (!enabled || q.controlCnt > 0) {
				// Pass queue structure by value
				if err := b.flushQueue(ctx, w, *q, traffic.Control); err != nil {
					return false, err
				}
				resetQueue(q)
			}
		}

		// Pass 2: background-only queues. Permits are acquired without
		// blocking the Run loop; queues that cannot get a permit are
		// retained and retried, with a bounded forced flush for fairness.
		retained := false
		if enabled {
			for i := range queues {
				if queueType(i) == kQueueEnrollSearch {
					continue
				}
				q := &queues[i]
				if q.pending == 0 {
					continue
				}
				if q.bgDeferredAt.IsZero() {
					q.bgDeferredAt = time.Now()
				}
				forced := time.Since(q.bgDeferredAt) >= b.bgFlushDelay()
				started, err := b.tryFlushBackgroundQueue(ctx, w, *q, forced)
				if err != nil {
					return false, err
				}
				if !started {
					retained = true
					continue
				}
				resetQueue(q)
			}
			if retained {
				// Retry retained queues promptly even without new blocks.
				retry := b.opts.flushInterval
				if d := b.bgFlushDelay(); d < retry {
					retry = d
				}
				timer.Reset(retry)
			}
		}

		// Reset threshold counters
		itemCnt = 0
		byteCnt = 0
		b.publishQueueSnapshot(&queues)
		return retained, nil
	}

	doFlushEnroll := func() error {
		q := &queues[kQueueEnrollSearch]
		if q.pending > 0 {
			if err := b.flushQueue(ctx, w, *q, traffic.Control); err != nil {
				return err
			}
			resetQueue(q)
		}
		enrollItemCnt = 0
		b.publishQueueSnapshot(&queues)
		return nil
	}

	// enqueue routes one received block into its internal queue and runs
	// the unchanged threshold/timer logic. When the gate is disabled every
	// block is accounted as control so the flush order matches the legacy
	// single-channel behaviour exactly.
	enqueue := func(blk *bulkT) error {
		queueIdx := blkToQueueType(blk)
		q := &queues[queueIdx]

		// Prepend block to head of target queue
		blk.next = q.head
		q.head = blk

		// Update pending count on target queue
		q.cnt += 1
		q.pending += blk.buf.Len()
		class := blk.class
		if !b.trafficEnabled() {
			class = traffic.Control
		}
		if class == traffic.Control {
			q.controlCnt++
		}

		if queueIdx == kQueueEnrollSearch {
			enrollItemCnt++
			if enrollItemCnt == 1 {
				enrollTimer.Reset(b.opts.enrollFlushInterval)
			}
			if enrollItemCnt >= b.opts.enrollFlushThresholdCnt {
				zerolog.Ctx(ctx).Trace().
					Str("mod", kModBulk).
					Int("enrollItemCnt", enrollItemCnt).
					Msg("Flush enroll search on threshold")
				if err := doFlushEnroll(); err != nil {
					return err
				}
				stopTimer(enrollTimer)
			}
			return nil
		}

		// Update threshold counters
		itemCnt += 1
		byteCnt += blk.buf.Len()

		// Start timer on first queued item
		if itemCnt == 1 {
			timer.Reset(b.opts.flushInterval)
		}

		// Threshold test, short circuit timer on pending count
		if itemCnt >= b.opts.flushThresholdCnt || byteCnt >= b.opts.flushThresholdSz {
			zerolog.Ctx(ctx).Trace().
				Str("mod", kModBulk).
				Int("itemCnt", itemCnt).
				Int("byteCnt", byteCnt).
				Msg("Flush on threshold")

			retained, flushErr := doFlush()
			if flushErr != nil {
				return flushErr
			}
			// A retained background queue keeps its own retry timer armed.
			if !retained {
				stopTimer(timer)
			}
		}
		return nil
	}

	for err == nil {

		// Strict priority: a ready control block must never wait behind
		// background blocks, regardless of Go's random select order.
		if b.trafficEnabled() {
			select {
			case blk := <-b.chControl:
				if e := enqueue(blk); e != nil {
					err = e
				}
				continue
			default:
			}
		}

		select {

		case blk := <-b.ch:
			err = enqueue(blk)

		case blk := <-b.chControl:
			err = enqueue(blk)

		case blk := <-b.chBackground:
			err = enqueue(blk)

		case <-enrollTimer.C:
			zerolog.Ctx(ctx).Trace().
				Str("mod", kModBulk).
				Int("enrollItemCnt", enrollItemCnt).
				Msg("Flush enroll search on timer")
			err = doFlushEnroll()

		case <-timer.C:
			zerolog.Ctx(ctx).Trace().
				Str("mod", kModBulk).
				Int("itemCnt", itemCnt).
				Int("byteCnt", byteCnt).
				Msg("Flush on timer")

			_, err = doFlush()

		case <-ctx.Done():
			err = ctx.Err()
		}

	}

	// cancelling context of each remote bulker when Run exits
	defer func() {
		b.remoteOutputMutex.RLock()
		defer b.remoteOutputMutex.RUnlock()
		for _, bulker := range b.bulkerMap {
			bulker.CancelFn()()
		}
	}()

	return err
}

// acquireFlushPermit obtains one concurrent-flush permit. When the traffic
// gate is disabled it uses the legacy flush semaphore (blocking acquire).
// When enabled, blocking requests go through the class gate while
// non-blocking requests use TryAcquire (background-only queues).
func (b *Bulker) acquireFlushPermit(acquireCtx context.Context, w *semaphore.Weighted, class traffic.Class, blocking bool) (func(), bool, error) {
	if !b.trafficEnabled() {
		if err := w.Acquire(acquireCtx, 1); err != nil {
			return nil, false, err
		}
		return func() { w.Release(1) }, true, nil
	}
	if blocking {
		ticket, err := b.trafficGate.Acquire(acquireCtx, class)
		if err != nil {
			return nil, false, err
		}
		return ticket.Release, true, nil
	}
	ticket, ok := b.trafficGate.TryAcquire(class)
	if !ok {
		return nil, false, nil
	}
	return ticket.Release, true, nil
}

// tryFlushBackgroundQueue attempts a non-blocking flush of a
// background-only queue. When force is true (the queue's starvation deadline
// elapsed) it waits for a permit, but a gate-side timeout/rejection only
// defers the queue: the Run loop must stay alive under sustained control
// pressure and retry on the next flush cycle.
func (b *Bulker) tryFlushBackgroundQueue(ctx context.Context, w *semaphore.Weighted, queue queueT, force bool) (bool, error) {
	acquireCtx, cancel := context.WithTimeout(ctx, defaultFlushContextTimeout)
	defer cancel()

	release, acquired, err := b.acquireFlushPermit(acquireCtx, w, traffic.Background, force)
	if err != nil {
		switch {
		case errors.Is(err, traffic.ErrCapacityTimeout),
			errors.Is(err, traffic.ErrCapacityRejected),
			// The 1-minute acquire window elapsed while the Run loop is
			// still alive: retain and retry instead of killing Run. The
			// parent ctx cancellation (shutdown) surfaces as Canceled and
			// must still propagate below.
			errors.Is(err, context.DeadlineExceeded):
			zerolog.Ctx(ctx).Debug().Err(err).
				Str("mod", kModBulk).
				Str("queue", queue.Type()).
				Int("szPending", queue.pending).
				Msg("background flush still waiting for a permit; queue retained")
			return false, nil
		default:
			zerolog.Ctx(ctx).Error().Err(err).Msg("background flushQueue Wait error")
			return false, err
		}
	}
	if !acquired {
		zerolog.Ctx(ctx).Trace().
			Str("mod", kModBulk).
			Str("queue", queue.Type()).
			Int("szPending", queue.pending).
			Msg("background flush deferred: no permit")
		return false, nil
	}
	b.spawnFlush(ctx, queue, traffic.Background, release)
	return true, nil
}

func (b *Bulker) flushQueue(ctx context.Context, w *semaphore.Weighted, queue queueT, class traffic.Class) error {
	start := time.Now()
	zerolog.Ctx(ctx).Trace().
		Str("mod", kModBulk).
		Int("cnt", queue.cnt).
		Int("szPending", queue.pending).
		Str("queue", queue.Type()).
		Str("class", class.String()).
		Msg("flushQueue Wait")

	acquireCtx, cancel := context.WithTimeout(ctx, defaultFlushContextTimeout)
	defer cancel()

	release, acquired, err := b.acquireFlushPermit(acquireCtx, w, class, true)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("flushQueue Wait error")
		return err
	}
	if !acquired {
		return errors.New("flushQueue: blocking acquire returned no permit")
	}

	zerolog.Ctx(ctx).Trace().
		Str("mod", kModBulk).
		Int("cnt", queue.cnt).
		Dur("tdiff", time.Since(start)).
		Int("szPending", queue.pending).
		Str("queue", queue.Type()).
		Msg("flushQueue Acquired")

	b.spawnFlush(ctx, queue, class, release)
	return nil
}

func (b *Bulker) spawnFlush(ctx context.Context, queue queueT, class traffic.Class, release func()) {
	go func() {
		start := time.Now()

		// deadline prevents bulker being blocked on flush
		flushCtx, cancel := context.WithTimeout(ctx, defaultFlushContextTimeout)
		defer cancel()
		// Carry the batch class to the ES transport gate and retries.
		flushCtx = traffic.WithClass(flushCtx, class)

		if b.tracer != nil {
			trans := b.tracer.StartTransaction(flushQueueTxNames[queue.ty], "bulker")
			trans.Context.SetLabel("queue.size", queue.cnt)
			trans.Context.SetLabel("queue.pending", queue.pending)
			trans.Context.SetLabel("traffic.class", class.String())
			ctx = apm.ContextWithTransaction(ctx, trans)
			defer trans.End()
		}

		defer release()

		var err error
		switch queue.ty {
		case kQueueRead, kQueueRefreshRead:
			err = b.flushRead(flushCtx, queue)
		case kQueueSearch, kQueueFleetSearch:
			err = b.flushSearch(flushCtx, queue)
		case kQueueEnrollSearch:
			err = b.flushEnrollSearch(flushCtx, queue)
		case kQueueAPIKeyUpdate:
			err = b.flushUpdateAPIKey(flushCtx, queue)
		default:
			err = b.flushBulk(flushCtx, queue)
		}

		if err != nil {
			failQueue(queue, err)
			apm.CaptureError(ctx, err).Send()
		}

		zerolog.Ctx(ctx).Trace().
			Err(err).
			Str("mod", kModBulk).
			Int("cnt", queue.cnt).
			Int("szPending", queue.pending).
			Str("queue", queue.Type()).
			Str("class", class.String()).
			Dur("rtt", time.Since(start)).
			Msg("flushQueue Done")

	}()
}

func failQueue(queue queueT, err error) {
	for n := queue.head; n != nil; {
		next := n.next // 'n' is invalid immediately on channel send
		select {
		case n.ch <- respT{
			err: err,
		}:
		default:
			panic("Unexpected blocked response channel on failQueue")
		}
		n = next
	}
}

func (b *Bulker) parseOpts(opts ...Opt) optionsT {
	var opt optionsT
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

func (b *Bulker) newBlk(action actionT, opts optionsT) *bulkT {
	blk := b.blkPool.Get().(*bulkT) //nolint:errcheck // we control what is placed in the pool
	blk.action = action
	if opts.Refresh {
		blk.flags.Set(flagRefresh)
	}
	blk.spanLink = opts.spanLink
	blk.hasSpanLink = opts.hasSpanLink
	blk.dedupeKey = opts.DedupeKey
	blk.refreshIndex = opts.RefreshIndex
	blk.class = opts.trafficClass

	return blk
}

func (b *Bulker) freeBlk(blk *bulkT) {
	blk.reset()
	b.blkPool.Put(blk)
}

func (b *Bulker) validateIndex(index string) error {
	// TODO: index
	return nil
}

func (b *Bulker) validateIndices(indices []string) error {
	for _, i := range indices {
		if err := b.validateIndex(i); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bulker) validateMeta(index, id string) error {

	// Quotes on id are legal, but weird.  Disallow for now.
	if strings.IndexByte(index, '"') != -1 || strings.IndexByte(id, '"') != -1 {
		return ErrNoQuotes
	}
	return nil
}

// TODO: Fail on non-escaped line feeds
func (b *Bulker) validateBody(body []byte) error {
	if !json.Valid(body) {
		return es.ErrInvalidBody
	}

	return nil
}

func (b *Bulker) dispatch(ctx context.Context, blk *bulkT) respT {
	start := time.Now()

	if b.trafficEnabled() {
		return b.dispatchClassified(ctx, blk)
	}

	// Check pending bulk dispatch limit before blocking on the channel.
	if limit := b.opts.maxPendingBulkDispatches; limit > 0 {
		pending := b.pendingBulkDispatches.Add(1)
		defer b.pendingBulkDispatches.Add(-1)
		if pending > limit {
			zerolog.Ctx(ctx).Warn().
				Str("mod", kModBulk).
				Str("action", blk.action.String()).
				Int64("pending", pending).
				Int64("limit", limit).
				Msg("Bulk dispatch rejected: too many pending")
			b.freeBlk(blk)
			return respT{err: ErrTooManyBulkDispatches}
		}
	}

	// Dispatch to bulk Run loop
	select {
	case b.ch <- blk:
	case <-ctx.Done():
		zerolog.Ctx(ctx).Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort queue")
		b.freeBlk(blk)
		return respT{err: ctx.Err()}
	}

	// Wait for response
	select {
	case resp := <-blk.ch:
		zerolog.Ctx(ctx).Trace().
			Err(resp.err).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch OK")

		return resp
	case <-ctx.Done():
		zerolog.Ctx(ctx).Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort response")
		// blk is in the Run loop's queue; drain the response and free
		// asynchronously so the caller can return immediately.
		go b.drainAndFreeAbortedBlk(blk)
	}

	return respT{err: ctx.Err()}
}

// dispatchClassified is the enabled-gate path: blocks are sent to per-class
// ingress channels so background pressure cannot head-of-line block control
// work. Only background dispatches are counted against the dispatch cap;
// control is bounded by its reserved capacity instead.
func (b *Bulker) dispatchClassified(ctx context.Context, blk *bulkT) respT {
	start := time.Now()

	if blk.class == traffic.Background {
		if limit := b.bgDispatchLimit(); limit > 0 {
			pending := b.pendingBgDispatches.Add(1)
			defer b.pendingBgDispatches.Add(-1)
			if pending > limit {
				b.rejectedBgDispatches.Add(1)
				if b.onBgDispatchRejected != nil {
					b.onBgDispatchRejected()
				}
				zerolog.Ctx(ctx).Warn().
					Str("mod", kModBulk).
					Str("action", blk.action.String()).
					Int64("pending", pending).
					Int64("limit", limit).
					Msg("Background bulk dispatch rejected: too many pending")
				b.freeBlk(blk)
				return respT{err: ErrTooManyBulkDispatches}
			}
		}
	}

	dst := b.chControl
	if blk.class == traffic.Background {
		dst = b.chBackground
	}

	select {
	case dst <- blk:
	case <-ctx.Done():
		zerolog.Ctx(ctx).Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Str("class", blk.class.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort queue")
		b.freeBlk(blk)
		return respT{err: ctx.Err()}
	}

	select {
	case resp := <-blk.ch:
		zerolog.Ctx(ctx).Trace().
			Err(resp.err).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Str("class", blk.class.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch OK")
		return resp
	case <-ctx.Done():
		zerolog.Ctx(ctx).Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.String()).
			Str("class", blk.class.String()).
			Bool("refresh", blk.flags.Has(flagRefresh)).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort response")
		go b.drainAndFreeAbortedBlk(blk)
	}

	return respT{err: ctx.Err()}
}

// drainAndFreeAbortedBlk waits for the Run loop to deliver a response to an
// abandoned blk, then returns it to the pool. If the response does not
// arrive within dispatchAbortDrainTimeout (e.g. during shutdown), it gives
// up rather than blocking forever; in that case blk is not reclaimed.
func (b *Bulker) drainAndFreeAbortedBlk(blk *bulkT) {
	select {
	case <-blk.ch:
		b.freeBlk(blk)
	case <-time.After(dispatchAbortDrainTimeout):
	}
}
