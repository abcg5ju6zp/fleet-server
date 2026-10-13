// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	itesting "github.com/elastic/fleet-server/v7/internal/pkg/testing"
	"github.com/elastic/fleet-server/v7/internal/pkg/testing/esutil"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var mockAny = mock.Anything

// metaDocForSession serializes an upload metadata document as stored in ES
func metaDocForSession(t *testing.T, uploadID, status string, size int64, start time.Time) []byte {
	t.Helper()
	doc := map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file": map[string]any{
			"size":      size,
			"ChunkSize": file.MaxChunkSize,
			"Status":    status,
		},
		"upload_id":    uploadID,
		"upload_start": start.UnixMilli(),
	}
	if status == string(file.StatusDone) {
		doc["transithash"] = map[string]any{"sha256": "publishedhash"}
	}
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return b
}

func newTestUploader(t *testing.T, client *elasticsearch.Client, bulker *itesting.MockBulk) *Uploader {
	t.Helper()
	c, err := cache.New(config.Cache{NumCounters: 100, MaxCost: 100000})
	require.NoError(t, err)
	return New(client, bulker, c, size_ptr(file.MaxChunkSize*10), time.Hour)
}

// A retried begin for an active, unexpired session returns the original
// session instead of creating a new document.
func TestBeginReusesActiveSession(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	start := time.Now().Add(-time.Minute)
	existingID := "existing-generation"

	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", es.ErrElasticVersionConflict).Once()
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(
		metaDocForSession(t, existingID, string(file.StatusAwaiting), 1024, start), nil,
	).Once()

	u := newTestUploader(t, nil, fakeBulk)
	info, err := u.Begin(t.Context(), []string{"ns"}, makeUploadRequestDict(map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file.size": int64(1024),
	}))
	require.NoError(t, err)
	assert.Equal(t, existingID, info.ID)
	assert.Equal(t, "act.ag", info.DocID)
	assert.Equal(t, file.StatusAwaiting, info.Status)
	fakeBulk.AssertNotCalled(t, "Index", mockAny, mockAny, mockAny, mockAny, mockAny)
}

// A retried begin for a published (READY) session returns that session so
// chunk and complete retries remain idempotent.
func TestBeginReusesPublishedSession(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	existingID := "published-generation"

	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", es.ErrElasticVersionConflict).Once()
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(
		metaDocForSession(t, existingID, string(file.StatusDone), 1024, time.Now().Add(-48*time.Hour)), nil,
	).Once()

	u := newTestUploader(t, nil, fakeBulk)
	info, err := u.Begin(t.Context(), nil, makeUploadRequestDict(map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file.size": int64(1024),
	}))
	require.NoError(t, err)
	assert.Equal(t, existingID, info.ID)
	assert.True(t, info.Published())
	assert.Equal(t, "publishedhash", info.TransitHash)
}

// A retried begin for an expired session starts a fresh generation, replacing
// the metadata document under the same deterministic document ID.
func TestBeginSupersedesExpiredSession(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	oldID := "old-generation"

	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", es.ErrElasticVersionConflict).Once()
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(
		metaDocForSession(t, oldID, string(file.StatusAwaiting), 1024, time.Now().Add(-2*time.Hour)), nil,
	).Once()
	indexCalled := false
	fakeBulk.On("Index",
		mockAny,
		mockAny,
		"act.ag",
		mockAny,
		mockAny,
	).Run(func(_ mock.Arguments) { indexCalled = true }).Return("", nil).Once()

	u := newTestUploader(t, nil, fakeBulk)
	info, err := u.Begin(t.Context(), nil, makeUploadRequestDict(map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file.size": int64(1024),
	}))
	require.NoError(t, err)
	assert.True(t, indexCalled, "metadata document must be replaced for a new generation")
	assert.NotEqual(t, oldID, info.ID)
	assert.Equal(t, file.StatusAwaiting, info.Status)
}

// Failed and deleted sessions are also superseded with a fresh generation.
func TestBeginSupersedesFailedSession(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()

	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", es.ErrElasticVersionConflict).Once()
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(
		metaDocForSession(t, "failed-generation", string(file.StatusFail), 1024, time.Now()), nil,
	).Once()
	fakeBulk.On("Index", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", nil).Once()

	u := newTestUploader(t, nil, fakeBulk)
	info, err := u.Begin(t.Context(), nil, makeUploadRequestDict(map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file.size": int64(1024),
	}))
	require.NoError(t, err)
	assert.NotEqual(t, "failed-generation", info.ID)
}

// Pinned attributes must match when reusing a session: a different file size
// is rejected instead of mixing the two files' chunks.
func TestBeginRejectsConflictingPinnedSize(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()

	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", es.ErrElasticVersionConflict).Once()
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(
		metaDocForSession(t, "existing", string(file.StatusAwaiting), 2048, time.Now()), nil,
	).Once()

	u := newTestUploader(t, nil, fakeBulk)
	_, err := u.Begin(t.Context(), nil, makeUploadRequestDict(map[string]any{
		"action_id": "act",
		"agent_id":  "ag",
		"src":       "agent",
		"file.size": int64(1024),
	}))
	assert.ErrorIs(t, err, ErrSessionConflict)
}

// Zero-byte files begin successfully and require no chunks.
func TestBeginZeroByteFile(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	fakeBulk.On("Create", mockAny, mockAny, mockAny, mockAny, mockAny).Return("", nil)

	u := newTestUploader(t, nil, fakeBulk)
	info, err := u.Begin(t.Context(), nil, makeUploadRequestDict(map[string]any{"file.size": int64(0)}))
	require.NoError(t, err)
	assert.Equal(t, 0, info.Count)
	assert.Equal(t, int64(0), info.Total)
}

// A chunk PUT that collides with an identical stored chunk is an idempotent
// success and never overwrites the stored document.
func TestWriteChunkIdempotentOnSameHash(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	esClient, mockTX := esutil.MockESClient(t)

	mockTX.RoundTripFn = func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/_create") {
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"version_conflict_engine_exception"}}`)),
				Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		}, nil
	}

	existing := file.Chunk{
		BID:  "bid",
		SHA2: "aa",
		Last: false,
		Data: []byte("x"),
	}
	raw, err := json.Marshal(existing)
	require.NoError(t, err)
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(raw, nil).Once()

	u := newTestUploader(t, esClient, fakeBulk)
	info := file.Info{ID: "gen", DocID: "bid", Source: "agent", ChunkSize: file.MaxChunkSize, Count: 1, Status: file.StatusAwaiting}
	chunk := file.ChunkInfo{Pos: 0, BID: "bid", UploadID: "gen", SHA2: "aa"}
	require.NoError(t, u.WriteChunk(t.Context(), info, chunk, []byte("data")))
}

// A chunk PUT that collides with a different stored chunk never overwrites it:
// the incoming bytes are isolated in a quarantine document and a conflict is
// reported.
func TestWriteChunkIsolatesContentConflict(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	esClient, mockTX := esutil.MockESClient(t)

	var quarantineCreated bool
	mockTX.RoundTripFn = func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(req.URL.Path, "/_create/conflict."):
			quarantineCreated = true
		case strings.Contains(req.URL.Path, "/_create"):
			return &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"version_conflict_engine_exception"}}`)),
				Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		}, nil
	}

	existing := file.Chunk{BID: "bid", SHA2: "different"}
	raw, err := json.Marshal(existing)
	require.NoError(t, err)
	fakeBulk.On("Read", mockAny, mockAny, mockAny, mockAny).Return(raw, nil)

	u := newTestUploader(t, esClient, fakeBulk)
	info := file.Info{ID: "gen", DocID: "bid", Source: "agent", ChunkSize: file.MaxChunkSize, Count: 1, Status: file.StatusAwaiting}
	chunk := file.ChunkInfo{Pos: 0, BID: "bid", UploadID: "gen", SHA2: "aa"}
	err = u.WriteChunk(t.Context(), info, chunk, []byte("data"))
	assert.ErrorIs(t, err, ErrChunkContentConflict)
	assert.True(t, quarantineCreated, "conflicting chunk must be isolated under a quarantine document ID")
}

// Completing an already published session with the matching digest is
// idempotent and performs no chunk verification or second publish.
func TestCompleteIdempotentForPublishedSession(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	client, _ := esutil.MockESClient(t)

	fakeBulk.On("Search", mockAny, mockAny, mockAny, mockAny).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{{
			ID:     "act.ag",
			Source: metaDocForSession(t, "gen", string(file.StatusDone), 10, time.Now()),
		}}},
	}, nil)

	u := newTestUploader(t, client, fakeBulk)
	info, err := u.Complete(t.Context(), "gen", "publishedhash")
	require.NoError(t, err)
	assert.True(t, info.Published())
	// no chunk search (data index) must have happened
	for _, call := range fakeBulk.Calls {
		if call.Method == "Search" {
			idx, _ := call.Arguments[1].(string)
			assert.NotContains(t, idx, "fromhost-data")
		}
	}
}

// Completing a published session with a different digest is rejected.
func TestCompleteRejectsPublishedDigestMismatch(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	client, _ := esutil.MockESClient(t)

	fakeBulk.On("Search", mockAny, mockAny, mockAny, mockAny).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{{
			ID:     "act.ag",
			Source: metaDocForSession(t, "gen", string(file.StatusDone), 10, time.Now()),
		}}},
	}, nil)

	u := newTestUploader(t, client, fakeBulk)
	_, err := u.Complete(t.Context(), "gen", "somethingelse")
	assert.ErrorIs(t, err, ErrStatusNoUploads)
}

// Zero-byte files complete with zero chunks as long as the transit digest is
// the hash of an empty sequence.
func TestCompleteZeroByteFile(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	client, mockTX := esutil.MockESClient(t)
	mockTX.RoundTripFn = func(req *http.Request) (*http.Response, error) {
		body := "{}"
		if strings.Contains(req.URL.Path, "/_update/") {
			body = `{"result":"updated"}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		}, nil
	}

	// meta search, then zero chunk search
	metaCall := 0
	fakeBulk.On("Client").Return(client)
	fakeBulk.On("Search",
		mockAny,
		mock.MatchedBy(func(idx string) bool { return strings.Contains(idx, "fromhost-meta") }),
		mockAny, mockAny,
	).Run(func(_ mock.Arguments) { metaCall++ }).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{{
			ID:     "act.ag",
			Source: metaDocForSession(t, "gen", string(file.StatusAwaiting), 0, time.Now()),
		}}},
	}, nil)
	fakeBulk.On("Search",
		mockAny,
		mock.MatchedBy(func(idx string) bool { return strings.Contains(idx, "fromhost-data") }),
		mockAny, mockAny,
	).Return(&es.ResultT{}, nil)

	u := newTestUploader(t, client, fakeBulk)
	info, err := u.Complete(t.Context(), "gen", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	require.NoError(t, err)
	assert.True(t, info.Published())
	assert.Equal(t, 1, metaCall)
}

// Chunks written before generations were tagged (no upload_id field) are
// accepted during completion via an unscoped fallback, so rolling upgrades do
// not break in-flight uploads.
func TestCompleteAcceptsLegacyUntaggedChunks(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	client, mockTX := esutil.MockESClient(t)
	fakeBulk.On("Client").Return(client)
	mockTX.RoundTripFn = func(req *http.Request) (*http.Response, error) {
		body := "{}"
		if strings.Contains(req.URL.Path, "/_update/") {
			body = `{"result":"updated"}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
		}, nil
	}

	size := int64(200)
	nullHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	fakeBulk.On("Search",
		mockAny,
		mock.MatchedBy(func(idx string) bool { return strings.Contains(idx, "fromhost-meta") }),
		mockAny, mockAny,
	).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{{
			ID:     "act.ag",
			Source: metaDocForSession(t, "gen", string(file.StatusAwaiting), size, time.Now()),
		}}},
	}, nil)
	// first (generation scoped) search finds nothing
	fakeBulk.On("Search",
		mockAny,
		mock.MatchedBy(func(idx string) bool { return strings.Contains(idx, "fromhost-data") }),
		mock.MatchedBy(func(q []byte) bool { return strings.Contains(string(q), `"upload_id":{"value"`) }),
		mockAny,
	).Return(&es.ResultT{}, nil).Once()
	// legacy unscoped search has no generation term and returns the untagged chunk
	fakeBulk.On("Search",
		mockAny,
		mock.MatchedBy(func(idx string) bool { return strings.Contains(idx, "fromhost-data") }),
		mock.MatchedBy(func(q []byte) bool { return !strings.Contains(string(q), `"upload_id":{"value"`) }),
		mockAny,
	).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{{
			ID: "act.ag.0",
			Fields: map[string]any{
				file.FieldBaseID: []any{"act.ag"},
				file.FieldSHA2:   []any{nullHash},
				"size":           []any{size},
				file.FieldLast:   []any{true},
			},
		}}},
	}, nil).Once()

	u := newTestUploader(t, client, fakeBulk)
	info, err := u.Complete(t.Context(), "gen", "5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456")
	require.NoError(t, err)
	assert.True(t, info.Published())
}
