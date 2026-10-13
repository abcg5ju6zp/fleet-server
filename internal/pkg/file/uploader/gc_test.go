// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	itesting "github.com/elastic/fleet-server/v7/internal/pkg/testing"
	"github.com/elastic/fleet-server/v7/internal/pkg/testing/esutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expired, unfinished sessions are marked DELETED (conditional update) and
// their chunks are reclaimed by generation. If the conditional update noops
// (the session was published, superseded by a new generation, or handled by
// another instance), no chunk deletion happens.
func TestCleanupExpiredSessionsScopedByGeneration(t *testing.T) {
	fakeBulk := itesting.NewMockBulk()
	client, mockTX := esutil.MockESClient(t)
	fakeBulk.On("Client").Return(client)

	expired := time.Now().Add(-48 * time.Hour)
	fakeBulk.On("Search", mockAny, mockAny, mockAny, mockAny).Return(&es.ResultT{
		HitsT: es.HitsT{Hits: []es.HitT{
			{
				ID:     "act.ag",
				Index:  ".fleet-fileds-fromhost-meta-agent",
				Source: metaDocForSession(t, "gen-A", string(file.StatusAwaiting), 10, expired),
			},
			{
				ID:     "act2.ag",
				Index:  ".fleet-fileds-fromhost-meta-agent",
				Source: metaDocForSession(t, "gen-B", string(file.StatusAwaiting), 10, expired),
			},
		}},
	}, nil)

	deletedGenerations := map[string]bool{}
	mockTX.RoundTripFn = func(req *http.Request) (*http.Response, error) {
		// gen-B is superseded/published concurrently: the conditional update noops
		if strings.Contains(req.URL.Path, "/_update/act2.ag") {
			return mockESResponse(200, `{"result":"noop"}`), nil
		}
		if strings.Contains(req.URL.Path, "/_update/") {
			return mockESResponse(200, `{"result":"updated"}`), nil
		}
		if strings.Contains(req.URL.Path, "/_delete_by_query") {
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			s := string(body)
			switch {
			case strings.Contains(s, "gen-A"):
				deletedGenerations["gen-A"] = true
				assert.Contains(t, s, file.FieldUploadID, "chunk cleanup must be scoped by the generation field")
			case strings.Contains(s, "gen-B"):
				deletedGenerations["gen-B"] = true
			}
			return mockESResponse(200, `{}`), nil
		}
		return mockESResponse(200, `{}`), nil
	}

	reclaimed, err := CleanupExpiredSessions(t.Context(), fakeBulk, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)
	assert.True(t, deletedGenerations["gen-A"], "chunks of the reclaimed generation must be deleted")
	assert.False(t, deletedGenerations["gen-B"], "a noop (superseded/published) session must not have its chunks deleted")
}

func mockESResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
	}
}
