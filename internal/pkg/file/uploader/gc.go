// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/dsl"
	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	"github.com/rs/zerolog"
	"go.elastic.co/apm/v2"
)

var queryExpiredUnfinishedSessions = prepareQueryExpiredUnfinishedSessions()

// prepareQueryExpiredUnfinishedSessions selects metadata documents that never
// reached READY (or DELETED) and whose session started before the cutoff.
// Published files are never selected and are therefore always preserved.
func prepareQueryExpiredUnfinishedSessions() *dsl.Tmpl {
	tmpl := dsl.NewTmpl()
	root := dsl.NewRoot()
	must := root.Query().Bool().Must()
	must.Terms("file.Status", []string{string(file.StatusAwaiting), string(file.StatusProgress)}, nil)
	must.Range("upload_start", dsl.WithRangeLTE(tmpl.Bind("cutoff")))
	root.Size(10000)
	tmpl.MustResolve(root)
	return tmpl
}

// CleanupExpiredSessions reclaims unfinished upload sessions older than
// sessionTimeout.
//
// It is safe across agent retries, Fleet Server restarts and multiple
// concurrently running Fleet Servers:
//   - each metadata document is atomically flipped to DELETED, only if it
//     still belongs to the same generation (upload_id) and is still
//     unfinished;
//   - chunk documents are reclaimed by generation (upload_id), so a stale
//     cleanup task can never delete the chunks of a new upload that reused the
//     deterministic document ID;
//   - READY (published) files are never touched.
//
// It returns the number of sessions that were reclaimed.
func CleanupExpiredSessions(ctx context.Context, bulker bulk.Bulk, sessionTimeout time.Duration) (int, error) {
	span, ctx := apm.StartSpan(ctx, "cleanupExpiredUploads", "process")
	defer span.End()
	log := zerolog.Ctx(ctx)

	cutoff := time.Now().Add(-sessionTimeout).UnixMilli()
	query, err := queryExpiredUnfinishedSessions.Render(map[string]any{"cutoff": cutoff})
	if err != nil {
		return 0, err
	}

	res, err := bulker.Search(ctx, MetaIndexPattern(), query)
	if err != nil {
		return 0, err
	}

	client := bulker.Client()
	reclaimed := 0
	for _, hit := range res.Hits {
		var meta file.MetaDoc
		if err := json.Unmarshal(hit.Source, &meta); err != nil {
			log.Warn().Err(err).Str("docID", hit.ID).Msg("skipping expired upload session with unparsable metadata")
			continue
		}
		generation := meta.UploadID
		if generation == "" {
			log.Warn().Str("docID", hit.ID).Msg("skipping expired upload session without a generation identifier")
			continue
		}

		deleted, err := MarkDeleted(ctx, client, meta.Source, hit.ID, generation)
		if err != nil {
			log.Warn().Err(err).Str("docID", hit.ID).Str("uploadID", generation).Msg("failed to mark expired upload session deleted")
			continue
		}
		if !deleted {
			// The session was published, superseded by a new generation, or
			// reclaimed by another Fleet Server instance: do not touch chunks.
			continue
		}

		if err := DeleteChunksByGeneration(ctx, bulker, meta.Source, generation); err != nil {
			log.Warn().Err(err).Str("docID", hit.ID).Str("uploadID", generation).Msg("expired upload session marked deleted, but chunk cleanup failed")
			continue
		}
		// also reclaim chunks written before generations were tracked; new
		// generations are tagged and remain untouched
		if err := DeleteLegacyChunksForBID(ctx, bulker, meta.Source, hit.ID); err != nil {
			log.Warn().Err(err).Str("docID", hit.ID).Str("uploadID", generation).Msg("expired upload session marked deleted, but legacy chunk cleanup failed")
		}
		reclaimed++
		log.Info().Str("docID", hit.ID).Str("uploadID", generation).Msg("reclaimed expired upload session")
	}
	return reclaimed, nil
}

// MetaIndexPattern returns the wildcard pattern covering all upload metadata
// indices.
func MetaIndexPattern() string {
	return fmt.Sprintf(UploadHeaderIndexPattern, "*")
}
