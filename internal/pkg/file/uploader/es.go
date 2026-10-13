// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/dsl"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	"github.com/elastic/fleet-server/v7/internal/pkg/file/cbor"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/rs/zerolog"
	"go.elastic.co/apm/v2"
)

const (
	// integration name is substituted in
	UploadHeaderIndexPattern = ".fleet-fileds-fromhost-meta-%s"
	UploadDataIndexPattern   = ".fleet-fileds-fromhost-data-%s"

	// quarantineBIDSuffix marks the bid of a chunk that conflicted with an
	// already confirmed chunk. The conflicting bytes are kept under this
	// separate bid (and a separate document ID) so the confirmed chunk is
	// never overwritten and normal bid-scoped reads never see the data.
	quarantineBIDSuffix = ".conflict"
)

var (
	MatchChunksByGeneration = prepareQueryChunksByGeneration()
	MatchLegacyChunksByBID  = prepareQueryLegacyChunksByBID()

	// statuses under which an upload may still transition to a terminal status
	unfinishedStatuses = []string{string(file.StatusAwaiting), string(file.StatusProgress)}
)

func prepareQueryChunksByGeneration() *dsl.Tmpl {
	tmpl := dsl.NewTmpl()
	root := dsl.NewRoot()
	root.Query().Term(file.FieldUploadID, tmpl.Bind(file.FieldUploadID), nil)
	tmpl.MustResolve(root)
	return tmpl
}

// prepareQueryLegacyChunksByBID matches chunks written before upload
// generations were tracked (no upload_id field) for a given base ID. These are
// reclaimed together with the generation-tagged chunks, but new generations
// always tag their chunks and are therefore never touched.
func prepareQueryLegacyChunksByBID() *dsl.Tmpl {
	tmpl := dsl.NewTmpl()
	root := dsl.NewRoot()
	boolNode := root.Query().Bool()
	boolNode.Must().Term(file.FieldBaseID, tmpl.Bind(file.FieldBaseID), nil)
	boolNode.MustNot().Exists(file.FieldUploadID)
	tmpl.MustResolve(root)
	return tmpl
}

/*
	Metadata Doc Operations
*/

// CreateFileDoc creates the metadata document for a new upload session.
// It uses op_type=create so that a deterministic document ID
// (actionID.agentID) cannot silently overwrite another session: a collision
// returns es.ErrElasticVersionConflict and the caller decides whether to
// reuse the existing session or start a fresh generation.
func CreateFileDoc(ctx context.Context, bulker bulk.Bulk, doc []byte, source string, fileID string) (string, error) {
	span, ctx := apm.StartSpan(ctx, "createFileInfo", "create")
	defer span.End()
	return bulker.Create(ctx, fmt.Sprintf(UploadHeaderIndexPattern, source), fileID, doc, bulk.WithRefresh())
}

// ReplaceFileDoc overwrites the metadata document with a fresh upload
// generation. This is used when the deterministic document ID is reused after
// the previous session expired, failed or was deleted.
func ReplaceFileDoc(ctx context.Context, bulker bulk.Bulk, doc []byte, source string, fileID string) (string, error) {
	span, ctx := apm.StartSpan(ctx, "replaceFileInfo", "index")
	defer span.End()
	return bulker.Index(ctx, fmt.Sprintf(UploadHeaderIndexPattern, source), fileID, doc, bulk.WithRefresh())
}

// metaUpdateScript atomically transitions the file status, but only while the
// document still belongs to the same upload generation and its status is in
// the allowed set. Any other case becomes a noop so that:
//   - finalization can never publish a superseded generation
//   - garbage collection can never delete/recycle a published (READY) file
//   - concurrent finalizers converge on a single transition
const metaUpdateScript = `
if (ctx._source.upload_id != params.gen) {
  ctx.op = 'none';
} else if (params.allowed.contains(ctx._source.file.Status)) {
  ctx._source.file.Status = params.status;
  if (params.hash != '') {
    ctx._source.transithash = ['sha256':params.hash];
  }
  if (params.status == params.ready) {
    ctx._source.upload_end = params.end;
  }
} else {
  ctx.op = 'none';
}
`

type metaUpdateResponse struct {
	Result string     `json:"result"`
	Error  es.ErrorT `json:"error"`
}

// updateFileMetaStatus conditionally updates the metadata document.
// It returns true when the status transition happened and false when the
// script noop-ed (wrong generation, or status outside the allowed set).
// A version conflict is surfaced as es.ErrElasticVersionConflict so callers
// can re-read the document rather than assuming a state.
func updateFileMetaStatus(ctx context.Context, client *elasticsearch.Client, source string, fileID string, generation string, status file.Status, hash string) (bool, error) {
	span, ctx := apm.StartSpan(ctx, "updateFileInfo", "update")
	defer span.End()

	allowed := unfinishedStatuses
	body := map[string]any{
		"script": map[string]any{
			"lang":   "painless",
			"source": metaUpdateScript,
			"params": map[string]any{
				"gen":     generation,
				"allowed": allowed,
				"status":  string(status),
				"hash":    hash,
				"ready":   string(file.StatusDone),
				"end":     time.Now().UnixMilli(),
			},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return false, err
	}

	resp, err := client.Update(
		fmt.Sprintf(UploadHeaderIndexPattern, source),
		fileID,
		bytes.NewReader(payload),
		client.Update.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusConflict {
		return false, es.ErrElasticVersionConflict
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, es.TranslateError(resp.StatusCode, raw)
	}

	var response metaUpdateResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return false, err
	}
	if response.Error.Type != "" {
		return false, fmt.Errorf("%s: %s caused by %s: %s", response.Error.Type, response.Error.Reason, response.Error.Cause.Type, response.Error.Cause.Reason)
	}
	zerolog.Ctx(ctx).Trace().Int("status_code", resp.StatusCode).Str("result", response.Result).Msg("updated file metadata document")
	return response.Result == "updated", nil
}

/*
	Chunk Operations
*/

type chunkCreateResponse struct {
	Result string     `json:"result"`
	Error  es.ErrorT `json:"error"`
}

// createChunkDoc writes a single chunk document with op_type=create.
// The response HTTP status is returned so callers can handle a 409 collision
// idempotently.
func createChunkDoc(ctx context.Context, client *elasticsearch.Client, body io.Reader, source string, chunkDocID string) (int, error) {
	span, _ := apm.StartSpan(ctx, "createChunk", "create")
	defer span.End()
	// This create doc happens *many* times per file, so it must not use any expensive parameters (e.g. refresh)
	// for a 5GB file, that is 1,280 create calls
	resp, err := client.Create(fmt.Sprintf(UploadDataIndexPattern, source), chunkDocID, body, func(req *esapi.CreateRequest) {
		req.DocumentID = chunkDocID
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Set("Content-Type", "application/cbor")
		req.Header.Set("Accept", "application/json")
	})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode == http.StatusConflict {
		// expected on retried chunk PUTs; handled by the caller
		return resp.StatusCode, nil
	}

	var response chunkCreateResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return 0, err
	}
	zerolog.Ctx(ctx).Trace().Int("status_code", resp.StatusCode).Str("result", response.Result).Msg("uploaded chunk")
	if response.Error.Type != "" {
		return resp.StatusCode, fmt.Errorf("%s: %s caused by %s: %s", response.Error.Type, response.Error.Reason, response.Error.Cause.Type, response.Error.Cause.Reason)
	}
	return resp.StatusCode, nil
}

// WriteChunk stores a chunk document idempotently, keyed by chunk position and
// content hash.
//
//   - First write: created normally.
//   - Retry with identical content (e.g. the previous HTTP response was lost):
//     the create collides, the existing chunk's hash matches and success is
//     returned without modifying anything.
//   - Retry with different content: the confirmed chunk is never overwritten.
//     The incoming bytes are isolated under a quarantine document (different
//     bid and document ID, tagged with the same generation) and
//     ErrChunkContentConflict is returned, so the session must restart.
func (u *Uploader) WriteChunk(ctx context.Context, info file.Info, chunk file.ChunkInfo, data []byte) error {
	log := zerolog.Ctx(ctx)
	chunkDocID := fmt.Sprintf("%s.%d", chunk.BID, chunk.Pos)
	encoder := cbor.NewChunkWriter(bytes.NewReader(data), chunk.Last, chunk.BID, info.ID, chunk.SHA2, info.ChunkSize)

	status, err := createChunkDoc(ctx, u.chunkClient, encoder, info.Source, chunkDocID)
	if err != nil {
		return err
	}
	if status == http.StatusCreated || status == http.StatusOK {
		return nil
	}
	if status != http.StatusConflict {
		return fmt.Errorf("unexpected status %d indexing chunk %s", status, chunkDocID)
	}

	// A chunk document already exists at this position. Determine whether this
	// is an idempotent retry of the same content or a content conflict.
	existing, err := file.GetChunk(ctx, u.bulker, UploadDataIndexPattern, info.Source, chunk.BID, chunk.Pos)
	if err != nil {
		return fmt.Errorf("unable to read existing chunk while handling collision: %w", err)
	}
	if strings.EqualFold(existing.SHA2, chunk.SHA2) {
		log.Trace().Str("fileID", chunk.BID).Int("chunkNum", chunk.Pos).Msg("duplicate chunk with matching content hash accepted as idempotent retry")
		return nil
	}

	return u.quarantineConflictingChunk(ctx, info, chunk, data)
}

// quarantineConflictingChunk persists bytes that collided with a different,
// already confirmed chunk under an isolated document, then reports the
// conflict. The confirmed chunk is left untouched. Garbage collection removes
// quarantine documents with the rest of the failed generation.
func (u *Uploader) quarantineConflictingChunk(ctx context.Context, info file.Info, chunk file.ChunkInfo, data []byte) error {
	log := zerolog.Ctx(ctx)
	log.Warn().
		Str("fileID", chunk.BID).
		Int("chunkNum", chunk.Pos).
		Str("existingHash", chunk.SHA2).
		Msg("chunk content conflict: isolating incoming chunk without overwriting the confirmed chunk")

	quarantineID := fmt.Sprintf("conflict.%s.%d", info.ID, chunk.Pos)
	quarantineBID := chunk.BID + quarantineBIDSuffix
	// use the indefinite-length (final) encoding so the quarantine copy never
	// has to declare a fixed chunk size
	encoder := cbor.NewChunkWriter(bytes.NewReader(data), true, quarantineBID, info.ID, chunk.SHA2, 0)
	status, err := createChunkDoc(ctx, u.chunkClient, encoder, info.Source, quarantineID)
	if err != nil {
		return fmt.Errorf("chunk content conflict, and failed to isolate conflicting chunk: %w", err)
	}
	if status != http.StatusCreated && status != http.StatusOK && status != http.StatusConflict {
		return fmt.Errorf("chunk content conflict, unexpected status %d isolating conflicting chunk", status)
	}
	return ErrChunkContentConflict
}

// DeleteChunksByGeneration removes every chunk document belonging to a
// generation, including quarantine documents. Used by garbage collection
// after an expired session is marked DELETED.
func DeleteChunksByGeneration(ctx context.Context, bulker bulk.Bulk, source string, generation string) error {
	q, err := MatchChunksByGeneration.Render(map[string]any{
		file.FieldUploadID: generation,
	})
	if err != nil {
		return err
	}
	return runDeleteByQuery(ctx, bulker, fmt.Sprintf(UploadDataIndexPattern, source), q)
}

// DeleteLegacyChunksForBID removes chunks for a base ID that predate
// generation tagging (no upload_id field). A newer generation's chunks are
// tagged and therefore safe.
func DeleteLegacyChunksForBID(ctx context.Context, bulker bulk.Bulk, source string, baseID string) error {
	q, err := MatchLegacyChunksByBID.Render(map[string]any{
		file.FieldBaseID: baseID,
	})
	if err != nil {
		return err
	}
	return runDeleteByQuery(ctx, bulker, fmt.Sprintf(UploadDataIndexPattern, source), q)
}

func runDeleteByQuery(ctx context.Context, bulker bulk.Bulk, index string, q []byte) error {
	span, ctx := apm.StartSpan(ctx, "deleteChunks", "delete_by_query")
	defer span.End()
	client := bulker.Client()
	resp, err := client.DeleteByQuery([]string{index}, bytes.NewReader(q), client.DeleteByQuery.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return es.TranslateError(resp.StatusCode, raw)
	}
	return nil
}

func EnsureChunksIndexed(ctx context.Context, client *elasticsearch.Client, source string) error {
	req := esapi.IndicesRefreshRequest{
		Index: []string{fmt.Sprintf(UploadDataIndexPattern, source)},
	}
	resp, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		zerolog.Ctx(ctx).Warn().Int("status_code", resp.StatusCode).Msg("File Chunk Index refresh gave abnormal response")
	}
	return nil
}

// ErrUncertainPublish wraps the case where the response to a finalization
// publish cannot be classified; callers must re-read the session instead of
// rebuilding a second file.
var ErrUncertainPublish = errors.New("upload publish outcome is uncertain")
