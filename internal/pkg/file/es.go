// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/dsl"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"go.elastic.co/apm/v2"
)

const (
	FieldBaseID   = "bid"
	FieldLast     = "last"
	FieldSHA2     = "sha2"
	FieldUploadID = "upload_id"
)

var (
	queryChunkInfo             = prepareChunkInfo(false, false)
	queryChunkInfoWithSize     = prepareChunkInfo(true, false)
	queryChunkInfoGen          = prepareChunkInfo(false, true)
	queryChunkInfoWithSizeGen  = prepareChunkInfo(true, true)
	QueryChunkInfoWithSize     = queryChunkInfoWithSize
	QueryChunkInfo             = queryChunkInfo
	QueryUploadID              = prepareFindMetaByUploadID()
)

// get fields other than the byte payload (data)
// when byGeneration is true, the query filters on the upload session
// generation (upload_id) in addition to the base document id, so chunks left
// behind by a previous, abandoned session are never returned.
func prepareChunkInfo(size, byGeneration bool) *dsl.Tmpl {
	tmpl := dsl.NewTmpl()
	root := dsl.NewRoot()
	root.Param("_source", false)
	must := root.Query().Bool().Must()
	must.Term(FieldBaseID, tmpl.Bind(FieldBaseID), nil)
	if byGeneration {
		must.Term(FieldUploadID, tmpl.Bind(FieldUploadID), nil)
	}
	root.Param("fields", []string{FieldSHA2, FieldLast, FieldBaseID, FieldUploadID})
	if size {
		root.Param("script_fields", map[string]any{
			"size": map[string]any{
				"script": map[string]any{
					"lang":   "painless",
					"source": "params._source.data.length",
				},
			},
		})
	}
	root.Size(10000)
	tmpl.MustResolve(root)
	return tmpl
}

func prepareFindMetaByUploadID() *dsl.Tmpl {
	tmpl := dsl.NewTmpl()
	root := dsl.NewRoot()
	root.Query().Term(FieldUploadID, tmpl.Bind(FieldUploadID), nil)
	tmpl.MustResolve(root)
	return tmpl
}

func GetMetadata(ctx context.Context, bulker bulk.Bulk, indexPattern string, uploadID string) ([]es.HitT, error) {
	span, ctx := apm.StartSpan(ctx, "getFileInfo", "search")
	defer span.End()
	query, err := QueryUploadID.Render(map[string]any{
		FieldUploadID: uploadID,
	})
	if err != nil {
		return nil, err
	}

	res, err := bulker.Search(ctx, fmt.Sprintf(indexPattern, "*"), query)
	if err != nil {
		return nil, err
	}

	return res.Hits, nil
}

// Retrieves a file Metadata as an Info object
func GetInfo(ctx context.Context, bulker bulk.Bulk, indexPattern string, uploadID string) (Info, error) {
	results, err := GetMetadata(ctx, bulker, indexPattern, uploadID)
	if err != nil {
		return Info{}, err
	}
	if len(results) == 0 {
		return Info{}, ErrInvalidID
	}
	if len(results) > 1 {
		return Info{}, fmt.Errorf("unable to locate upload record, got %d records, expected 1", len(results))
	}

	return infoFromHit(results[0])
}

// GetInfoByDocID retrieves a file Metadata by its deterministic document ID
// (actionID.agentID) instead of its session upload_id. Used when an upload
// begin collides with an existing session document.
func GetInfoByDocID(ctx context.Context, bulker bulk.Bulk, index, docID string) (Info, error) {
	span, ctx := apm.StartSpan(ctx, "getFileInfoByDocID", "read")
	defer span.End()
	out, err := bulker.Read(ctx, index, docID)
	if err != nil {
		return Info{}, err
	}
	var fi MetaDoc
	if err := json.Unmarshal(out, &fi); err != nil {
		return Info{}, fmt.Errorf("file meta doc parsing error: %w", err)
	}
	return infoFromMeta(docID, fi), nil
}

func infoFromHit(hit es.HitT) (Info, error) {
	var fi MetaDoc
	if err := json.Unmarshal(hit.Source, &fi); err != nil {
		return Info{}, fmt.Errorf("file meta doc parsing error: %w", err)
	}
	return infoFromMeta(hit.ID, fi), nil
}

func infoFromMeta(docID string, fi MetaDoc) Info {
	info := Info{
		ID:         fi.UploadID,
		DocID:      docID,
		Source:     fi.Source,
		AgentID:    fi.AgentID,
		ActionID:   fi.ActionID,
		Namespaces: fi.Namespaces,
		ChunkSize:  fi.File.ChunkSize,
		Total:      fi.File.Size,
		Start:      fi.Start,
		Status:     Status(fi.File.Status),
	}
	if fi.File.ChunkSize > 0 {
		// calculate number of chunks required (0 for a zero-byte file)
		info.Count = int((fi.File.Size + fi.File.ChunkSize - 1) / fi.File.ChunkSize)
	}
	if fi.TransitHash != nil {
		info.TransitHash = fi.TransitHash.SHA2
	}
	return info
}

// retrieves a full chunk document, Data included
func GetChunk(ctx context.Context, bulker bulk.Bulk, indexPattern string, source string, fileID string, chunkNum int) (Chunk, error) {
	var chunk Chunk
	out, err := bulker.Read(ctx, fmt.Sprintf(indexPattern, source), fmt.Sprintf("%s.%d", fileID, chunkNum))
	if err != nil {
		return chunk, err
	}
	err = json.Unmarshal(out, &chunk)
	return chunk, err
}

type GetChunkInfoOpt struct {
	IncludeSize bool
	RequireHash bool
	// UploadID restricts results to chunks belonging to this upload session
	// generation. Empty means no generation filter (e.g. files written by
	// Kibana, which predate generation-tagged chunks).
	UploadID string
}

// Retrieves a subset of chunk document fields, specifically omitting the Data payload (bytes)
// the chunk's ordered index position (Pos) is also parsed from the document ID.
// Optionally adding the calculated field "size", that is the length, in bytes, of the Data field.
// and optionally validating that a hash field is present
func GetChunkInfos(ctx context.Context, bulker bulk.Bulk, index string, baseID string, opt GetChunkInfoOpt) ([]ChunkInfo, error) {
	span, ctx := apm.StartSpan(ctx, "getChunksInfo", "process")
	defer span.End()
	tpl := queryChunkInfo
	switch {
	case opt.IncludeSize && opt.UploadID != "":
		tpl = queryChunkInfoWithSizeGen
	case opt.IncludeSize:
		tpl = queryChunkInfoWithSize
	case opt.UploadID != "":
		tpl = queryChunkInfoGen
	}
	params := map[string]any{
		FieldBaseID: baseID,
	}
	if opt.UploadID != "" {
		params[FieldUploadID] = opt.UploadID
	}
	query, err := tpl.Render(params)
	if err != nil {
		return nil, err
	}

	bSpan, bCtx := apm.StartSpan(ctx, "searchChunksInfo", "search")
	res, err := bulker.Search(bCtx, index, query)
	bSpan.End()
	if err != nil {
		return nil, err
	}

	chunks := make([]ChunkInfo, len(res.Hits))

	var (
		bid      string
		last     bool
		sha2     string
		uploadID string
		size     int
		ok       bool
	)

	vSpan, _ := apm.StartSpan(ctx, "validateChunksInfo", "validate")
	defer vSpan.End()
	for i, h := range res.Hits {
		if bid, ok = getResultsFieldString(h.Fields, FieldBaseID); !ok {
			return nil, fmt.Errorf("unable to retrieve %s field from chunk document", FieldBaseID)
		}
		if last, ok = getResultsFieldBool(h.Fields, FieldLast); !ok {
			// Files written by Kibana omit this field for all intermediate chunks
			// and only write last:true on final chunk. False by default
			last = false
		}
		if sha2, ok = getResultsFieldString(h.Fields, FieldSHA2); opt.RequireHash && !ok {
			return nil, fmt.Errorf("unable to retrieve %s field from chunk document", FieldSHA2)
		}
		// generation is optional for older/Kibana-written chunks
		uploadID, _ = getResultsFieldString(h.Fields, FieldUploadID)
		if size, ok = getResultsFieldInt(h.Fields, "size"); opt.IncludeSize && !ok {
			return nil, errors.New("unable to retrieve size from chunk document")
		}
		chunkid := strings.TrimPrefix(h.ID, bid+".")
		chunkNum, err := strconv.Atoi(chunkid)
		if err != nil {
			return nil, fmt.Errorf("unable to parse chunk number from id %s: %w", h.ID, err)
		}
		chunks[i] = ChunkInfo{
			Pos:      chunkNum,
			BID:      bid,
			Last:     last,
			SHA2:     sha2,
			UploadID: uploadID,
			Size:     size,
			Index:    h.Index,
			ID:       h.ID,
		}
	}

	return chunks, nil
}

// convenience function for translating the elasticsearch "field" response format
// of "field": { "a": [value], "b": [value] }
func getResultField(fields map[string]any, key string) (any, bool) {
	array, ok := fields[key].([]any)
	if !ok {
		return nil, false
	}
	if len(array) < 1 {
		return nil, false
	}
	return array[0], true
}

func getResultsFieldString(fields map[string]any, key string) (string, bool) {
	val, ok := getResultField(fields, key)
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}
func getResultsFieldBool(fields map[string]any, key string) (bool, bool) {
	val, ok := getResultField(fields, key)
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}
func getResultsFieldInt(fields map[string]any, key string) (int, bool) {
	val, ok := getResultField(fields, key)
	if !ok {
		return 0, false
	}
	switch n := val.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}
