// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.elastic.co/apm/v2"

	"github.com/elastic/fleet-server/v7/internal/pkg/file"
)

const cleanupTimeout = 30 * time.Second

var (
	ErrFailValidation  = errors.New("file contents failed validation")
	ErrStatusNoUploads = errors.New("file closed, not accepting uploads")
)

func (u *Uploader) Complete(ctx context.Context, id string, transitHash string) (file.Info, error) {
	span, ctx := apm.StartSpan(ctx, "completeUpload", "process")
	defer span.End()
	// always fetch fresh (never the cache) so that status checks reflect the
	// outcome of concurrent finalizations and garbage collection
	info, err := file.GetInfo(ctx, u.bulker, UploadHeaderIndexPattern, id)
	if err != nil {
		return info, err
	}

	// A previously completed session: finalization is idempotent. When the
	// supplied digest matches the published one, return the original result.
	// The file is never rebuilt or duplicated.
	if info.Published() {
		if strings.EqualFold(info.TransitHash, transitHash) {
			return info, nil
		}
		return info, ErrStatusNoUploads
	}
	// if failed or deleted, exit
	if !info.StatusCanUpload() {
		return info, ErrStatusNoUploads
	}

	// complete may be called before most recent chunks are available for search yet
	// this explicitly calls refresh once at the end, instead of refreshing on each chunk
	if err := EnsureChunksIndexed(ctx, u.bulker.Client(), info.Source); err != nil {
		return info, fmt.Errorf("unable to refresh chunk data index: %w", err)
	}

	chunks, err := u.getChunksForValidation(ctx, info)
	if err != nil {
		return info, err
	}

	vSpan, _ := apm.StartSpan(ctx, "validateUpload", "validate")
	if !u.allChunksPresent(ctx, info, chunks) {
		vSpan.End()
		// missing chunks are a client-retriable condition; do not delete or
		// fail the session yet, the agent may still upload the missing chunks
		return info, ErrMissingChunks
	}
	if !u.verifyChunkInfo(ctx, info, chunks, transitHash) {
		vSpan.End()
		u.failUpload(ctx, info)
		return info, ErrFailValidation
	}
	vSpan.End()

	/*
		Upload OK. Atomically publish the final metadata. Only once the status
		flips to READY may the file be served whole or via Range requests.
	*/
	published, err := Publish(ctx, u.bulker.Client(), info, transitHash)
	if err != nil {
		// The publish response is uncertain (timeout, transport error,
		// version conflict): read the original session outcome. Never rebuild
		// a second file.
		if recovered, ok := u.recoverPublished(ctx, id, transitHash, err); ok {
			return recovered, nil
		}
		return info, err
	}
	if !published {
		// The conditional update noop-ed: another transition already happened
		// or the generation was superseded. Re-read the authoritative state.
		if recovered, ok := u.recoverPublished(ctx, id, transitHash, ErrUncertainPublish); ok {
			return recovered, nil
		}
		return info, ErrUncertainPublish
	}

	info.Status = file.StatusDone
	info.TransitHash = transitHash
	u.cache.SetUpload(info.ID, info)
	return info, nil
}

// getChunksForValidation fetches the session's chunk metadata, scoped to the
// current generation. When chunks appear missing, it falls back once to an
// unscoped lookup and accepts chunks without a generation tag: these are
// legacy chunks written before generations were tracked. Chunks tagged with a
// different generation are always excluded.
func (u *Uploader) getChunksForValidation(ctx context.Context, info file.Info) ([]file.ChunkInfo, error) {
	idx := fmt.Sprintf(UploadDataIndexPattern, info.Source)
	opt := file.GetChunkInfoOpt{IncludeSize: true, RequireHash: true, UploadID: info.ID}
	chunks, err := file.GetChunkInfos(ctx, u.bulker, idx, info.DocID, opt)
	if err != nil {
		return nil, err
	}
	if info.Count == 0 || len(chunks) >= info.Count {
		return chunks, nil
	}

	legacy, err := file.GetChunkInfos(ctx, u.bulker, idx, info.DocID, file.GetChunkInfoOpt{
		IncludeSize: true,
		RequireHash: true,
	})
	if err != nil {
		return nil, err
	}
	accepted := make([]file.ChunkInfo, 0, len(legacy))
	for _, c := range legacy {
		if c.UploadID == "" || c.UploadID == info.ID {
			accepted = append(accepted, c)
		}
	}
	return accepted, nil
}

// failUpload marks the session failed and reclaims the generation's chunks.
// Best effort: errors are logged but never mask the validation failure
// returned to the caller.
func (u *Uploader) failUpload(ctx context.Context, info file.Info) {
	log := zerolog.Ctx(ctx)
	// detach from the request context so cleanup can finish if the client went away
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()

	if err := MarkFailed(cleanupCtx, u.bulker.Client(), info); err != nil {
		log.Warn().Err(err).Str("fileID", info.DocID).Str("uploadID", info.ID).Msg("file upload failed chunk validation, but encountered an error setting the upload status to failure")
	}
	// delete every chunk document of this generation (tagged chunks and
	// conflict-isolated quarantine copies alike)
	if err := DeleteChunksByGeneration(cleanupCtx, u.bulker, info.Source, info.ID); err != nil {
		log.Warn().Err(err).Str("fileID", info.DocID).Str("uploadID", info.ID).Msg("file upload failed chunk validation, but encountered an error deleting left-behind chunk data")
	}
}

// recoverPublished re-reads the session metadata when a publish response is
// uncertain. It reports success only when the session is READY with the
// expected transit digest.
func (u *Uploader) recoverPublished(ctx context.Context, id string, transitHash string, cause error) (file.Info, bool) {
	log := zerolog.Ctx(ctx)
	info, err := file.GetInfo(ctx, u.bulker, UploadHeaderIndexPattern, id)
	if err != nil {
		log.Warn().Err(err).Str("uploadID", id).Msg("unable to re-read upload session after uncertain publish response")
		return file.Info{}, false
	}
	if info.Published() && strings.EqualFold(info.TransitHash, transitHash) {
		log.Info().Err(cause).Str("uploadID", id).Msg("publish response was uncertain, but the upload is already published with a matching digest")
		return info, true
	}
	log.Warn().Err(cause).Str("uploadID", id).Str("status", string(info.Status)).Msg("publish outcome uncertain and session is not published with the expected digest")
	return file.Info{}, false
}

func (u *Uploader) allChunksPresent(ctx context.Context, info file.Info, chunks []file.ChunkInfo) bool {
	log := zerolog.Ctx(ctx)
	// check overall count (zero chunks expected for a zero-byte file)
	if len(chunks) != info.Count {
		log.Warn().Int("expectedCount", info.Count).Int("received", len(chunks)).Interface("chunks", chunks).Msg("mismatch number of chunks")
		return false
	}

	// now ensure all positions are accounted for, no gaps, etc
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Pos < chunks[j].Pos
	})

	for i, c := range chunks {
		if c.Pos != i {
			log.Warn().Int("expected", i).Interface("chunk", c).Msg("chunk position doesn't match. May be a gap in uploaded file")
			return false
		}
		if c.UploadID != "" && c.UploadID != info.ID {
			log.Warn().Str("expected", info.ID).Str("got", c.UploadID).Msg("chunk belongs to a different upload generation")
			return false
		}
	}

	return true
}

func (u *Uploader) verifyChunkInfo(ctx context.Context, info file.Info, chunks []file.ChunkInfo, transitHash string) bool {
	log := zerolog.Ctx(ctx)
	// verify all chunks except last are info.ChunkSize size
	// verify last: false (or field excluded) for all except final chunk
	// verify final chunk is last: true
	// verify the total byte count matches the pinned file size
	// verify hash

	hasher := sha256.New()
	var totalSize int

	for i, chunk := range chunks {
		if i < info.Count-1 {
			// all chunks except last must have last:false
			// and be PRECISELY info.ChunkSize bytes long
			if chunk.Last {
				log.Debug().Int("chunkID", i).Msg("non-final chunk was incorrectly marked last")
				return false
			}
			if chunk.Size != int(info.ChunkSize) {
				log.Debug().Int64("requiredSize", info.ChunkSize).Int("chunkID", i).Int("gotSize", chunk.Size).Msg("chunk was undersized")
				return false
			}
		} else {
			// last chunk must be marked last:true
			// and can be any valid size (0,ChunkSize]
			if !chunk.Last {
				log.Debug().Int("chunkID", i).Msg("final chunk was not marked as final")
				return false
			}
			if chunk.Size == 0 {
				log.Debug().Int("chunkID", i).Msg("final chunk was 0 size")
				return false
			}
			if chunk.Size > int(info.ChunkSize) {
				log.Debug().Int("chunk-size", chunk.Size).Int("maxsize", int(info.ChunkSize)).Msg("final chunk was oversized")
				return false
			}
		}
		totalSize += chunk.Size

		// write the byte-decoded hash for this chunk to the
		// running hash for the entire file (transithash)
		rawHash, err := hex.DecodeString(chunk.SHA2)
		if err != nil {
			log.Warn().Err(err).Msg("error decoding chunk hash")
			return false
		}
		if n, err := hasher.Write(rawHash); err != nil {
			log.Error().Err(err).Msg("error computing transitHash from component chunk hashes")
			return false
		} else if n != len(rawHash) {
			log.Error().Int("wrote", n).Int("expected", len(rawHash)).Msg("transitHash calculation failure, could not write to hasher")
			return false
		}
	}

	if int64(totalSize) != info.Total {
		log.Warn().Int64("expectedSize", info.Total).Int("actualSize", totalSize).Msg("sum of chunk sizes does not match the pinned file size")
		return false
	}

	calcHash := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(transitHash, calcHash) {
		log.Warn().Str("provided-hash", transitHash).Str("calc-hash", calcHash).Msg("file upload streaming hash does not match")
		return false
	}

	return true
}
