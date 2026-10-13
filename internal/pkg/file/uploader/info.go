// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package uploader

import (
	"context"

	"github.com/elastic/fleet-server/v7/internal/pkg/file"
	"github.com/elastic/go-elasticsearch/v8"
)

// MarkFailed atomically marks an upload session as failed, provided the
// document still belongs to the generation and has not already reached a
// terminal status. A noop result is not an error.
func MarkFailed(ctx context.Context, client *elasticsearch.Client, info file.Info) error {
	_, err := updateFileMetaStatus(ctx, client, info.Source, info.DocID, info.ID, file.StatusFail, "")
	return err
}

// Publish atomically publishes the final metadata (READY + transit hash) for a
// validated upload. It returns true when this call performed the transition.
// A false result means the document was not in an publishable state for this
// generation (already transitioned, or superseded by a new generation).
func Publish(ctx context.Context, client *elasticsearch.Client, info file.Info, hash string) (bool, error) {
	return updateFileMetaStatus(ctx, client, info.Source, info.DocID, info.ID, file.StatusDone, hash)
}

// MarkDeleted atomically marks an expired, unfinished upload session as
// deleted. The document must still belong to the given generation and be in an
// unfinished status; READY files are never deleted. Returns true only when the
// transition happened, so callers know they own the reclamation of chunks.
func MarkDeleted(ctx context.Context, client *elasticsearch.Client, source string, docID string, generation string) (bool, error) {
	return updateFileMetaStatus(ctx, client, source, docID, generation, file.StatusDel, "")
}
