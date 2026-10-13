// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package gc

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/file/uploader"
	"github.com/elastic/fleet-server/v7/internal/pkg/scheduler"
)

// getUploadsGCFunc returns the scheduled work that reclaims unfinished,
// expired agent file-upload sessions and their chunk documents. Reclamation
// is generation (upload_id) scoped inside the uploader package, so published
// files and newer uploads that reuse a document ID are never affected.
func getUploadsGCFunc(bulker bulk.Bulk, sessionTimeout time.Duration) scheduler.WorkFunc {
	return func(ctx context.Context) error {
		log := zerolog.Ctx(ctx).With().Str("ctx", "expired upload sessions cleanup").Logger()

		log.Debug().Dur("sessionTimeout", sessionTimeout).Msg("reclaim expired upload sessions")
		reclaimed, err := uploader.CleanupExpiredSessions(ctx, bulker, sessionTimeout)
		if err != nil {
			log.Debug().Err(err).Msg("failed to reclaim expired upload sessions")
			return err
		}
		log.Debug().Int("count", reclaimed).Msg("reclaimed expired upload sessions")
		return nil
	}
}
