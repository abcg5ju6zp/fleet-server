// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package gc

import (
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/file/uploader"
	"github.com/elastic/fleet-server/v7/internal/pkg/scheduler"
)

const (
	defaultScheduleInterval            = time.Hour
	defaultCleanupIntervalAfterExpired = "30d"                                      // cleanup with expiration older than 30 days from now
	defaultUploadSessionTimeout        = uploader.DefaultUploadLifetime            // unfinished upload sessions older than this are reclaimed
)

// Schedules returns the GC schedules
func Schedules(bulker bulk.Bulk, scheduleInterval time.Duration, cleanupIntervalAfterExpired string, uploadSessionTimeout time.Duration) []scheduler.Schedule {
	if scheduleInterval == 0 {
		scheduleInterval = defaultScheduleInterval
	}
	if cleanupIntervalAfterExpired == "" {
		cleanupIntervalAfterExpired = defaultCleanupIntervalAfterExpired
	}
	if uploadSessionTimeout == 0 {
		uploadSessionTimeout = defaultUploadSessionTimeout
	}

	return []scheduler.Schedule{
		{
			Name:     "fleet actions cleanup",
			Interval: scheduleInterval,
			WorkFn:   getActionsGCFunc(bulker, cleanupIntervalAfterExpired),
		},
		{
			Name:     "expired file upload sessions cleanup",
			Interval: scheduleInterval,
			WorkFn:   getUploadsGCFunc(bulker, uploadSessionTimeout),
		},
	}
}
