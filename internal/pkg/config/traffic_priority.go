// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package config

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

const (
	defaultControlReservedRatio = 0.5
	defaultBackgroundWait       = 2 * time.Second
	defaultBulkBackgroundWait   = time.Second
)

// TrafficPriorityConfig configures class-aware overload protection. When
// disabled (default) Fleet Server behaves exactly as without the feature.
type TrafficPriorityConfig struct {
	// Enabled turns traffic classification and capacity reservation on.
	Enabled bool `config:"enabled"`

	// ControlReservedRatio derives the control reservation when an absolute
	// reserved value is left zero. Zero selects the default (0.5).
	ControlReservedRatio float64 `config:"control_reserved_ratio"`

	// Entry gate (HTTP in-flight requests).
	EntryMaxInFlight       int64         `config:"entry_max_in_flight"`
	EntryControlReserved   int64         `config:"entry_control_reserved"`
	BackgroundWaitTimeout  time.Duration `config:"background_wait_timeout"`
	MaxBackgroundWaiters   int           `config:"max_background_waiters"`

	// Bulk gate (concurrent flushes inside the bulk engine).
	BulkControlReserved          int64         `config:"bulk_control_reserved"`
	BulkBackgroundWaitTimeout    time.Duration `config:"bulk_background_wait_timeout"`
	BulkBackgroundFlushDelay     time.Duration `config:"bulk_background_flush_delay"`
	MaxPendingBackgroundDispatches int64       `config:"max_pending_background_dispatches"`

	// Elasticsearch transport gate (one permit per HTTP attempt, retries
	// re-acquire with the same class).
	ESMaxInFlight             int64         `config:"es_max_in_flight"`
	ESControlReserved         int64         `config:"es_control_reserved"`
	ESBackgroundWaitTimeout   time.Duration `config:"es_background_wait_timeout"`
}

// Validate checks raw configuration values. Relationships that require
// derived capacities (entry/es C from other config sections) are validated
// in Resolve.
func (c *TrafficPriorityConfig) Validate() error {
	var errs []error
	nonNeg := func(name string, v int64) {
		if v < 0 {
			errs = append(errs, fmt.Errorf("traffic_priority.%s must not be negative, got %d", name, v))
		}
	}
	nonNeg("entry_max_in_flight", c.EntryMaxInFlight)
	nonNeg("entry_control_reserved", c.EntryControlReserved)
	nonNeg("bulk_control_reserved", c.BulkControlReserved)
	nonNeg("max_pending_background_dispatches", c.MaxPendingBackgroundDispatches)
	nonNeg("es_max_in_flight", c.ESMaxInFlight)
	nonNeg("es_control_reserved", c.ESControlReserved)
	if c.MaxBackgroundWaiters < 0 {
		errs = append(errs, fmt.Errorf("traffic_priority.max_background_waiters must not be negative, got %d", c.MaxBackgroundWaiters))
	}
	if c.ControlReservedRatio < 0 || c.ControlReservedRatio > 1 {
		errs = append(errs, fmt.Errorf("traffic_priority.control_reserved_ratio must be within [0,1], got %v", c.ControlReservedRatio))
	}
	for name, d := range map[string]time.Duration{
		"background_wait_timeout":      c.BackgroundWaitTimeout,
		"bulk_background_wait_timeout": c.BulkBackgroundWaitTimeout,
		"bulk_background_flush_delay":  c.BulkBackgroundFlushDelay,
		"es_background_wait_timeout":   c.ESBackgroundWaitTimeout,
	} {
		if d < 0 {
			errs = append(errs, fmt.Errorf("traffic_priority.%s must not be negative, got %s", name, d))
		}
	}
	return joinErrors(errs)
}

// TrafficSnapshots are the resolved gate rules and bulk-only knobs applied at
// runtime. The three gate snapshots are swapped atomically on configuration
// reload.
type TrafficSnapshots struct {
	Entry traffic.Snapshot
	Bulk  traffic.Snapshot
	ES    traffic.Snapshot

	// BulkBackgroundFlushDelay bounds how long a background-only bulk queue
	// may be deferred before the scheduler forces a flush.
	BulkBackgroundFlushDelay time.Duration
	// MaxPendingBackgroundDispatches rejects background bulk dispatches above
	// this count; zero means unbounded.
	MaxPendingBackgroundDispatches int64
}

// Resolve derives the immutable gate snapshots from this config and the
// capacities owned by other configuration sections.
//
// maxConnections is server.limits.max_connections, flushMaxPending is
// server.bulk.flush_max_pending and esMaxConnections is
// output.elasticsearch.max_conn_per_host.
func (c *TrafficPriorityConfig) Resolve(maxConnections, flushMaxPending, esMaxConnections int64, flushInterval time.Duration) (*TrafficSnapshots, error) {
	if c == nil || !c.Enabled {
		return &TrafficSnapshots{
			Entry: traffic.Snapshot{Enabled: false},
			Bulk:  traffic.Snapshot{Enabled: false},
			ES:    traffic.Snapshot{Enabled: false},
		}, nil
	}

	ratio := c.ControlReservedRatio
	if ratio == 0 {
		ratio = defaultControlReservedRatio
	}
	bgWait := c.BackgroundWaitTimeout
	if bgWait == 0 {
		bgWait = defaultBackgroundWait
	}
	bulkBgWait := c.BulkBackgroundWaitTimeout
	if bulkBgWait == 0 {
		bulkBgWait = defaultBulkBackgroundWait
	}
	esBgWait := c.ESBackgroundWaitTimeout
	if esBgWait == 0 {
		esBgWait = defaultBackgroundWait
	}
	flushDelay := c.BulkBackgroundFlushDelay
	if flushDelay == 0 {
		flushDelay = 2 * flushInterval
	}

	entryC := orDefault(c.EntryMaxInFlight, maxConnections)
	bulkC := flushMaxPending
	esC := orDefault(c.ESMaxInFlight, esMaxConnections)

	snaps := &TrafficSnapshots{
		Entry: traffic.Snapshot{
			Enabled:               true,
			Capacity:              entryC,
			ReservedControl:       reserve(c.EntryControlReserved, entryC, ratio),
			BackgroundWaitTimeout: bgWait,
			MaxBackgroundWaiters:  c.MaxBackgroundWaiters,
		},
		Bulk: traffic.Snapshot{
			Enabled:               true,
			Capacity:              bulkC,
			ReservedControl:       reserve(c.BulkControlReserved, bulkC, ratio),
			BackgroundWaitTimeout: bulkBgWait,
		},
		ES: traffic.Snapshot{
			Enabled:               true,
			Capacity:              esC,
			ReservedControl:       reserve(c.ESControlReserved, esC, ratio),
			BackgroundWaitTimeout: esBgWait,
		},
		BulkBackgroundFlushDelay:       flushDelay,
		MaxPendingBackgroundDispatches: c.MaxPendingBackgroundDispatches,
	}

	var errs []error
	if err := snaps.Entry.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("traffic_priority entry gate: %w", err))
	}
	if err := snaps.Bulk.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("traffic_priority bulk gate: %w", err))
	}
	if err := snaps.ES.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("traffic_priority elasticsearch gate: %w", err))
	}
	if err := joinErrors(errs); err != nil {
		return nil, err
	}
	return snaps, nil
}

func orDefault(v, def int64) int64 {
	if v > 0 {
		return v
	}
	return def
}

// reserve picks the explicit reserved value when set, otherwise derives it
// from the ratio (rounded up, never zero).
func reserve(explicit, capacity int64, ratio float64) int64 {
	if explicit > 0 {
		return explicit
	}
	r := int64(math.Ceil(float64(capacity) * ratio))
	if r < 1 {
		return 1
	}
	if r > capacity {
		return capacity
	}
	return r
}

func joinErrors(errs []error) error {
	return errors.Join(errs...)
}
