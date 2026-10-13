// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package config

import (
	"testing"
	"time"

	"github.com/elastic/go-ucfg"
	"github.com/elastic/go-ucfg/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrafficPriorityDisabledResolve(t *testing.T) {
	var c TrafficPriorityConfig
	require.NoError(t, c.Validate())

	snaps, err := c.Resolve(100, 8, 128, 250*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, snaps.Entry.Enabled)
	assert.False(t, snaps.Bulk.Enabled)
	assert.False(t, snaps.ES.Enabled)
}

func TestTrafficPriorityResolveDefaults(t *testing.T) {
	c := TrafficPriorityConfig{Enabled: true}
	snaps, err := c.Resolve(100, 8, 128, 250*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, int64(100), snaps.Entry.Capacity)
	assert.Equal(t, int64(50), snaps.Entry.ReservedControl)
	assert.Equal(t, 2*time.Second, snaps.Entry.BackgroundWaitTimeout)

	assert.Equal(t, int64(8), snaps.Bulk.Capacity)
	assert.Equal(t, int64(4), snaps.Bulk.ReservedControl)
	assert.Equal(t, time.Second, snaps.Bulk.BackgroundWaitTimeout)
	assert.Equal(t, 500*time.Millisecond, snaps.BulkBackgroundFlushDelay)

	assert.Equal(t, int64(128), snaps.ES.Capacity)
	assert.Equal(t, int64(64), snaps.ES.ReservedControl)
	assert.Equal(t, 2*time.Second, snaps.ES.BackgroundWaitTimeout)
}

func TestTrafficPriorityResolveExplicit(t *testing.T) {
	c := TrafficPriorityConfig{
		Enabled:                    true,
		EntryMaxInFlight:           40,
		EntryControlReserved:       10,
		BackgroundWaitTimeout:      3 * time.Second,
		MaxBackgroundWaiters:       5,
		BulkControlReserved:        6,
		BulkBackgroundFlushDelay:   750 * time.Millisecond,
		ESMaxInFlight:              64,
		ESControlReserved:          12,
		MaxPendingBackgroundDispatches: 99,
	}
	snaps, err := c.Resolve(100, 8, 128, 250*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, int64(40), snaps.Entry.Capacity)
	assert.Equal(t, int64(10), snaps.Entry.ReservedControl)
	assert.Equal(t, 5, snaps.Entry.MaxBackgroundWaiters)
	assert.Equal(t, int64(6), snaps.Bulk.ReservedControl)
	assert.Equal(t, int64(64), snaps.ES.Capacity)
	assert.Equal(t, int64(12), snaps.ES.ReservedControl)
	assert.Equal(t, int64(99), snaps.MaxPendingBackgroundDispatches)
	assert.Equal(t, 750*time.Millisecond, snaps.BulkBackgroundFlushDelay)
}

func TestTrafficPriorityResolveRatio(t *testing.T) {
	c := TrafficPriorityConfig{Enabled: true, ControlReservedRatio: 0.25}
	snaps, err := c.Resolve(100, 8, 128, 250*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(25), snaps.Entry.ReservedControl)
	assert.Equal(t, int64(2), snaps.Bulk.ReservedControl) // ceil(2)
	assert.Equal(t, int64(32), snaps.ES.ReservedControl)
}

func TestTrafficPriorityResolveErrors(t *testing.T) {
	cases := []struct {
		name string
		c    TrafficPriorityConfig
	}{
		{"entry reserved exceeds capacity", TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 4, EntryControlReserved: 5}},
		{"zero derived entry capacity", TrafficPriorityConfig{Enabled: true}},
		{"zero derived bulk capacity", TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 4}},
	}
	// zero derived capacities only error when the corresponding upstream
	// capacity is zero/unset.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			switch tc.name {
			case "entry reserved exceeds capacity":
				_, err := tc.c.Resolve(100, 8, 128, 250*time.Millisecond)
				require.Error(t, err)
			case "zero derived entry capacity":
				_, err := tc.c.Resolve(0, 8, 128, 250*time.Millisecond)
				require.Error(t, err)
			case "zero derived bulk capacity":
				_, err := tc.c.Resolve(100, 0, 128, 250*time.Millisecond)
				require.Error(t, err)
			}
		})
	}
}

func TestTrafficPriorityRawValidation(t *testing.T) {
	require.Error(t, (&TrafficPriorityConfig{ControlReservedRatio: 2}).Validate())
	require.Error(t, (&TrafficPriorityConfig{EntryMaxInFlight: -1}).Validate())
	require.Error(t, (&TrafficPriorityConfig{MaxBackgroundWaiters: -1}).Validate())
	require.Error(t, (&TrafficPriorityConfig{BackgroundWaitTimeout: -time.Second}).Validate())
}

func TestTrafficPriorityUnpack(t *testing.T) {
	raw := map[string]any{
		"enabled":                  true,
		"entry_max_in_flight":      40,
		"entry_control_reserved":   10,
		"control_reserved_ratio":   0.25,
		"background_wait_timeout":  "5s",
		"max_background_waiters":   3,
		"bulk_control_reserved":    3,
		"es_max_in_flight":         64,
		"es_control_reserved":      8,
		"es_background_wait_timeout": "250ms",
	}
	cfg, err := ucfg.NewFrom(raw, DefaultOptions...)
	require.NoError(t, err)

	var tp TrafficPriorityConfig
	require.NoError(t, cfg.Unpack(&tp, DefaultOptions...))
	assert.True(t, tp.Enabled)
	assert.Equal(t, int64(40), tp.EntryMaxInFlight)
	assert.Equal(t, int64(10), tp.EntryControlReserved)
	assert.Equal(t, 0.25, tp.ControlReservedRatio)
	assert.Equal(t, 5*time.Second, tp.BackgroundWaitTimeout)
	assert.Equal(t, 3, tp.MaxBackgroundWaiters)
	assert.Equal(t, int64(3), tp.BulkControlReserved)
	assert.Equal(t, int64(64), tp.ESMaxInFlight)
	assert.Equal(t, int64(8), tp.ESControlReserved)
	assert.Equal(t, 250*time.Millisecond, tp.ESBackgroundWaitTimeout)
}

func TestTrafficPriorityUnpackInvalid(t *testing.T) {
	// Validate runs during unpack: invalid ratios must surface as load errors.
	cfg, err := yaml.NewConfig([]byte(`
enabled: true
control_reserved_ratio: 2
`), DefaultOptions...)
	require.NoError(t, err)

	var tp TrafficPriorityConfig
	require.Error(t, cfg.Unpack(&tp, DefaultOptions...))
}

func TestTrafficPriorityUnpackAsServerSection(t *testing.T) {
	cfg, err := yaml.NewConfig([]byte(`
server:
  traffic_priority:
    enabled: true
    entry_max_in_flight: 10
`), DefaultOptions...)
	require.NoError(t, err)

	type wrapper struct {
		Server Server `config:"server"`
	}
	var w wrapper
	require.NoError(t, cfg.Unpack(&w, DefaultOptions...))
	assert.True(t, w.Server.TrafficPriority.Enabled)
	assert.Equal(t, int64(10), w.Server.TrafficPriority.EntryMaxInFlight)
}
