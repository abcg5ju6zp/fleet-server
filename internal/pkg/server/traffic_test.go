// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package server

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
)

func trafficTestConfig(tp config.TrafficPriorityConfig) *config.Config {
	return &config.Config{
		Inputs: []config.Input{{
			Server: config.Server{
				Limits: config.ServerLimits{MaxConnections: 100},
				Bulk: config.ServerBulk{
					FlushInterval:   250 * time.Millisecond,
					FlushMaxPending: 8,
				},
				TrafficPriority: tp,
			},
		}},
		Output: config.Output{
			Elasticsearch: config.Elasticsearch{MaxConnPerHost: 128},
		},
	}
}

func TestTrafficGatesApplyKeepsLastGood(t *testing.T) {
	gates := newTrafficGates()

	// Initial disabled config.
	snaps, err := gates.Apply(trafficTestConfig(config.TrafficPriorityConfig{}))
	require.NoError(t, err)
	require.NotNil(t, snaps)
	assert.False(t, gates.entry.Enabled())

	// Valid enabled config.
	snaps, err = gates.Apply(trafficTestConfig(config.TrafficPriorityConfig{
		Enabled:               true,
		EntryMaxInFlight:      10,
		EntryControlReserved:  4,
		BulkControlReserved:   3,
		ESMaxInFlight:         64,
		BackgroundWaitTimeout: 2 * time.Second,
	}))
	require.NoError(t, err)
	assert.True(t, gates.entry.Enabled())
	assert.Equal(t, int64(10), snaps.Entry.Capacity)
	assert.Equal(t, int64(4), snaps.Entry.ReservedControl)
	assert.Equal(t, int64(3), snaps.Bulk.ReservedControl)
	assert.Equal(t, int64(64), snaps.ES.Capacity)
	st := gates.status()
	assert.True(t, st.OK)

	// Invalid config is rejected: previous snapshot survives, status
	// records the failure.
	bad := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 2, EntryControlReserved: 9})
	_, err = gates.Apply(bad)
	require.Error(t, err)
	assert.Equal(t, int64(10), gates.entry.CurrentSnapshot().Capacity, "last good snapshot kept")
	assert.True(t, gates.entry.Enabled())
	st = gates.status()
	assert.False(t, st.OK)
	assert.NotEmpty(t, st.Error)
}

func TestReloadAction(t *testing.T) {
	cur := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 10})

	// Initial start.
	restart, _, reconfigure := reloadAction(nil, cur, nil)
	assert.True(t, restart)
	assert.False(t, reconfigure)

	// Traffic-only valid change: no restart, live reconfigure only.
	next := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 20})
	restart, _, reconfigure = reloadAction(cur, next, nil)
	assert.False(t, restart)
	assert.True(t, reconfigure)

	// No change at all.
	same := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 10})
	restart, _, reconfigure = reloadAction(cur, same, nil)
	assert.False(t, restart)
	assert.False(t, reconfigure)

	// Invalid traffic change (already normalized to previous by caller):
	// neither restart nor reconfigure.
	bad := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 10})
	restart, _, reconfigure = reloadAction(cur, bad, errors.New("boom"))
	assert.False(t, restart)
	assert.False(t, reconfigure)

	// Unrelated server change triggers a restart even with traffic changed.
	timeoutChange := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 20})
	timeoutChange.Inputs[0].Server.Timeouts.CheckinLongPoll = 42 * time.Second
	doRestart, why, doReconfigure := reloadAction(cur, timeoutChange, nil)
	assert.True(t, doRestart)
	assert.False(t, doReconfigure)
	assert.NotEmpty(t, why)

	// Output change triggers a restart.
	outChange := trafficTestConfig(config.TrafficPriorityConfig{Enabled: true, EntryMaxInFlight: 20})
	outChange.Output.Elasticsearch.MaxConnPerHost = 64
	doRestart, _, doReconfigure = reloadAction(cur, outChange, nil)
	assert.True(t, doRestart)
	assert.False(t, doReconfigure)
}

func TestConfigureBulker(t *testing.T) {
	gates := newTrafficGates()
	snaps, err := gates.Apply(trafficTestConfig(config.TrafficPriorityConfig{
		Enabled:                        true,
		MaxPendingBackgroundDispatches: 7,
	}))
	require.NoError(t, err)
	require.NotNil(t, snaps)
	assert.Equal(t, 500*time.Millisecond, snaps.BulkBackgroundFlushDelay, "default 2x flush interval")

	b := bulk.NewBulker(nil, nil, bulk.WithTrafficScheduling(gates.bulk, 0, 0))
	gates.configureBulker(b)
	assert.Equal(t, int64(7), b.BackgroundDispatchLimit())

	// nil bulker must be a no-op.
	gates.configureBulker(nil)
}
