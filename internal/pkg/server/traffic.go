// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package server

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/api"
	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/traffic"
)

// trafficGates groups the three class-aware gates shared across listeners,
// the bulk engine and the Elasticsearch transports. The gates live for the
// whole Fleet.Run so traffic-only configuration changes are applied by
// atomic snapshot swaps without restarting subsystems.
type trafficGates struct {
	entry *traffic.Gate
	bulk  *traffic.Gate
	es    *traffic.Gate

	snaps atomic.Pointer[config.TrafficSnapshots]

	l                sync.RWMutex
	lastApplyOK      bool
	lastApplyAt      time.Time
	lastApplyError   string
	lastApplyVersion int64
}

func newTrafficGates() *trafficGates {
	g := &trafficGates{lastApplyOK: true}
	for _, tgt := range []**traffic.Gate{&g.entry, &g.bulk, &g.es} {
		gate, err := traffic.NewGate(nil) // disabled until the first Apply
		if err != nil {
			panic(err)
		}
		*tgt = gate
	}
	return g
}

// applyStatus is a point-in-time view of the last reload outcome, used by the
// internal debug endpoint.
type applyStatus struct {
	OK      bool      `json:"ok"`
	At      time.Time `json:"at"`
	Error   string    `json:"error,omitempty"`
	Version int64     `json:"version"`
}

func (g *trafficGates) status() applyStatus {
	g.l.RLock()
	defer g.l.RUnlock()
	return applyStatus{OK: g.lastApplyOK, At: g.lastApplyAt, Error: g.lastApplyError, Version: g.lastApplyVersion}
}

func (g *trafficGates) record(ok bool, err error) {
	g.l.Lock()
	defer g.l.Unlock()
	g.lastApplyOK = ok
	g.lastApplyAt = time.Now()
	g.lastApplyVersion++
	if err != nil {
		g.lastApplyError = err.Error()
	} else {
		g.lastApplyError = ""
	}
}

func (g *trafficGates) lastSnaps() *config.TrafficSnapshots {
	return g.snaps.Load()
}

// Apply resolves the config's traffic_priority section and atomically swaps
// all gate snapshots. Invalid configs are rejected and the previous rules
// remain in force.
func (g *trafficGates) Apply(cfg *config.Config) (*config.TrafficSnapshots, error) {
	tp := cfg.Inputs[0].Server.TrafficPriority
	snaps, err := tp.Resolve(
		int64(cfg.Inputs[0].Server.Limits.MaxConnections),
		int64(cfg.Inputs[0].Server.Bulk.FlushMaxPending),
		int64(cfg.Output.Elasticsearch.MaxConnPerHost),
		cfg.Inputs[0].Server.Bulk.FlushInterval,
	)
	if err != nil {
		g.record(false, err)
		return g.snaps.Load(), err
	}
	if err := g.entry.Apply(&snaps.Entry); err != nil {
		g.record(false, err)
		return g.snaps.Load(), err
	}
	if err := g.bulk.Apply(&snaps.Bulk); err != nil {
		g.record(false, err)
		return g.snaps.Load(), err
	}
	if err := g.es.Apply(&snaps.ES); err != nil {
		g.record(false, err)
		return g.snaps.Load(), err
	}
	g.snaps.Store(snaps)
	g.record(true, nil)
	return snaps, nil
}

// configureBulker pushes bulk-specific scheduling settings to a running
// bulker. Capacity rules are shared through the bulk gate.
func (g *trafficGates) configureBulker(b *bulk.Bulker) {
	if snaps := g.snaps.Load(); snaps != nil && b != nil {
		b.ReconfigureTraffic(snaps.MaxPendingBackgroundDispatches, snaps.BulkBackgroundFlushDelay)
	}
}

// stages returns the gate per metrics stage label.
func (g *trafficGates) stages() map[string]*traffic.Gate {
	return map[string]*traffic.Gate{
		"entry":      g.entry,
		"bulk_flush": g.bulk,
		"es":         g.es,
	}
}

// TrafficStatus implements api.TrafficStatusProvider for the debug endpoint
// and metrics sampler.
func (f *Fleet) TrafficStatus() api.TrafficStatus {
	gates := f.traffic
	status := api.TrafficStatus{Stages: []api.TrafficStageStatus{}}
	if gates == nil {
		return status
	}
	for _, stage := range []string{"entry", "bulk_flush", "es"} {
		gate := gates.stages()[stage]
		snap := gate.CurrentSnapshot()
		st := gate.Stats()
		status.Enabled = status.Enabled || st.Enabled
		status.Stages = append(status.Stages, api.TrafficStageStatus{
			Stage:    stage,
			Enabled:  st.Enabled,
			Snapshot: snap,
			Stats:    st,
		})
	}
	if b := f.currentBulker(); b != nil {
		status.Bulk = b.TrafficStats()
	}
	s := gates.status()
	status.Reload = api.TrafficReloadStatus{OK: s.OK, At: s.At, Error: s.Error, Version: s.Version}
	return status
}

// trafficConfigChanged reports whether the raw traffic_priority sections
// differ between two configs.
func trafficConfigChanged(curCfg, newCfg *config.Config) bool {
	if curCfg == nil {
		return true
	}
	return !reflect.DeepEqual(
		curCfg.Inputs[0].Server.TrafficPriority,
		newCfg.Inputs[0].Server.TrafficPriority,
	)
}

// reloadAction decides what a configuration reload must do after the traffic
// gates have been attempted: restart subsystems (restart=true), only
// reconfigure the live bulker/gates (reconfigure=true), or do nothing.
//
// applyErr non-nil means the incoming traffic_priority was invalid; the
// caller has normalized it back to the previous value, so it must neither
// restart nor reconfigure.
func reloadAction(curCfg, newCfg *config.Config, applyErr error) (restart bool, reason string, reconfigure bool) {
	changed, why := serverChangedIgnoringTraffic(curCfg, newCfg)
	if changed {
		return true, why, false
	}
	if curCfg != nil && applyErr == nil && trafficConfigChanged(curCfg, newCfg) {
		return false, "", true
	}
	return false, "", false
}

// serverChangedIgnoringTraffic mirrors Fleet.configChangedServer but treats
// traffic_priority-only differences as unchanged so they do not trigger a
// full subsystem restart.
func serverChangedIgnoringTraffic(curCfg, newCfg *config.Config) (bool, string) {
	if curCfg == nil {
		return true, "initial server configuration"
	}
	switch {
	case !reflect.DeepEqual(curCfg.Fleet.CopyNoLogging(), newCfg.Fleet.CopyNoLogging()):
		return true, "fleet configuration has changed"
	case !reflect.DeepEqual(curCfg.Output, newCfg.Output):
		return true, "output configuration has changed"
	case !reflect.DeepEqual(serverWithoutTraffic(curCfg), serverWithoutTraffic(newCfg)):
		return true, "server configuration has changed"
	default:
		return false, ""
	}
}

// serverWithoutTraffic returns a shallow server copy with the
// traffic_priority section blanked for DeepEqual purposes. Only value
// comparisons are performed on the returned copy; nested pointers are not
// mutated.
func serverWithoutTraffic(cfg *config.Config) config.Server {
	s := cfg.Inputs[0].Server
	s.TrafficPriority = config.TrafficPriorityConfig{}
	return s
}
