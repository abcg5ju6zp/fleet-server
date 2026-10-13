# Review — Traffic Priority & Overload Protection

- **Reviewer**: independent fresh-context subagent (read-only), codebase evidence with file:line; reviewer ran go vet/go build/-race tests/bench against git HEAD.
- **Date**: 2026-02-13
- **Artifacts reviewed**: `spec.md`, `tasks.md`, all new/modified files (traffic, config, bulk, es, api, server, checkin, monitor, reference.yml, docs, changelog fragment).
- **Initial result**: Request changes — 1 blocker, 2 majors, several minors.
- **Final result after remediation**: **PASS** (all blocker/major fixed with regression tests; minors fixed or explicitly accepted).
- **Second review (focused re-review)**: PASS / mergeable; two new minors found and fixed the same day —
  (1) forced background acquire now also retains on `context.DeadlineExceeded`
  from the acquire window so a large configured bulk wait timeout cannot kill
  Run; (2) threshold-triggered flush keeps the retained-queue retry timer
  (`doFlush` returns `retained`; `stopTimer` skipped when a queue is retained).
  Regression tests stable across 3× `-race` runs.

## Checkpoints

| # | Checkpoint | Initial | Final |
| --- | --- | --- | --- |
| CP-R1 | 并发正确性（Gate 准入/释放/热替换/observer；bulk Run 不退出） | FAIL | PASS |
| CP-R2 | 关闭模式等价性 | PASS | PASS |
| CP-R3 | 热更新原子性/坏配置保持旧值/无重启 | FAIL (data race) | PASS |
| CP-R4 | 类别传播贯穿长轮询、批处理、重试 | FAIL (toggle stranding) | PASS |
| CP-R5 | 指标平衡、注册幂等、队列采样 | CONCERN (missing series) | PASS |
| CP-R6 | 错误映射 429 不误伤 control | PASS | PASS (polish applied) |
| CP-R7 | 容量语义与连接槽/路由名额不冲突 | PASS | PASS |
| CP-R8 | 测试质量 | CONCERN (missing regression) | PASS (2 new deterministic tests) |

## Findings and remediation

### BLOCKER-1 — forced background flush killed the bulk Run loop
- Finding: `tryFlushBackgroundQueue(force=true)` propagated the bulk gate's
  `traffic.ErrCapacityTimeout` (default 1s) through `doFlush`, terminating
  Run; ≥8 long-poll msearch flushes holding the permit pool could trigger it
  in normal operation — the feature killed the process in the exact scenario
  it protects.
- Fix: `internal/pkg/bulk/engine.go` — gate-side timeout/rejection in the
  background path now retains the queue (`return false, nil`) and retries on
  the next cycle; only ctx/non-traffic errors exit Run.
- Evidence: `TestForceFlushGateTimeoutKeepsRunAlive` (400ms of repeated force
  deadlines while one long flush holds the only permit, then control preempts
  and deferred background drains; `msearch→bulk→msearch` order asserted, all
  callers return).

### MAJOR-2 — data race on `Fleet.bulker`
- Finding: pointer written by runServer goroutine, read by reload/status/
  sampler goroutines without synchronization.
- Fix: `bulkerPtr atomic.Pointer[bulk.Bulker]` + set/current helpers; all
  reads updated (`internal/pkg/server/fleet.go`, `traffic.go`).

### MAJOR-3 — enable→disable stranded deferred queues
- Finding: after disabling, pass1 required `controlCnt>0` while pass2 was
  skipped, so a deferred background-only queue could sit until a new block
  arrived.
- Fix: pass1 flushes every pending queue when the gate is disabled
  (`!enabled || controlCnt > 0`), historical order preserved.
- Evidence: `TestDisableFlushesDeferredBackgroundQueue` (retained refresh-bulk
  queue drains on Apply(disabled) as `bulk` call after the initial msearch).

### Minors fixed
- Added `http_server_traffic_priority_bulk_dispatch_rejected_total`
  (callback `SetBackgroundDispatchRejectedCallback` wired from server) and
  `http_server_traffic_priority_bulk_queue_bytes{queue}`.
- Remote-output bulkers no longer share the main flush gate; they keep the
  legacy flush semaphore and only share the ES transport gate (matches spec
  Non-goals).
- `Gate.Apply(nil)` safely disables instead of panicking
  (`TestApplyNilDisables`).
- Entry middleware: `errors.Is`; a canceled waiter no longer writes a 429;
  test updated.
- reference.yml notes that enabling requires positive
  max_connections/entry capacity (invalid value is rejected by Resolve with
  a clear error).
- NOTICE verified regenerated with the mage-equivalent command: unchanged
  (the new go.mod indirect `kylelemons/godebug` is testutil-only).

### Accepted / by design
- multi-ops bypass the background dispatch cap via `multiDispatch`; the
  multi API has zero in-repo callers today (documented TODO) and entries are
  class-routed with full gate/flush protection.
- /debug route isolation on agent listeners is structural (route only added
  to the metrics mux in `InitMetrics`); integration test asserts presence on
  the metrics listener.
- Gate observer callbacks fire under gate lock; adapters use atomic metric
  ops only and must not re-enter the gate (documented contract).
- JSON snapshot emits durations as Go nanosecond integers; accepted for a
  debug endpoint alongside the machine-readable metrics.

## Final verification

- `go build ./...` clean; `gofmt -l internal/ cmd/` empty; `go vet
  ./internal/...` has only one pre-existing warning in an untouched test.
- `go test -race` green: traffic, bulk, api, server, es, config, checkin,
  monitor (all affected packages).
- Full non-integration `go test ./internal/... ./cmd/...` green;
  `go vet -tags integration` compiles.
- Benchmarks: disabled admit 4.24 ns/op, 0 allocs; uncontended 106 ns/op, 4
  allocs (waiter/list node/ticket/channel).
- Local golangci-lint binary is incompatible with Go 1.27 (export-data
  version); CI pinned linter remains authoritative.
