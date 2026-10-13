# Traffic priority and overload protection

Fleet Server can reserve capacity for **control-plane** traffic so that
bulk **background** traffic (diagnostic collection, file distribution)
cannot delay policy and action delivery during overload.

The feature is **off by default**. When off, behavior is identical to
previous versions: no additional admission control, unchanged request
ordering, responses and metrics.

## Classes

| Class | Endpoints |
| --- | --- |
| control | agent check-in, action acknowledgements, OpAMP (`/v1/opamp`), enroll, status, PGP key retrieval, audit-unenroll |
| background | diagnostic uploads (`/api/fleet/uploads`, chunks), file download (`/api/fleet/file`), artifact fetch (`/api/fleet/artifacts`) |

Anything else (unknown paths) is not managed by the gate and keeps its
existing behavior.

## How capacity is shared

Every protected stage (HTTP entry, the bulk engine flush pool, the
Elasticsearch transport) has total capacity `C` with a control reservation
`R` and a background floor `S = C - R`.

- Control requests always take a free reservation first.
- Background requests take floor permits first and may **borrow** idle
  reserved capacity.
- New background borrowing stops the moment a control request is waiting;
  when permits free up, waiting control requests are served before queued
  background requests.
- A background request that cannot get in within `*_wait_timeout` receives
  HTTP `429` (`TrafficLimit`, same envelope as existing limit errors).
- Admitted work is never revoked or reclassified: requests already running
  complete under the rules and class they were admitted with.
- Cancellation is honored at every stage: a client that disconnects while
  queued releases its waiter immediately.

Capacity is enforced at three layers, all carrying the class through
batching and retries:

1. **HTTP entry gate** — outermost middleware, before the connection
   throttle and per-route limiters. A waiting background request therefore
   does not hold a connection slot or a route permit. Check-in long polls
   and OpAMP connections hold an entry permit for their full lifetime, so
   `entry_max_in_flight` must be sized for the number of concurrently
   connected agents (default: derived from `limits.max_connections`).
2. **Bulk engine gate** — control-bearing flush queues are scheduled before
   background-only queues; a background-only queue that cannot get a flush
   permit is retried without blocking the run loop and is force-flushed
   within `bulk_background_flush_delay` so background cannot be starved
   indefinitely.
3. **Elasticsearch transport gate** — one permit per HTTP attempt on the
   shared ES clients (including the no-retry monitor client used for chunk
   uploads/downloads and index monitors). Elasticsearch retries re-acquire
   with the same class and do not hold permits during backoff.

## Configuration

See `fleet-server.reference.yml` for the full `server.traffic_priority`
section. Key fields:

- `enabled` (default `false`)
- `control_reserved_ratio` (default `0.5`) — used for each stage whose
  absolute reservation is left at `0`
- `entry_max_in_flight` / `entry_control_reserved` /
  `background_wait_timeout` / `max_background_waiters`
- `bulk_control_reserved` / `bulk_background_wait_timeout` /
  `bulk_background_flush_delay` /
  `max_pending_background_dispatches`
- `es_max_in_flight` / `es_control_reserved` /
  `es_background_wait_timeout`

Invalid values are rejected at load time, with a clear validation error.

## Hot reload

`traffic_priority` changes apply **without restarting listeners or the bulk
engine**: gate snapshots are swapped atomically and only new admissions use
the new rules, while in-flight requests finish under their acquired rules.
An invalid reload is rejected and logged; the last valid rules stay in
force and the server keeps running.

## Observability

- Metrics (Prometheus on `/metrics`, mirrored scalars under
  `http_server.traffic_priority` in `/stats`):
  - `http_server_traffic_priority_inflight{stage,class}`
  - `http_server_traffic_priority_waiting{stage,class}`
  - `http_server_traffic_priority_borrowed{stage,class}`
  - `http_server_traffic_priority_admitted_total{stage,class,mode}`
    (`reserved`/`floor`/`borrowed`)
  - `http_server_traffic_priority_rejected_total{stage,class,reason}`
    (`timeout`/`waiters`/`canceled`)
  - `http_server_traffic_priority_wait_seconds{stage,class}`
  - `http_server_traffic_priority_bulk_queue_depth{queue,class}`
  - `http_server_traffic_priority_config_last_reload_successful` and
    `..._timestamp_seconds`
- Point-in-time JSON snapshot: `GET /debug/traffic-priority` on the
  **internal metrics listener only** (localhost, same listener as
  `/metrics` and `/stats`). It is never exposed on the agent-facing
  listeners.
