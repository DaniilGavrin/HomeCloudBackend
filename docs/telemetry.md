# Telemetry Core and Metrics History

The Backend owns telemetry collection and history. This package does not depend on
Netdata, Prometheus, or a separate monitoring daemon.

## Core contract

- Collectors implement `telemetry.Collector` and are registered before Core.Start.
- Collectors run independently on their own interval and receive a bounded context.
- A sample has a stable metric name, node ID, optional bounded labels, unit, value,
  and UTC timestamp.
- The latest cache is in memory; stale series expire after 10 minutes, while historical samples are persisted asynchronously.
- A full history queue is reported as `telemetry.ErrQueueFull`; collection and
  WebSocket request goroutines never wait for SQLite writes.

## SQLite history

The `hc_metric_samples` table stores three resolutions:

| Resolution | Retention | Purpose |
| --- | --- | --- |
| 1 second | 1 hour | Recent detail |
| 10 seconds | 24 hours | Intraday charts |
| 1 minute | `monitoring.metrics_retention_days` (default 14 days, configurable 1–14) | Long-range charts |

A background writer batches inserts in transactions. Rollups preserve average,
minimum, maximum, and sample count. The existing SQLite connection limit remains
one; telemetry uses the same database through the bounded writer.

Schema changes are explicit. On an existing installation with
`auto_migrate: false`, run the database initialization command once before
starting this Backend version:

```sh
go run . init-db
```

Back up `data/homecloud.db` before applying schema migrations.

## WebSocket commands

Clients must subscribe to `system.metrics` when
`message_bus.require_subscription: true`.

### Latest values

```json
{
  "v": 1,
  "id": "metrics-latest-1",
  "type": "command",
  "action": "system.metrics.latest",
  "data": {
    "node_id": "node-id",
    "metric": "cpu.usage_percent"
  }
}
```

Both filters are optional. The response contains `samples`, internal telemetry
`stats`, and `server_time`. Stats include collection errors, queue drops, SQLite
write failures, rollup failures, and retention cleanup failures.

### Historical series

```json
{
  "v": 1,
  "id": "metrics-query-1",
  "type": "command",
  "action": "system.metrics.query",
  "data": {
    "node_id": "node-id",
    "metric": "cpu.usage_percent",
    "from": "2026-11-17T10:00:00Z",
    "to": "2026-11-17T11:00:00Z",
    "resolution_ms": 10000,
    "limit": 5000
  }
}
```

Supported resolutions are 1000, 10000, and 60000 milliseconds. If omitted,
the history layer selects a resolution based on the requested time range.
Queries are limited to 14 days and 20,000 points.

## Process inventory command

`system.processes` returns an on-demand process table, sorted by resident memory
by default. The request accepts `limit` (1–200, default 50) and `sort_by`
(`memory`, `pid`, or `name`). It includes PID, process name, state, RSS, memory
percentage, thread count, and start time. Command lines, environment variables,
and file descriptors are never returned.

```json
{
  "v": 1,
  "id": "process-list-1",
  "type": "command",
  "action": "system.processes",
  "data": { "limit": 50, "sort_by": "memory" }
}
```

The command has a five-second collection deadline and a bounded result size.

## Built-in host collectors

The Backend registers nine collectors when `monitoring.enabled: true`:

| Collector | Cadence | Metrics |
| --- | --- | --- |
| CPU | `monitoring.sample_interval_seconds` | Total and per-core utilization; Linux 1/5/15-minute load average |
| Memory | `monitoring.sample_interval_seconds` | RAM total/used/available/free, cache/buffers when available, swap and percentages |
| Network | `monitoring.sample_interval_seconds` | Per-interface RX/TX bytes, packets, errors and drops per second |
| Disk | 5 seconds minimum | Per-device read/write throughput, IOPS, average latency and utilization; filesystem capacity and inode usage (scan throttled to 30 seconds) |
| Processes | 5 seconds minimum | Total process count |
| Hardware sensors | 5 seconds minimum | Temperature, high and critical thresholds where Linux exposes sensors |
| Backend | 5 seconds minimum | Goroutines, Go heap, allocations, GC cycles, process RSS/VMS and threads |
| Host | 30 seconds minimum | Uptime and boot timestamp |
| Services | 5 seconds | Backend, connected Media Gateway and registered Storage Service nodes availability |

The generated configuration defaults to a one-second sample interval. It can be
adjusted from 1 to 60 seconds. Disk IO is deliberately sampled no faster than every
five seconds. Filesystem capacity scans are throttled to 30 seconds to avoid repeatedly
walking mounted filesystems.

Metric names use dotted lowercase identifiers. Interface, device, and mountpoint
labels are stable dimensions; IP addresses, MAC addresses, process arguments, and
other sensitive/high-cardinality values are not included in these series. The process
collector stores only an aggregate process count; detailed process listings are
available on demand through `system.processes`. Service availability is represented as `1` (online)
or `0` (offline), with a `service` label. Disabled Media Gateway is omitted; Storage
Service metrics are emitted for registered storage nodes.

For live updates, subscribe to the exact `system.metrics` event topic. The Backend
publishes each collector batch through the existing session-scoped Event Bus. No
second WebSocket connection or wildcard subscription is introduced.
