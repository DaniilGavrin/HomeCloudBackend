# Telemetry Core and Metrics History

The Backend owns telemetry collection and history. This package does not depend on
Netdata, Prometheus, or a separate monitoring daemon.

## Core contract

- Collectors implement `telemetry.Collector` and are registered before Core.Start.
- Collectors run independently on their own interval and receive a bounded context.
- A sample has a stable metric name, node ID, optional bounded labels, unit, value,
  and UTC timestamp.
- The latest cache is in memory; historical samples are persisted asynchronously.
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
`stats`, and `server_time`.

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

## Current scope

This phase provides the collector contract, scheduler, latest-value cache,
asynchronous SQLite history, retention/rollup worker, and read commands. Hardware
collectors and live event streaming are subsequent phases.
