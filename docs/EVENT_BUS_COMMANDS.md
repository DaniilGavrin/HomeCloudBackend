# HomeCloudBackend — Event Bus (WebSocket JSON v1)

**Works on one persistent WS connection after `hello`.** No JWT/token is introduced. For Postman, connect separately for each simulated device, but keep each connection open.

This patch adds a generic in-memory Event Bus with **exact topic** subscriptions. Topics require dotted lowercase names (e.g. `media.playback.changed`). The subscriber is the current WS session. Subscriptions are removed on disconnect; after reconnect the client must subscribe again.

> `queued` means the event entered the recipient's bounded outbound queue, **not** that the recipient has read, understood, or executed it. No durable history/replay or wildcard subscriptions in this version.

## 1. Subscribe (existing message type, now supports arbitrary topics)

```json
{
  "v": 1,
  "id": "sub-001",
  "type": "subscribe",
  "topic": "media.playback.changed"
}
```

Response:

```json
{
  "v": 1,
  "id": "sub-001",
  "type": "subscribed",
  "data": { "topic": "media.playback.changed" }
}
```

## 2. Unsubscribe (NEW message type)

```json
{
  "v": 1,
  "id": "unsub-001",
  "type": "unsubscribe",
  "topic": "media.playback.changed"
}
```

Response type: `unsubscribed`. Unsubscribe is idempotent.

## 3. Publish an event (NEW command: `events.publish`)

```json
{
  "v": 1,
  "id": "publish-001",
  "type": "command",
  "action": "events.publish",
  "data": {
    "topic": "media.playback.changed",
    "payload": {
      "position_ms": 2500,
      "state": "playing"
    }
  }
}
```

Response example when two devices are subscribed and their queues have room:

```json
{
  "v": 1,
  "id": "publish-001",
  "type": "result",
  "action": "events.publish",
  "data": {
    "topic": "media.playback.changed",
    "subscribers": 2,
    "queued": 2,
    "dropped": 0
  }
}
```

Each subscriber receives:

```json
{
  "v": 1,
  "type": "event",
  "topic": "media.playback.changed",
  "data": {
    "from": "desktop-main",
    "payload": {
      "position_ms": 2500,
      "state": "playing"
    }
  }
}
```

`from` is assigned by the server from the sender's `hello.device_id`, **not** accepted from a client-provided field. `payload` can be any valid JSON value. `events.publish` forbids client publication to the reserved `system.*`, `backend.*`, and `clients.*` namespaces.

## 4. List own subscriptions (NEW command: `events.subscriptions`)

```json
{
  "v": 1,
  "id": "topics-001",
  "type": "command",
  "action": "events.subscriptions"
}
```

Response:

```json
{
  "v": 1,
  "id": "topics-001",
  "type": "result",
  "action": "events.subscriptions",
  "data": {
    "topics": ["media.playback.changed", "system.health"]
  }
}
```

## 5. Subscribe to backend device events

```json
{
  "v": 1,
  "id": "sub-connected",
  "type": "subscribe",
  "topic": "clients.connected"
}
```

And in the same connection, subscribe to disconnects:

```json
{
  "v": 1,
  "id": "sub-disconnected",
  "type": "subscribe",
  "topic": "clients.disconnected"
}
```

When another client connects (or disconnects), the subscriber receives an event with a `payload` containing `device_id` and `client_type`, and `from: "backend"`.

## 6. Compatibility with existing commands

- `system.health` still requires `subscribe` to `system.health` if YAML `message_bus.require_subscription` is `true`. It remains a **request command**, not an automatic periodic health event.
- `clients.send`, `clients.list`, `system.status`, `ping`, `hello` and `disconnect` keep their existing JSON contract.
- `clients.send` is **addressed delivery to one device**, while `events.publish` is **fanout to all subscribers**.
- `websocket.max_message_bytes` and `websocket.max_outbound_queue_messages` still apply. The broker caps each session at 128 exact topic subscriptions.
- `message_bus.slow_consumer_policy: disconnect` causes overflowing event recipient queues to close the recipient connection. With `drop_noncritical`, the broker counts dropped events instead.

## 7. Test on Windows (Go 1.27.3)

```powershell
go test ./...
go test -race ./internal/events ./internal/commands/... ./internal/connections ./internal/wsserver
go run . serve
```

In Postman: connect **TV**, send hello and subscribe to `media.playback.changed`; connect **PC**, send hello, then `events.publish`. TV receives the event. Add a second subscriber, and both receive it. `unsubscribe` stops future delivery to only that subscriber.
