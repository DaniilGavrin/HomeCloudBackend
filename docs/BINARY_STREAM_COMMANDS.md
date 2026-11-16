# HomeCloudBackend — Binary Stream Manager v1

**Purpose:** binary relay between two active HomeCloud WS clients. The server is
**not** a media decoder, PDF processor or persistent file store.

## Endpoints

- Control plane: `ws://127.0.0.1:8080/ws` (unchanged). Both devices perform
  normal `hello` once and keep the connection alive.
- Data plane: `ws://127.0.0.1:8080/ws/stream` (one **additional** WebSocket
  per participant per stream). If `server.websocket_path` changes, the stream
  endpoint is that path + `/stream`.
- Both endpoints use existing private LAN / Tailscale listeners and existing
  WebSocket Origin restrictions. There are **no application tokens**; device
  labels only identify the routing endpoints, not authentic identities.

## Example — PDF from PC to TV

### 1. PC sends `streams.create` to its control `/ws`

```json
{
  "v": 1,
  "id": "stream-001",
  "type": "command",
  "action": "streams.create",
  "data": {
    "target": "livingroom-tv",
    "name": "example.pdf",
    "mime_type": "application/pdf",
    "size_bytes": 26214400
  }
}
```

The response has `type=result`, `action=streams.create` and `data` like:

```json
{
  "stream_id": "9a9b494976fe93252bc108c88037b112",
  "from": "desktop-main",
  "target": "livingroom-tv",
  "name": "example.pdf",
  "mime_type": "application/pdf",
  "size_bytes": 26214400,
  "path": "/ws/stream",
  "status": "waiting",
  "created_at": "2026-10-25T12:00:00Z"
}
```

`stream_id` above is illustrative. Always use the **actual** ID from response.

### 2. TV receives a `streams.offer` event on its control `/ws`

```json
{
  "v": 1,
  "type": "event",
  "topic": "streams.offer",
  "data": {
    "stream_id": "9a9b494976fe93252bc108c88037b112",
    "from": "desktop-main",
    "target": "livingroom-tv",
    "name": "example.pdf",
    "mime_type": "application/pdf",
    "size_bytes": 26214400,
    "path": "/ws/stream",
    "status": "waiting",
    "created_at": "2026-10-25T12:00:00Z"
  }
}
```

The direct `streams.offer` does **not** require an Event Bus subscription.

### 3. TV opens `/ws/stream` and sends `stream.hello`

```json
{
  "v": 1,
  "type": "stream.hello",
  "stream_id": "9a9b494976fe93252bc108c88037b112",
  "device_id": "livingroom-tv",
  "role": "receiver"
}
```

### 4. PC opens a separate `/ws/stream` and sends

```json
{
  "v": 1,
  "type": "stream.hello",
  "stream_id": "9a9b494976fe93252bc108c88037b112",
  "device_id": "desktop-main",
  "role": "sender"
}
```

Each data connection receives `stream.attached` first. After **both** are
attached, both receive `stream.ready`. Start sending bytes **only after**
receiving `stream.ready` on the sender data socket.

### 5. PC sends chunks as real WS binary messages

Chunk a file into ~64 KiB binary messages; never encode into JSON/Base64.

```javascript
// Example for our future client application (not a new protocol).
const chunkSize = 64 * 1024;
for (let offset = 0; offset < file.size; offset += chunkSize) {
  const chunk = file.slice(offset, offset + chunkSize);
  // Real client: respect bufferedAmount / backpressure before continuing.
  streamSocket.send(chunk);
}
```

Each WebSocket binary **message** is forwarded to the TV as a corresponding
binary message. The server uses `Reader()` + `Writer()` + `io.CopyBuffer()`:
whole files are not loaded into memory. The TV application handles decoding,
writing, buffering, presentation or playback.

### 6. PC signals successful end on the **stream** socket

```json
{"v":1,"type":"stream.end"}
```

Both data sockets then receive `stream.complete` containing `stream_id`,
`bytes` and SHA-256; after that, the stream sockets close normally.
Clients should independently validate expected size and SHA-256.

Normal WebSocket disconnect **without** `stream.end` is treated as failed,
not as successful end-of-file.

## Other control commands

List only streams in which the current device participates:

```json
{"v":1,"id":"streams-list-001","type":"command","action":"streams.list"}
```

Cancel a pending or running stream (only source/target may cancel):

```json
{"v":1,"id":"streams-cancel-001","type":"command","action":"streams.cancel","data":{"stream_id":"ACTUAL_STREAM_ID"}}
```

Both participants receive a direct `streams.closed` event on control `/ws`
with the final status, including failures; no subscription is required.
For additional Event Bus observability, subscribe to `streams.finished` or
`streams.failed`. These `streams.*` topics are reserved for the backend, and
clients cannot forge them via `events.publish`.

## Test without Postman binary editor

Server must be running (`go run . serve`). From a **separate PowerShell**:

```powershell
go run ./scripts/stream-demo.go
# Or test a real local file:
go run ./scripts/stream-demo.go -file "C:\\Temp\\example.pdf"
```

The script connects two simulated devices to the running server, creates a
stream, sends binary blocks, copies the received blocks to a temporary file,
checks length and SHA-256, and deletes its temporary file. It does not print or
play media. Use `-server ws://LAN-IP:8080/ws` for a remote server.

## v1 bounds / deliberate limitations

- Up to **32** concurrent streams; at most **4** per client.
- A `streams.create` offer expires if both endpoints have not reached ready
  within **60 seconds**.
- A single binary WebSocket message is capped at **1 MiB**. Send as many
  messages as needed. Total per stream is capped at **8 GiB**.
- `size_bytes=0` means unknown size (up to hard cap). Nonzero advertised size
  must match actual transmitted bytes; otherwise the transfer fails.
- The existing `/ws` limit of 65,536 bytes for JSON remains unchanged.
- Transfers are transient/in-memory, one sender and one receiver, no resume,
  no persistent jobs, no transcoding, no file-system writing by the backend.
- The sender/receiver `device_id` must match an active control `/ws` client.
  As specified for this trusted-network architecture, this is identification,
  **not proof of identity**.
- For future browser clients, `WebSocket.send()` queues chunks and can grow
  `bufferedAmount`; implement client-side flow control and avoid unbounded
  browser-side queues.
- Flow control is provided by the receiver writer blocking during `io.Copy`;
  no unbounded relay buffers are allocated server-side.
- No arbitrary URL is downloaded and no path is opened by this relay. Future
  CUPS/storage/media integrations will be separate consumers.
- Hardware/network firewall and Tailscale access rules remain essential.

## Verification

```powershell
go test ./...
go test -race ./internal/streaming ./internal/commands/streams ./internal/wsserver
go run . serve
```
