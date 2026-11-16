# KAN-51 — authoritative HomeCloud room status and all-panel sync (v1)

**Scope:** HomeCloudBackend + HomeCloud Media Gateway, control protocol. The actual Android TV decoder and HomeCloud UI implementations are separate clients; this ticket defines their verified control contract. Main Backend never relays media bytes.

## Roles / truth

- `media.room.offer`: command was **delivered**, not accepted by receiver or executed.
- `media.room.participant` `APPLIED`: TV/Gateway processed the control offer, **not** playing.
- `media.room.attach` `READY`: direct TV/Gateway WebSocket attached, **not** playing.
- `media.room.report` `gateway/STREAMING`: a real non-empty HCM1 binary media frame was **written** to direct WS. This does not assert TV received/decoded it.
- `media.room.report` `tv/PLAYING`: actual TV decoder is playing media. **Only when both reports exist** and the room is ACTIVE, TV/Gateway both APPLIED and attachment READY can Backend show `playback_state=PLAYING`.
- `PAUSED`, `BUFFERING` / seek position, `ENDED`, `IDLE`, `ERROR`: actual TV playback status; source/transport errors can also come from Gateway.
- EOF on Gateway (`gateway/ENDED`) is **not** TV playback EOF. The TV may still be decoding queued data; room remains ACTIVE. TV eventually reports `ENDED`, then `IDLE`.

## UI bootstrap / reconnect

1. Open the identified main Backend control WebSocket.
2. Subscribe separately to `media.room.changed`, `media.room.closed`, `media.room.lost`, `media.room.error`, and optionally `media.command.status` via `type=subscribe`/`topic=...` and await `subscribed` for each.
3. Request **snapshot after subscriptions**:

```json
{"v":1,"id":"rooms-snapshot-1","type":"command","action":"media.rooms.snapshot","data":{}}
```

The response is `type=result`, `action=media.rooms.snapshot`:

```json
{"schema_version":1,"instance_id":"backend-...","sequence":42,"rooms":[{"room_id":"room-...","playback_id":"play-...","generation":1,"revision":6,"sequence":42,"instance_id":"backend-...","room_state":"ACTIVE","playback_state":"PAUSED","position_ms":73300,"tv_execution":"PAUSED","gateway_execution":"STREAMING"}]}
```

**Critical:** The sample above is an abbreviated `data` value; actual room snapshots also contain target node/device, Gateway, media source, participant ACKs, attach phase, and error code. The event bus **wraps** authoritative payloads as `data.payload`:

```json
{"v":1,"type":"event","topic":"media.room.changed","data":{"from":"backend","payload":{"instance_id":"backend-...","room_id":"room-...","revision":7,"sequence":43,"playback_state":"PLAYING"}}}
```

4. Replace entire local room map with `snapshot.rooms`, NOT merge (rooms absent from snapshot are closed).
5. Buffer events that arrive after subscribing but before the snapshot result. Apply only events whose `payload.instance_id` equals snapshot's `instance_id` and `payload.sequence > snapshot.sequence`. Then use strictly increasing `sequence` for all events. `media.room.closed` carries a full tombstone `Snapshot` with `room_state=CLOSED`, `close_reason`, higher `sequence`; delete its room after applying the event. On a different `instance_id` (Backend restart), discard cached state and take a new snapshot.
6. If the UI detects sequence gaps or loses its control WS, subscribe then re-snapshot. Never infer PLAYING from command-status messages.

`media.rooms.list` and `media.rooms.get` are retained for legacy consumers. New UI panels should prefer `media.rooms.snapshot`.

## Actual playback reports

Only identified **TV** or registered **Gateway** control WS may call `media.room.report`. Calls by panels or for wrong device, playback, generation, attachment epoch, or older sequence are rejected.

```json
{"v":1,"id":"tv-progress-21","type":"command","action":"media.room.report","data":{"room_id":"room-...","playback_id":"play-...","generation":1,"connection_epoch":1,"participant":"tv","report_sequence":21,"state":"PAUSED","position_ms":95500}}
```

TV state values: `PLAYING`, `PAUSED`, `BUFFERING`, `ENDED`, `IDLE`, `ERROR`. Gateway values: `STREAMING`, `BUFFERING`, `ENDED`, `IDLE`, `ERROR`. `report_sequence` is a monotonically increasing positive integer per participant, control session and attachment epoch; identical retries are harmless. TV must report `BUFFERING` and its actual target position when it begins seek; report `PLAYING` only **after** actual playback resumes. The client controls the real playback action; this API only records verified results. Only TV may set non-zero `position_ms`.

Errors: use `state:ERROR`, `error_code` uppercase digits/underscore 1..64 chars, e.g. `SOURCE_UNAVAILABLE`, without file paths/secrets.

Gateway sends these reports automatically after its **first successfully written binary HCM1 data frame** and after actual HCM1 EOF, but only when a trusted `StreamOpener` is installed. Handshake-only (no source) does not assert streaming.

## State and ordering

- Per-room `revision` increases for every authoritative state transition.
- Backend-wide `sequence` increases for every room mutation (including STOP, LOST, CLOSED) and is **not** reset on a new playback within the same Backend instance.
- `instance_id` changes on Backend restart. The initial snapshot and all authoritative changed/closed events carry it.
- New `playback_id`, generation or direct WebSocket epoch invalidates earlier execution reports. Old reports cannot put a new source back into PLAYING.
- Status `EXECUTED` of the original play `command_id` is emitted only after both sides' independent proof of playback. `ACCEPTED`, `DELIVERED`, and `APPLIED` are not equivalent to executed.
- A TV or Gateway can report `ERROR`. Backend retains the room with playback state ERROR for display/diagnostics; room loss/timeouts or explicit STOP use existing KAN-50 cleanup.
- Heartbeats run independently of playback and continue after EOF/IDLE.

## Tests / limitations

- Go manager tests: dual confirmation, pause, seek position, EOF/IDLE, stale session/epoch/generation/sequence, duplicate idempotence, reconnect invalidates PLAYING, snapshot tombstone ordering.
- Backend in-process integration: 2 independent UI subscribers receive same state and reconnect snapshot; stale report cannot overwrite.
- Gateway WS integration: STREAMING/ENDED reports correspond to actual binary frame writes.
- The **currently installed Android TV Player has no decoder/media pipeline**, so it cannot truthfully emit decoder `PLAYING`, `PAUSED`, or seek reports yet. Its integration is still needed before claiming a physical-TV end-to-end demo. Do **not** simulate real playback by replying PLAYING on receiving offer/ready.
