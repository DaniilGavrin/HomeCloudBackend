# KAN-54 — EOF playback vs STOP viewing

## Authoritative lifecycle

- `media.room.report` from **Gateway** with `state: ENDED` means last HCM1 EOF frame has been written, **not** that playback finished on the TV. Room state does not change to CLOSED.
- `media.room.report` from authenticated **TV** with `state: ENDED`, same `room_id`, `playback_id`, `generation`, `connection_epoch`, and strictly increasing `report_sequence`, confirms decoder EOF. Backend publishes `media.room.changed` and exactly one `media.playback.ended` (when transitioning into ENDED). Payload includes `room_id`, `playback_id`, `generation`, `position_ms`, `revision`, `sequence`, and `instance_id`.
- A later `TV IDLE` is allowed; both `ENDED` and `IDLE` leave room `ACTIVE`, control WS, independent TV/Gateway heartbeats and (where connected) direct WS alive. The TV shows a fullscreen waiting state until a new file is chosen.
- New media on the same active room has a new `playback_id`; old observations are rejected by playback/generation fences.

## STOP

The UI or the **target TV** sends:

```json
{"v":1,"id":"request-1","type":"command","action":"media.rooms.stop","data":{"room_id":"room-...","command_id":"stop-uuid"}}
```

Backend rejects Gateway-origin STOP and wrong-TV STOP. The first STOP removes the room from the active registry, deletes heartbeat/control state, sends `media.room.closed` with `reason=STOP` to TV and Gateway, then publishes authoritative `media.room.closed` to all subscribed UIs. Peer socket writes are attempted with short bounded timeouts; physical peer cleanup cannot be guaranteed when a device is offline.

Subsequent STOP retries with the same *or a different* `command_id` return the existing CLOSED result without emitting a second terminal event. A bounded 256-room tombstone cache provides retries; after eviction the request returns `ROOM_NOT_FOUND` but never resurrects a room. Next PLAY creates a **different random room_id**.

Gateway receives `media.room.closed`, revokes the direct WS ticket (closing the live socket), removes sender-control routing and calls idempotent local room STOP (canceling worker/room contexts). TV dismisses its fullscreen state **only** on authoritative close/snapshot; never on source EOF or on mere request dispatch. After Backend restart, subscribe before taking `media.rooms.snapshot` and reconcile the empty snapshot as terminal.

## Validation boundary

This task adds contract tests and TV fullscreen *placeholder*, not a real decoder. The physical Android TV EOF and STOP visual behavior needs acceptance testing after source readers/codec pipeline/TV playback are integrated. A successful CI build is not equivalent to this end-to-end acceptance.
