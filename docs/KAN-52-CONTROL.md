# KAN-52 — HomeCloud room playback control, protocol v1

## Scope and security boundary

HomeCloudBackend is the authoritative room owner. Direct media bytes remain on Gateway↔TV `/ws/stream` only. UI and remote issue JSON commands over their existing identified Backend control WS. No new public API or cloud services are introduced.

All messages are WS v1 JSON with fields `v:1,id,type:"command",action,data`. Room selection uses the **canonical room_id**, not an untrusted target_device_id supplied by a control request. Playback identity (playback_id, generation) is checked against current room state; delivery to Gateway/TV includes the current `connection_epoch` and increasing `command_sequence`.

**Warning:** The present WS `hello` uses device IDs as routing labels, not cryptographic authentication. Production deployment requires trusted local/Tailscale peers and authentication hardening separately. Never expose the control WS to an untrusted network.

## Client command

```
{"v":1,"id":"request-ui-123","type":"command","action":"media.room.control","data":{
 "room_id":"room-...", "playback_id":"play-...", "generation":1,
 "command_id":"ui-123", "action":"pause"
}}
```

`action` = `pause` | `resume` | `play` (resume alias) | `volume` | `mute`. `volume` requires `"volume":0..100`; `mute` requires `"muted":true|false`. No other action-specific fields are allowed. Every `command_id` must be unique per operation; identical retries receive the cached result, conflicting retries are rejected.

A successful result has `status:"DELIVERED"` (or `EXECUTED` when TV reports an already applied local volume/mute change). `DELIVERED` explicitly **does not** mean execution.

## Delivery event to the correct devices

```
{"v":1,"type":"event","topic":"media.room.control","data":{
 "room_id":"room-...", "playback_id":"play-...", "generation":1,
 "connection_epoch":1, "command_sequence":2, "command_id":"ui-123",
 "action":"pause", "origin":"ui"
}}
```

For pause/resume/play: the exact room TV and Gateway each receive this event. For volume/mute: only the exact room TV receives it. The Gateway **never** receives volume/mute. A TV-originated action is not echoed back to that same TV; pause/resume is still sent to the Gateway.

## Execution acknowledgement

```
{"v":1,"id":"ack-1","type":"command","action":"media.room.control.ack","data":{
 "room_id":"room-...","playback_id":"play-...","generation":1,
 "connection_epoch":1,"command_sequence":2,"command_id":"ui-123",
 "participant":"tv","phase":"APPLIED"
}}
```

`phase` = `APPLIED` or `FAILED`; failure may include an uppercase error_code (e.g., `PLAYER_NOT_READY`, `NO_ACTIVE_STREAM`). Only the target TV can acknowledge the TV operation; only the registered Gateway session can acknowledge the Gateway operation. The authority assigns all identities/versions; a stale command or ACK cannot affect a replacement playback. Per-room control timeout: 12s. A newer issued control supersedes any still-pending prior operation, with old ACKs fenced.

Snapshot fields (`media.rooms.snapshot`, also on `media.room.changed`) include `control_sequence`, `control_command_id`, `control_action`, `control_status`, `control_origin`, `control_error_code`, optional `volume` and `muted`. These fields are updated at dispatch stages and after execution; they share the existing `instance_id`/`sequence` ordering used by KAN-51. `volume`/`muted` remain unknown until TV execution is confirmed.

`PAUSED` is committed on actual TV + Gateway acknowledgements. A successful `resume` produces `BUFFERING`, never a false `PLAYING`; the existing independent execution reports of KAN-51 still own the transition to PLAYING. No command closes or changes the room_id, and no command selects another physical file.

## TV remote and looping

When the physical TV applies a local remote command (e.g., volume or pause), it sends **the same** `media.room.control` action, but the Backend derives `origin:tv` from the identified connection. The Backend treats the TV-side operation as already applied, forwards only Gateway-relevant operations, and emits one authoritative change to subscribed panels. The TV must not reapply room-changed broadcasts as remote input; subscribe-before-snapshot and the KAN-51 monotonic fence apply.

## TV Player integration limit

`HomeCloud-TV-Player` now has a control adapter listening for the room/control events and responding through its existing Rust WS task. It works with a live `video[data-homecloud-media]` HTML media element when one exists. As of this task, the TV app still has no actual HCM1 decoder/Media3 pipeline and currently displays the waiting screen; `PLAYER_NOT_READY` is the correct outcome for commands that require a real player. This is NOT hardware volume control: an HTMLMediaElement only controls its media stream volume. Native Android TV AudioManager integration and physical-device testing remain prerequisites before claiming full KAN-52 acceptance.
