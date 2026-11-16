# KAN-53 / MG-13: generation-fenced timeline seek (Backend)

## Status

Seek coordination is implemented as a *protocol and room-state foundation*.
Actual local/remote file ranging, keyframe indexing, FFmpeg seek and playback
on a physical Android TV remain dependent on MG-17/MG-19 and MG-22–MG-24.
Do not close the issue on the basis of compilation or synthetic tests alone.

## UI/TV → Backend: `media.rooms.seek`

Already-identified main control WebSocket; `type=command`, `action=media.rooms.seek`:

```json
{"v":1,"id":"req-53","type":"command","action":"media.rooms.seek","data":{
 "command_id":"seek-53-1","room_id":"room-...","playback_id":"play-...",
 "generation":1,"position_ms":120000}}
```

- `command_id` is idempotent; a reused ID with different parameters conflicts.
- `position_ms` is a requested **timeline timestamp**, NOT a byte offset.
- A room must be ACTIVE, directly attached, and both peers online. The TV
  itself may request seek only for its matching `device_id`; Gateway cannot.
- `room_id` and `playback_id` stay unchanged; generation increments, old
  execution reports and previous attach/commands are invalidated.
- The target is `seek_target_ms`. `position_ms` remains the *last observed TV
  decoder position* until a new authenticated `media.room.report` arrives.
- On delivery failure after mutation the room closes, avoiding split-brain
  ownership of incompatible stream generations.
- Max requested timeline 7 days. Target 0 is valid.

## Backend → Gateway + TV: fresh `media.room.offer`

An existing offer event type is reused with the same room/playback, an
**incremented `generation`**, new one-use `attach_ticket`, and
`seek_position_ms` (including zero). Both participants MUST discard frames,
commands and buffers associated with the old generation and establish a new
direct WS attachment. The old control socket remains up.

## Gateway → Backend: `media.room.seek.source`

When an authenticated Gateway source/pipeline actually resolves a keyframe or
segment start, it sends this command on its existing control WebSocket:

```json
{"v":1,"id":"gw-seek-1","type":"command","action":"media.room.seek.source","data":{
 "room_id":"room-...","playback_id":"play-...","generation":2,
 "connection_epoch":2,"source_position_ms":118500}}
```

The Backend checks current Gateway session, generation and attach epoch. It
stores `seek_source_ms` separately from `position_ms`; a Gateway cannot claim
that the TV decoder displayed a frame. A second conflicting result is stale.

The existing TV `media.room.report` remains the only authoritative playback
position. The existing independent TV/Gateway execution reports still gate
PLAYING. All changes increment `revision` and the global `sequence` and fan out
on `media.room.changed`. UI clients subscribe before requesting snapshot.

## Remaining integration acceptance

- Implement local and remote media `SeekStreamOpener` sources using a container
  demuxer/timeline index; no naive `milliseconds * bitrate` math.
- Implement FFmpeg seek with keyframe-aware preroll and transcoder restart.
- TV clears old decoder/JS/native buffers before new attach and reports actual
  reached `position_ms`, including seek backward, forward, after EOF.
- Test local and remote files, bounded queues, cancellation during blocked I/O,
  event ordering across two UI panels, and real hardware.
