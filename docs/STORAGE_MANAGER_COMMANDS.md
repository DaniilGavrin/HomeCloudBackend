# HomeCloudBackend — STORAGE MANAGER v1

Storage Manager is disabled until explicitly enabled in YAML. All volumes and
file metadata are dynamic SQLite rows, **not** entries in config.yaml.

## Setup (PowerShell)

From project root, after extracting the patch:

```powershell
.\scripts\enable-storage.ps1
go run . init-db
go test ./...
go run . serve
```

The setup script preserves your existing YAML, creates a timestamped backup,
changes only three values inside `storage:` and creates `data/files`.
Alternatively, edit `storage.enabled: true`, `storage.transfer_transport:
"websocket"` and `storage.websocket_binary_file_transfer: true` yourself.

Existing `storage.allowed_roots: ["./data/files"]` restricts which storage
paths may be registered. To use an NFS/SMB/USB mount, explicitly add its
**existing directory** to `allowed_roots` in YAML and restart. The directory
must be accessible and writable by HomeCloudBackend. Do not use an SQLite
file itself on NFS/SMB; the storage volumes may be network mounts, but the
application SQLite database stays local.

## Register a volume (first-time, only once per directory)

```json
{"v":1,"id":"storage-reg-01","type":"command","action":"storage.register","data":{"name":"Main local storage","path":"./data/files","priority":10}}
```

Returns `storage_id`, online status and free/total space. At least one volume
must be explicitly registered for `storage.upload.prepare` with `storage_id:
"auto"` to succeed. A duplicate `path` is rejected, not silently registered
again. `priority` is an integer 0..1000; larger values are chosen first,
provided the target has enough space.

## List volumes

```json
{"v":1,"id":"storage-list-01","type":"command","action":"storage.list"}
```

## Prepare an upload

```json
{"v":1,"id":"upload-01","type":"command","action":"storage.upload.prepare","data":{"name":"vakansiya.pdf","mime_type":"application/pdf","size_bytes":3246124,"storage_id":"auto"}}
```

Returns a one-use `transfer_id`, `file_id`, `storage_id`, and `path` of
`/ws/storage`. Transfer must begin within **2 minutes**.

Open another WebSocket connection to `ws://127.0.0.1:8080/ws/storage` and send:

```json
{"v":1,"type":"storage.hello","transfer_id":"TRANSFER_ID","device_id":"desktop-main","role":"upload"}
```

`device_id` must match the already connected control client that prepared the
transfer. After `storage.ready`, send **binary** WebSocket messages up to
1 MiB each. Then send:

```json
{"v":1,"type":"storage.end","sha256":"COMPLETE_FILE_SHA256_OPTIONAL"}
```

Server checks the declared exact file length and optional SHA-256, finalizes
the temporary file and inserts its metadata in SQLite. It answers with
`storage.complete` including the stored file metadata. On mismatch it sends
`storage.error` and deletes the partial upload. This endpoint does **not**
accept arbitrary filesystem destinations.

## List files

```json
{"v":1,"id":"storage-files-01","type":"command","action":"storage.files.list","data":{"storage_id":"","limit":100}}
```

`storage_id` can be omitted or empty for all volumes. `limit` is capped at 500.

## Prepare a download

```json
{"v":1,"id":"download-01","type":"command","action":"storage.download.prepare","data":{"file_id":"FILE_ID"}}
```

Open `/ws/storage` and send:

```json
{"v":1,"type":"storage.hello","transfer_id":"TRANSFER_ID","device_id":"desktop-main","role":"download"}
```

After `storage.ready`, the server emits 64 KiB binary messages from disk,
then `storage.complete` with size, SHA-256, and file metadata. The receiving
Go client **must** call `SetReadLimit(1 << 20)` (or larger) before reading.
Write binary chunks to a file, verify SHA-256, then use the file.

## Cancel unclaimed transfer

```json
{"v":1,"id":"cancel-01","type":"command","action":"storage.transfer.cancel","data":{"transfer_id":"TRANSFER_ID"}}
```

This cancels only transfers that have not yet attached. Active transfers are
terminated by closing their dedicated socket; file transfers end automatically
on error. Transfers do not resume after disconnect in v1.

## Automatic event

After a successful upload, the server publishes Event Bus topic:
`storage.file.created`. A client that subscribes to the topic receives the
metadata; the source receives `storage.complete` independently.

## Full roundtrip test

Open the running server and register a volume using `storage.register` once.
Then:

```powershell
go run ./scripts/storage-demo.go -file "C:\Users\user\Downloads\vakansiya_pvz_A4.pdf"
```

The demo uploads the file, downloads it, and compares SHA-256. It does not
remove the stored file. Test only with files you want to keep on disk.

## Scope / safeguards

- Storage Manager v1 supports directory-backed volumes: local filesystem,
  mounted USB, NFS or SMB. S3 and cloud APIs are **not** implemented yet.
- Does not replace the existing client-to-client `/ws/stream` binary relay.
  `/ws/storage` is a dedicated server-side upload/download transport.
- `config.yaml` holds allowed directories/limits; SQLite holds registered
  volumes and files. Do not publish `config.yaml` or its backups.
- Files are stored under `<volume>/.homecloud/objects/<file_id>.bin`. File names
  are metadata only and cannot select arbitrary server paths.
- Max upload is controlled by `storage.max_upload_size_mb` (default 1024).
- Endpoints are accessible to trusted LAN/Tailscale devices **without
  application tokens**, according to the architecture you chose. Consequently
  any trusted-network client that can impersonate a connected `device_id`
  may influence stored files. Protect the network and avoid public port
  forwarding. Directory restrictions are not a substitute for OS permissions.
- Deletion, resume, encryption-at-rest, quotas per storage, mount health checks,
  deduplication and automatic rebalancing are **not yet implemented**.
