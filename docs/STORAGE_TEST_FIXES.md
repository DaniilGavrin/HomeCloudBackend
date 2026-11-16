# HomeCloudBackend — Storage activation + test fixes v1

This patch is based on the project export dated 2026-10-25 15:19:16.
It contains **only the 7 updated Go/PowerShell source and test files** plus this readme. No YAML, database, credentials, binaries, or full project tree are included.

## Fixes

1. `scripts/enable-storage.ps1`: match Windows CRLF and Unix LF in the existing YAML storage section. Keep the original newline style and inline comments. Back up `config/config.yaml` before modifying its three storage settings.
2. `internal/wsserver/clients_send_integration_test.go`: after receiving the `disconnected` application response, complete the **client** side of WebSocket close handshake before checking the server registry.
3. `internal/wsserver/events_integration_test.go`: same close-handshake correction for disconnect/reconnect and presence-event tests.
4. `internal/wsserver/server.go`: use 10 seconds as the control-message write-timeout fallback when a test/server configuration has a zero or negative `WriteTimeoutSeconds`. Otherwise the first hello response attempts to write under an already-expired context and the client sees EOF.
5. `wsconfig/checkstart.go`: treat omitted `security.authorization.permissions_storage` as the default `database` backend in STRICT mode, while still rejecting any unsupported nonempty backend.
6. `wsconfig/config_matrix_test.go`: update the explicit number of leaves in the YAML/schema parity test from the old 195 to the current 184. The actual schema/YAML fields are still compared exhaustively.
7. `wsconfig/checkstart_test.go`: regression coverage for the optional backend default and rejection of unsupported backends.

## Applying on Windows PowerShell

From `C:\Users\user\IdeaProjects\HomeCloudBackend`:

```powershell
# Optional safety net; do NOT overwrite your own repository with a full archive.
# Extract the PATCH into the project root, not into internal/.
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\HomeCloudBackend_Storage_And_Test_Fixes_v1.zip" -DestinationPath . -Force

.\scripts\enable-storage.ps1
go run . init-db
go test ./... -count=1
```

The storage-enabling script backs up your real YAML locally before modifying it. It must print `[CONFIG] Enabled storage` (or `Storage already enabled`), not `Missing storage setting: enabled`.

Run race tests after a successful ordinary test pass:

```powershell
go test -race ./... -count=1
```

Launch the server, then in a second terminal:

```powershell
go run . serve
go run ./scripts/storage-demo.go -file "C:\Users\user\Downloads\vakansiya_pvz_A4.pdf"
```

The demo is expected to persist the file in the registered storage volume. Your storage must be registered before uploading (see `docs/STORAGE_MANAGER_COMMANDS.md`).

## Validation and scope

The changed Go files were parsed/formatted by `gofmt` 1.23.2; the updated YAML section matcher was separately simulated against both CRLF and LF layouts. Full project tests must run on the user's Go 1.27.3 because the sandbox only has Go 1.23.2 and lacks the project dependency set. The source fix is not a substitute for running the full suite.

If any failing test remains, provide **the updated first failing test and server log**. Do not overwrite `config/config.yaml` or the database with sample files.
