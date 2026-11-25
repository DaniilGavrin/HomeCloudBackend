# Device Bridge process contract v1

The Backend starts Device Bridge as a child process when `device_bridge.enabled` is true.

## Environment

The Backend provides:

- `HOMECLOUD_BACKEND_WS_URL`: local WebSocket endpoint, for example `ws://127.0.0.1:8094/ws`.
- `HOMECLOUD_DEVICE_BRIDGE_CONFIG`: absolute path to the Bridge configuration file.
- `HOMECLOUD_DEVICE_BRIDGE_SUPERVISED=1`: process is managed by Backend.

The Bridge must connect only to this local endpoint using the existing WS JSON v1 envelope. It must identify itself with `device_id=homecloud-device-bridge` and `client_type=device_bridge`.

## Lifecycle

1. Backend starts the local loopback listener before starting the child.
2. Bridge connects and completes the existing `hello` handshake.
3. Bridge sends `device.bridge.register` with `contracts_version=1` and its version.
4. Bridge loads its local registry and completes full reconciliation.
5. Bridge sends `device.bridge.ready`.
6. Bridge sends `device.bridge.heartbeat` periodically. The Backend considers the connection stale when no heartbeat arrives within the configured timeout.

The process must treat EOF on stdin as a graceful shutdown request. Backend closes stdin first and sends a process kill only if the configured shutdown deadline expires.

## Recovery

Backend continues operating when Bridge cannot start or becomes unavailable. The supervisor retries indefinitely with exponential backoff, capped by `max_backoff_seconds` and randomized jitter. It does not queue or replay device commands.

The Backend's loopback listener is plaintext by design and must bind to an explicit loopback IP only. It is not exposed on LAN or Tailscale interfaces. This is a local trust boundary, not cryptographic process attestation.

## Configuration

See the `device_bridge` section in `config/config.yaml`. The feature is disabled by default to preserve existing installations. Enable it only after installing the Bridge executable and creating its configuration.
