# HC Storage Service file endpoints

HC SS automatically starts file transfer WebSocket listeners on port 8093 on
detected private IPv4 interfaces; `file_ws` is no longer configured by the
administrator, and there are no tokens, per-device static IPs or auth modes.

The agent advertises discovered connections over the existing Backend control
WS as part of `storage.agent.inventory`:

```json
{
  "file_ws": {
    "endpoint": "ws://192.168.0.147:8093/ws/files",
    "endpoints": [
      "ws://192.168.0.147:8093/ws/files",
      "ws://100.114.58.74:8093/ws/files"
    ],
    "mode": "trusted_network"
  }
}
```

`endpoint` and `mode` are retained for **older HomeCloudUI** clients and
do not control authentication. New clients use the list, falling back when a
network is unreachable. Backend validates private numeric IPs and WebSocket
paths, and forwards these strings through `storage.agent.status`; it never
relays actual file data.

Network access must be controlled by a host firewall or equivalent local
network policy. Restrict port 8093 to trusted private clients. The channel
itself does not have user authentication or encryption; host filesystem root
permissions and descriptor-confined operations remain enforced in HC SS.

Upgrade in order: Backend, HC SS, HomeCloudUI. Do not rewrite HC SS roots,
storage identity or SQLite. Roll back each executable from its deployment
backup if endpoint registration fails.
