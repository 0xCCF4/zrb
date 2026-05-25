# ADR 0007 — ClientReady message before server spawns zfs receive

## Status

Accepted

## Context

Before this change, the server unconditionally spawned `zfs receive` immediately after sending `ServerHello`, then
blocked in `read_stream_with_cancel` waiting for a full 4 MiB chunk. The client could legitimately return early without
sending any stream data — for example, when `zrb resume` is run a second time after the transfer had already completed (
newest snapshot already on the Remote). In that case, `conn.stdin` was never dropped before `conn.child.wait()`, so the
SSH pipe stayed open indefinitely. Both sides blocked: the server on `read_exact` and the client on `wait()`. Result:
silent hang, no output.

## Decision

Add a `ClientReady { ok: bool, message: String }` message that the client sends after receiving `ServerHello` and
resolving whether it has data to send. The server reads `ClientReady` before spawning `zfs receive`. If `ok: false`, the
server logs the reason and returns cleanly. If `ok: true`, it spawns the receive process and reads the stream as before.

Client-side: `conn.stdin` is now explicitly dropped before `conn.child.wait()` in both `send_to_remote` and
`resume_to_remote`, ensuring the SSH pipe is always closed regardless of how the protocol ends.

This requires a protocol version bump (0.1 → 0.2): `ClientReady` appears on the wire between `ServerHello` and the
binary stream; a 0.1 server would interpret the first `ClientReady` byte as stream data, corrupting the transfer.

## Alternatives considered

- **Client-side stdin close only**: Dropping `conn.stdin` before `wait()` breaks the deadlock but the server still
  spawns `zfs receive` on every no-data case, consuming a ZFS process and producing a spurious broken-pipe error in its
  logs.
- **Sentinel zero-length chunk**: Client sends a chunk with `actual_size=0` and `has_more=0` to signal "nothing to
  send." Rejected: conflates the binary framing layer with flow control; server still spawns `zfs receive`
  unnecessarily.
