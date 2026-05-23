# ADR 0001 — Use ZFS native resume tokens for interrupted transfers

## Status
Accepted

## Context
The Source is a laptop that may go offline mid-transfer (systemd shutdown timeout, network drop). The Remote must be able to hold partial receive state and allow the Source to resume on reconnect, without retransmitting already-received data. The hold window is configurable (e.g. 1–3 days); after expiry the partial state is discarded.

## Decision
Use `zfs receive -s` on the Remote and `zfs send -t <token>` on the Source for resumption. The Resume Token is retrieved via `zfs get receive_resume_token` and exchanged during the Protocol handshake.

## Alternatives considered
- **Chunk-skipping with background daemon**: track chunk count in a persistent daemon; client re-sends from `zfs send` start but discards already-sent chunks. Rejected: reimplements ZFS's own resume logic, requires a long-running daemon process on the Remote.
- **Temp file replay**: buffer received chunks to disk, replay into fresh `zfs receive` on reconnect. Rejected: doubles disk I/O, still requires chunk tracking.

## Consequences
- Remote must run `zfs receive -A <dataset>` to abort a stale in-progress receive after the hold timeout expires.
- The Protocol handshake must carry the Resume Token from Remote to Source when one is present.
- `zfs send -t` sends the full remaining stream from the resume point — the normal Incremental Base selection is bypassed when resuming.
