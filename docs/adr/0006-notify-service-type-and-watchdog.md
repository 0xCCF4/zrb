# ADR 0006 — systemd service type: notify + per-chunk watchdog pings

## Status

Accepted

## Context

`zrb send` is invoked as a systemd service on the Source. Transfers can run for minutes to hours. Without a watchdog, a
stalled transfer (hung SSH connection, unresponsive remote, blocked `zfs send` subprocess) is invisible to systemd — the
service appears healthy until it is manually inspected or the timer fires again.

`sd-notify` was already a dependency and `NotifyState::Ready` / `NotifyState::Stopping` were already emitted in
`main.rs`, but the service type was `oneshot`, which causes systemd to silently discard all sd_notify signals.

## Decision

Change the NixOS client module's send service type from `oneshot` to `notify`. Emit `NotifyState::Watchdog` once per 4
MiB chunk inside the progress callback in `ops/send.rs`. Expose `WatchdogSec` as an optional per-job option in the NixOS
module, defaulting to `"1m"`.

The watchdog interval is deliberately set against the chunk boundary: if no chunk completes within `WatchdogSec`, the
transfer has stalled and systemd should intervene. On fast links many chunks complete per minute; on slow links the
value should be raised.

## Alternatives considered

- **Stay on `oneshot`**: simple but loses all sd_notify signalling and makes hung transfers invisible to systemd.
- **Background watchdog thread**: a separate thread could ping on a fixed timer, decoupled from chunk size. Rejected:
  adds threading complexity and the chunk boundary is a natural and meaningful liveness checkpoint — if data is moving,
  chunks complete.
- **`WatchdogSec` in prune service**: prune is a short local operation with no network I/O and no sd_notify calls;
  changing its type would be misleading. Prune stays `oneshot`.

## Consequences

- Deployments that add `WatchdogSec` to the service config get automatic kill-and-restart on stalled transfers.
- The `NotifyState::Ready` and `NotifyState::Stopping` calls in `main.rs` are now honoured by systemd and visible in
  `systemctl status`.
- Callers of `ops::send::send` and `ops::send::send_resume` get watchdog pings for free; they cannot opt out. This is
  acceptable: `sd_notify` is a no-op when no watchdog socket is present (e.g. in tests or manual invocations).
- The `WatchdogSec` default of `"1m"` may be too tight for very slow links (sub-70 KB/s per chunk). Users on such links
  should raise it via the `watchdogSec` job option.
