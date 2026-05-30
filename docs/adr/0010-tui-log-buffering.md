# ADR 0010 — Buffer log records during TUI alternate-screen mode

## Status

Accepted

## Context

`zrb send --tui` runs the transfer inside a ratatui alternate-screen session.
`env_logger` writes directly to stderr regardless of terminal state, so any
`log::*` call emitted by the concurrent send operation bleeds through into the
alternate screen and corrupts the display.

## Decision

Replace the direct `env_logger::Builder::init()` call with a `TuiLogger` — a
`'static` wrapper around an `env_logger::Logger` that is installed as the
global `log` backend at startup.

`TuiLogger` holds a `Mutex<Option<Vec<StoredRecord>>>`:

- `None` → pass every record straight through to the inner `env_logger::Logger`
  (normal non-TUI behaviour, zero overhead).
- `Some(buf)` → serialize each record into a `StoredRecord` (level, target,
  module\_path, file, line, args as `String`) and append to the buffer.

`run_countdown` and `run_transfer` swap the buffer to `Some` on entry (when
`EnterAlternateScreen` is issued) and take it back to `None` on exit (when
`LeaveAlternateScreen` is issued), returning the drained records.

`main.rs` calls `tui::replay_buffered(records)` after the TUI task joins. Each
stored record is reconstructed into a `log::Record` and fed back to the inner
`env_logger::Logger`, producing output identical to non-TUI runs.

## Alternatives considered

- **Caller-owned flag in main.rs**: simpler, but the alternate-screen lifetime
  is owned by `tui.rs`; duplicating that boundary in two places would drift.
- **Simple `eprintln!` replay with a custom format**: avoids reconstructing
  `log::Record` but produces output inconsistent with non-TUI runs.
- **Redirect stderr to a pipe**: OS-level approach; fragile across platforms and
  would also capture subprocess stderr, which is not the target.
