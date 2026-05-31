# Inline progress bars via indicatif with TTY auto-detection

When `zrb send` is run without `--tui`, the terminal gets no transfer feedback
beyond log lines. We decided to add auto-detected inline progress: if stdout is
a TTY, `indicatif` multi-progress bars update in place per remote; if stdout is
not a TTY, one line per active remote is written to stderr every 10 seconds.

## Considered options

**TTY inline rendering — `indicatif` vs custom ANSI cursor-up.** Custom ANSI
(print N rows, cursor-up N, overwrite) was ruled out because it breaks on
terminal resize and interleaves badly with log output. `indicatif` handles both.
The dependency cost is acceptable — it is the Rust ecosystem standard for this
use case.

**Activation — explicit `--progress` flag vs TTY auto-detect.** An explicit flag
was ruled out because auto-detect is the UNIX convention (`curl`, `git clone`,
`rsync` all do this) and matches the existing `isatty` guard already used by
`--tui`. No `--no-progress` flag is added; redirecting stderr suppresses
non-TTY output in the normal way.

**No-TTY output — stdout vs stderr.** Stderr was chosen because the progress
lines are informational, not data, and the same channel the user would redirect
to suppress them.

## Consequences

- `indicatif` is added as a runtime dependency.
- The `--tui` flag remains the full-screen alternate-screen experience. Inline
  progress is the default when a TTY is present.
- No countdown, no keyboard shortcuts in inline mode — Ctrl-C is the only
  cancellation affordance.
- `send()` and `send_resume()` are unchanged; they already accept
  `Option<Sender<SendEvent>>`. The new inline progress task is a second consumer
  of that channel, wired up in `main.rs` alongside the existing TUI path.
