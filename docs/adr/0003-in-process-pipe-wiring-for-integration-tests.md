# ADR 0003 — In-process pipe wiring for integration tests

## Status

Accepted

## Context

Integration tests need to exercise the full client↔server protocol against real
ZFS datasets without requiring a running SSH daemon, key configuration, or
`ForceCommand` setup. The send path (`send_to_remote`) currently spawns SSH
internally, and the server path (`run_server`) reads from `stdin`/`stdout`
directly — both are hardwired to real I/O.

## Decision

Expose a generic inner function at each boundary:

- `ops::server::run_server_on<R: Read, W: Write>` — the server state machine
  decoupled from stdin/stdout
- `ops::send::send_on<R: Read, W: Write>` — the send state machine decoupled
  from SSH

Production code calls these through thin wrappers that provide the real
stdin/stdout or SSH pipes. Tests wire them directly over `std::io::pipe()` pairs
running on separate threads.

## Alternatives considered

**Real SSH daemon** — requires key generation, `sshd` config, `ForceCommand`
wiring per test. Slow, fragile, needs root or a dedicated test user. Rejected.

**Mock/trait abstraction for ZFS calls** — would allow protocol-only tests with
no ZFS at all. Rejected for now: the additional abstraction layer has ongoing
maintenance cost and the end-to-end test with real ZFS catches more bugs.

## Consequences

- `run_server_on` and `send_on` are public API surface. They should not be
  called by anything except the thin wrappers and tests.
- Tests require ZFS and are `#[ignore]` by default; run with
  `sudo cargo test -- --include-ignored`.
- The file-backed pool approach (`truncate` + `zpool create`) keeps pool
  creation fast (~1 s) and isolated per test run.
