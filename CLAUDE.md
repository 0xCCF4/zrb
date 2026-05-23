# zrb — ZFS Remote Backup

## Build and test commands

```
cargo build
cargo test
cargo clippy --all-targets -- -W clippy::pedantic -D warnings
```

### NixOS module tests

Two suites live under `nix/tests/` and are exposed as flake checks:

**Eval tests** (`nix/tests/eval.nix`) — pure Nix evaluation, no VM, runs on all platforms:

```
nix build .#checks.x86_64-linux.module-eval-tests
```

Checks that the server and client modules produce the expected `environment.etc` entries,
`systemd.services`/`timers`, and `users.users` configuration at eval time.  Fast (~seconds).

**VM tests** (`nix/tests/vm.nix`) — boots three QEMU VMs (server, client, clientNoPrune)
and runs a Python test script against them.  Linux-only; skipped on macOS via
`lib.optionalAttrs pkgs.stdenv.isLinux`.  Slow (~5 minutes, no KVM):

```
nix build .#checks.x86_64-linux.module-vm-tests
```

Checks runtime behaviour: system user created, config file written to disk,
`authorized_keys` contains the `command=` entry, systemd timers enabled/absent.

Both suites are included in `nix flake check`.

## Crate structure

- `src/lib.rs` — public library API; all domain logic lives here
- `src/main.rs` — thin `clap` wrapper; no business logic
- Modules: `config`, `snapshot`, `retention`, `protocol`, `zfs`, `ssh`, `ops`

## Definition of done for every issue

Before marking work complete, run:

```
cargo clippy --all-targets -- -W clippy::pedantic -D warnings
```

and fix all diagnostics until it exits 0.

## Agent skills

### Issue tracker

Issues live as local markdown files under `.scratch/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default label vocabulary (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context repo — one `CONTEXT.md` at root plus `docs/adr/`. See `docs/agents/domain.md`.
