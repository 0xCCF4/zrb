# Contributor Onboarding

This guide orients a new developer. For user installation and operator setup, see [README.md](README.md).

## ZFS background

**Snapshots** are point-in-time, copy-on-write references to a dataset's state. They cost almost nothing to create and
are named `dataset@snapshot-name`. The key property `zrb` exploits: given two snapshots A and B on the same dataset,
`zfs send -i A B` produces a binary stream of *only the changes between them* — an incremental send. The Remote can
apply that stream with `zfs receive` to bring its copy of the dataset up to date without retransmitting data already
present.

**Resume tokens** handle interrupted transfers. When the Remote runs `zfs receive -s`, a partial receive is
checkpointed. The token is a ZFS-native opaque string retrievable via `zfs get receive_resume_token <dataset>`. On
reconnect, the Source issues `zfs send -t <token>` instead of a normal incremental send — ZFS skips the already-received
bytes and picks up mid-stream. `zrb` exchanges this token during the protocol handshake.

## Prerequisites

Both tracks are required:

| Track                              | What it covers                                        |
|------------------------------------|-------------------------------------------------------|
| Rust toolchain (`rustup`, `cargo`) | Building the binary, running unit tests, clippy       |
| Nix with flakes enabled            | Eval tests, VM integration tests, reproducible builds |

Install Nix from [nixos.org/download](https://nixos.org/download) and enable flakes in `~/.config/nix/nix.conf`:

```
experimental-features = nix-command flakes
```

## Build and test

```sh
# Compile
cargo build

# Unit tests
cargo test

# Lint (must exit 0 before any PR)
cargo clippy --all-targets -- -W clippy::pedantic -D warnings
```

**Nix eval tests** — pure evaluation, no VM, fast (~seconds):

```sh
nix build .#checks.x86_64-linux.module-eval-tests
```

Verifies that the NixOS client and server modules produce the expected `systemd.services`, `systemd.timers`,
`environment.etc`, and `users.users` entries at eval time.

**Nix VM tests** — boots three QEMU VMs (server, client, clientNoPrune), Linux-only, slow (~5 min):

```sh
nix build .#checks.x86_64-linux.module-vm-tests
```

Checks runtime behaviour: system user created, config written to disk, `authorized_keys` wired up with `ForceCommand`,
timers enabled or absent as configured.

Run both at once:

```sh
nix flake check
```

## Codebase map

```
src/
├── main.rs          — clap CLI; no business logic
├── lib.rs           — public API re-exports
├── config.rs        — SourceConfig / ServerConfig TOML parsing
├── snapshot/        — snapshot naming (zrb- prefix + UTC timestamp)
├── retention/       — RetentionPolicy; decides which snapshots to delete
├── protocol/        — wire types (ClientHello, ServerHello, ServerStatus, ClientReady, Chunk, ControlFrame)
│   └── codec.rs     — JSON + binary framing; read/write helpers
├── zfs/             — thin wrappers around zfs(8) and zpool(8) subprocesses
│   ├── client.rs    — zfs list, zfs send, zfs receive, zfs destroy
│   └── estimator.rs — zfs send -n -v for incremental-base selection
├── ssh/
│   └── transport.rs — opens SSH subprocess; wires stdin/stdout into the protocol layer
└── ops/             — one file per subcommand
    ├── snapshot.rs  — zrb snapshot
    ├── list.rs      — zrb list
    ├── send.rs      — zrb send / zrb send --resume
    ├── prune.rs     — zrb prune / prune --dry-run / prune --abort-resume
    └── server.rs    — zrb server (ForceCommand handler)
```

All domain logic lives in the library (`src/lib.rs` and its modules). `main.rs` is a thin dispatch layer — keep it that
way.

## Client / server split

`zrb` runs on two hosts with asymmetric roles:

- **Source** (e.g. a laptop) initiates everything. It creates snapshots, opens the SSH connection, selects the incremental
  base, and drives the transfer. Config: `~/.config/zrb/config.toml` (`SourceConfig`).
- **Remote** (the always-on server) is passive. It is invoked via SSH `ForceCommand` — the Source's SSH key triggers
  `zrb server --client <name>` instead of a shell. Config: `~/.config/zrb/server.toml` (`ServerConfig`).

The Remote never dials out. All coordination flows over the single SSH stdio pipe opened by the Source.

## Protocol orientation

Four phases over one SSH connection:

**1. Handshake (JSON)**

```
Source → Remote   ClientHello  { version, client_name, target_dataset }
Remote → Source   ServerStatus { ok: bool, message? }   ← version gate
Remote → Source   ServerHello  { head: <snap | null>, resume_token? }
```

The server validates that client and server share the same *major.minor* version (patch differences are tolerated — ADR
0005). If the check fails, `ServerStatus.ok` is `false` and the server closes; the client surfaces the message and
exits. No transfer happens.

After accepting, the server sends its most recent zrb-managed snapshot (`head`) and any pending resume token. The client
uses `head` as the incremental base — it must exist in the local snapshot list. If `head` is absent locally, the send
fails with a divergence error ("prune it from the Remote and retry"). If `head` is `null`, no prior backups exist on
the Remote and the client performs a full send. The server sends only this single snapshot name (not the full list) to
minimise wire overhead — ADR 0008.

**2. Ready (JSON)**

```
Source → Remote   ClientReady  { ok: bool, message? }
```

The client signals whether it actually has data to send. `ok: false` means nothing to transfer (e.g. the newest
snapshot is already on the Remote); the server exits cleanly without spawning `zfs receive`. `ok: true` starts the
transfer phase. This step prevents a deadlock that occurs when the client decides not to send but the server has
already started waiting for stream data (ADR 0007).

**3. Transfer (binary)**

Fixed 4 MB `Chunk`s, each followed by a 5-byte `ControlFrame` (`u32 actual_size` + `u8 has_more`). If a resume token was
present, the client issues `zfs send -t <token>` — incremental base selection is bypassed entirely.

**4. Status (JSON)**

```
Remote → Source   ServerStatus { ok: bool, message? }
```

The Remote reports success or error after `zfs receive` completes. On success:
- **Source-side**: a `zrb:<remote-name>` ZFS hold is placed on the sent snapshot, preventing prune from deleting the
  incremental base before the Remote has a chance to prune its own older snapshots.
- **Server-side**: a `zrb:received` ZFS hold is placed on the just-received snapshot for the same reason.

`zrb prune` skips any snapshot carrying a `zrb:*` hold and logs a notice. Holds are moved atomically (new hold placed
before old hold is released) so there is never a window with zero holds on the dataset.

The security boundary is ZFS delegation (`zfs allow`) — it restricts which datasets the backup OS user can write to at
the kernel level. The `allowed_datasets` list in `ServerConfig` and the Client Name binding are defence-in-depth, not
the primary enforcement (ADR 0002).

## Further reading

- [CONTEXT.md](CONTEXT.md) — canonical glossary; resolve terminology disputes here
- [docs/adr/](docs/adr/) — records of non-obvious decisions with their trade-offs
- [CLAUDE.md](CLAUDE.md) — build commands, test commands, definition of done
