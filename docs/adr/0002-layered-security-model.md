# ADR 0002 — Layered security model: ZFS delegation + SSH key binding + config allowlist

## Status
Accepted

## Context
The Remote exposes a `zrb server` endpoint via SSH ForceCommand. Multiple Source hosts may connect using different SSH keys. The system must prevent a compromised key or misconfigured client from writing to datasets it does not own.

## Decision
Three layers, each independently enforceable:

1. **ZFS delegation** (`zfs allow`) — the backup OS user is granted only `receive,create,mount` on specific dataset subtrees. This is the authoritative enforcement layer; the OS enforces it regardless of what the tool does.

2. **SSH key → Client Name set binding** — each public key in `authorized_keys` has its own `ForceCommand` specifying one or more `--client <name>` arguments. The client self-declares its name; the server rejects handshakes where the declared name is not in the permitted set for the connecting key. Multiple clients may share one SSH key if all their names are listed in that key's `--client` set.

3. **Config allowlist** (`allowed_datasets`) — maps Client Names to permitted receive targets. Validated before any receive begins; mismatches produce a clear configuration error rather than a cryptic ZFS permission denial.

## Alternatives considered
- **Single shared SSH key for all clients with no name restriction**: rejected — a compromised key would grant full access with any client name. The `--client` set on each key limits blast radius to the explicitly listed names.
- **Config allowlist as primary boundary**: rejected — config files can be misconfigured; ZFS delegation is kernel-enforced and cannot be bypassed by the tool.

## Consequences
- Each Source host requires its own SSH key pair and a corresponding `authorized_keys` entry on the Remote.
- Setup documentation must cover all three layers: `zfs allow`, `authorized_keys` ForceCommand, and server config allowlist.
- ZFS delegation scope should be as narrow as possible (per-dataset subtree, not whole pool).
