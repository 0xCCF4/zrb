# ADR 0008 — ServerHello sends only the head snapshot

## Status

Accepted

## Context

`ServerHello` previously contained a full `snapshots: Vec<String>` — every zrb-managed
snapshot on the destination dataset. The client used this list in two ways:

1. **Incremental base selection** — find the most recent server snapshot and verify it
   exists locally before starting `zfs send -i`.
2. **"Already on server" check in `zrb resume`** — confirm the client's latest snapshot
   is not already present on the server.

Both uses only ever needed the most recent server snapshot (the destination head). The
full list added O(snapshots) wire overhead for zero benefit.

## Decision

Replace `snapshots: Vec<String>` with `head: Option<String>` — the single most recent
zrb-managed snapshot on the target dataset, or absent if none exists.

`select_incremental_base` is simplified from a function that ran `max_by_key` over a list
to a direct lookup of `head` in the local snapshot list.

The "already on server" check in `resume_on` compares `latest` against `head` only; a
situation where `latest` is present on the server but not the head is now treated as
divergence (better error message) rather than "already received."

This is a breaking wire-protocol change: a `0.1.x` client sends `snapshots` and cannot
parse a `head` response, and vice versa. The crate version bumps `0.1.x → 0.2.0`; the
existing major.minor version gate rejects mismatched pairs automatically.

## Alternatives considered

- **Keep the full list, use only the last element**: wastes bandwidth proportional to
  history depth (a dataset with 365 snapshots sends 365 names on every transfer); rejected.
- **Add a separate `head` field alongside `snapshots`**: backwards-compatible but bloats
  the protocol permanently; rejected.
