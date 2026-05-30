# ADR 0009 — Parse `zfs` output as JSON; require ZFS userland ≥ 2.3

## Status
Accepted

## Decision
All `zfs list` and `zfs get` calls use the `--json` flag introduced in OpenZFS 2.3. `zfs` stderr text matching (e.g. `"dataset does not exist"`, `"no such tag"`) is unchanged — JSON mode applies to stdout only. The binary checks `zfs version` at startup (after argument parsing) and fails with a clear message that includes the found version if the requirement is not met.

## Why
The previous approach parsed tab-separated and whitespace-delimited text output. That format is undocumented, locale-sensitive, and has changed across ZFS releases. JSON output is versioned (`vers_major`/`vers_minor` in the envelope) and stable by design.

## Alternatives considered
- **Keep text parsing**: avoids the version floor but stays brittle. Rejected because ZFS 2.3 is widely available (ships with NixOS 24.11+, Ubuntu 24.10+) and the tool already targets modern ZFS features.
- **Probe `--json` instead of parsing `zfs version`**: simpler, but gives no "found version X" in the error message — harder to diagnose on misconfigured systems.
- **Fall back to text parsing on older ZFS**: avoids the hard floor but doubles the code paths and test surface indefinitely. Rejected.

## Consequences
- `zfs holds --json` does not exist in ZFS 2.3 or 2.4; `snapshot_holds`, `find_held_snapshot_in`, and `batch_snapshot_holds` remain on tab-separated text parsing.
- Serde structs for the JSON envelope live in `src/zfs/schema.rs`; deserialization failures surface as a new `ClientError::JsonParse(serde_json::Error)` variant.
- ZFS 2.3+ must be noted as a system requirement in README and NixOS module documentation.
