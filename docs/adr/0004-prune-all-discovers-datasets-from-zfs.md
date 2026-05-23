# ADR 0004 — `zrb prune --all` discovers datasets from ZFS, not from config

## Status
Accepted

## Context
The NixOS client module needs to prune all zrb-managed snapshots on a timer without
knowing in advance which datasets exist. Two approaches were considered:

1. **Config-driven**: read the `datasets` map from the source config and prune each
   key listed there.
2. **Discovery-driven**: run `zfs list -t snapshot`, collect every dataset that has at
   least one `zrb-`prefixed snapshot, and prune those.

## Decision
Use discovery-driven (`--all` scans ZFS, ignores the `datasets` map).

## Consequences
- The NixOS module emits a single `ExecStart = zrb prune --all` regardless of how many
  datasets are configured — no Nix eval-time coupling to the dataset list.
- Prune correctly catches datasets that were removed from the config but still have old
  snapshots on disk (orphaned snapshots are cleaned up automatically).
- Prune runs on datasets that were snapshotted outside of a `zrb send` job (e.g. via
  `zrb snapshot` run manually), which is the desired behaviour.
- The `datasets` map is not consulted, so a misconfigured or missing config file causes
  `prune --all` to fail at the retention-read step, not silently skip datasets. Retention
  settings are still read from the config file.
