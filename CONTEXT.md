# zfs-remote-backup — Domain Glossary

## Source
The host that owns the live data. Intermittently online (e.g. a laptop). Initiates all transfers by pushing to the Remote.

## Remote / Target
The always-on SSH-reachable server that receives and stores backup snapshots. Never initiates contact with the Source.

## Snapshot
A ZFS point-in-time snapshot created and managed by zrb. Always named with the prefix `zrb-` followed by an ISO-8601 UTC timestamp (e.g. `pool/dataset@zrb-2026-05-22T14:30:00Z`). The tool only manages snapshots matching this prefix; all others are ignored.

## Dataset Mapping
An explicit config entry that maps a source dataset path to its destination path on the Remote. There is no auto-derivation — every backed-up dataset has its own mapping entry.

## Send
The compound operation of: creating a Snapshot on the Source, connecting to the Remote via SSH, performing the structured Protocol handshake, and transferring the snapshot as an Incremental Send. Subcommand: `zrb send`.

## Snapshot (subcommand)
Creates a zrb-prefixed Snapshot locally without transferring it to the Remote. Subcommand: `zrb snapshot`.

## Incremental Base
The snapshot chosen as the base for `zfs send -i`. Selected by estimating the transfer size (`zfs send -n -v`) for each snapshot present on both Source and Remote, then picking the one with the smallest estimated size.

## Prune
The operation of deleting snapshots that fall outside the Retention Policy. Runs locally on whichever host invokes it — Source and Remote prune independently. No cross-host communication. Subcommand: `zrb prune`.

Three invocation forms:
- `zrb prune <dataset>` — prunes a single named dataset.
- `zrb prune <dataset> --recursive` — prunes the named dataset and all child datasets.
- `zrb prune --all` — discovers every dataset on the host that has at least one `zrb-`prefixed snapshot and prunes each one. Does not consult the `datasets` map in the config; retention settings are still read from the config file.

Two modifier flags usable with any of the above forms:
- `--dry-run` — previews what would be kept and deleted without performing any deletions. If a resume transfer is in progress and the hold period has not elapsed, prints a "skipped" notice instead of a snapshot list.
- `--abort-resume` — overrides a resume hold: aborts any in-progress resume token and prunes the dataset regardless of the hold period. Without this flag, a dataset whose resume token is within the hold period is skipped.

## Retention Policy
The tiered ruleset governing which snapshots to keep:
- **Daily**: keep the last N snapshots unconditionally
- **Weekly**: beyond N, keep one per week up to 1 month back
- **Monthly**: beyond 1 month, keep one per month up to 1 year back
- **Yearly**: beyond 1 year, keep one per year

Each host has its own Retention Policy in its own config file.

## Server Mode
The mode in which zrb runs on the Remote, invoked via SSH `ForceCommand`. Handles the Protocol handshake — sends its snapshot list, receives the client's transfer request, validates the target dataset against its config, then pipes stdin to `zfs receive`. Subcommand: `zrb server`.

## Protocol
The structured communication between client (Source) and server (Remote) over a single SSH connection. The client speaks first:
1. **Handshake phase** — JSON messages: client sends `ClientHello` (declaring its Client Name, target dataset, and compiled version); server validates the version (major and minor must match) and replies with `ServerStatus` (version accept/reject). If rejected, server closes and client surfaces the message. If accepted, server then sends `ServerHello` (its snapshot list and any pending Resume Token).
2. **Ready phase** — JSON: client sends `ClientReady` after evaluating whether it has data to send. If `ok: false` (e.g. newest snapshot already on the Remote), the server exits cleanly without spawning `zfs receive`. If `ok: true`, the transfer phase begins.
3. **Transfer phase** — binary stream: fixed 4 MB Chunks, each followed by a Control Frame. The client selects the Incremental Base locally (from snapshots common to both sides) and begins streaming immediately after `ClientReady`.
4. **Status phase** — JSON: server reports success or error after the stream ends.

## Resume Token
A ZFS-native opaque string saved by `zfs receive -s` when a transfer is interrupted mid-stream. Retrieved via `zfs get receive_resume_token <dataset>`. When present, the client issues `zfs send -t <token>` instead of a normal Incremental Send. The Remote discards the token (via `zfs receive -A`) when Prune runs on the target dataset, after which the next Send retries from scratch.

## Client Name
A user-chosen human-readable identifier for a Source host, set once in the Source's config file. Included in handshake JSON. On the Remote, each SSH authorized key entry binds a key to a set of permitted Client Names via one or more `ForceCommand --client <name>` arguments. The client self-declares its name; the server rejects any handshake where the declared name is not in the permitted set for the connecting key. Multiple clients may share one SSH key if they are all listed in that key's `--client` set.

## Security Boundary
ZFS delegation (`zfs allow`) is the authoritative security boundary — it restricts which datasets the backup OS user can write to at the filesystem level. The server-side `allowed_datasets` config and the Client Name binding are defence-in-depth layers that catch configuration errors early and provide clear error messages, not the primary enforcement mechanism.

## Chunk
A fixed 4 MB block of raw ZFS send stream data. Zero-padded when the final chunk is shorter than 4 MB.

## Control Frame
A 5-byte binary struct appended after each Chunk: `u32 actual_size` (real data bytes in the preceding Chunk) + `u8 has_more` (1 if another Chunk follows, 0 if stream is complete).

## List
Displays zrb-managed snapshots on the local host, grouped by dataset. Subcommand: `zrb list`.

Three invocation forms:
- `zrb list` — lists all datasets that have at least one zrb-managed snapshot.
- `zrb list <dataset>` — lists snapshots for that dataset only.
- `zrb list <dataset> --recursive` — lists the named dataset and all child datasets. `zrb list --recursive` (no dataset) behaves the same as `zrb list`.

## Bandwidth Limit
An optional per-remote cap on transfer throughput, enforced in bytes/sec internally. Configured as `bandwidth_limit` in the Source's `RemoteConfig` using a human-readable string: an optional SI prefix (`k`/`K` = ×1 000, `m`/`M` = ×1 000 000, `g`/`G` = ×1 000 000 000) and an optional unit suffix (`bit`/`bits` → divide by 8 to convert from bits/sec to bytes/sec; no suffix or trailing `B` → bytes/sec). Decimal values are accepted. A bare integer string is bytes/sec. Examples: `"10M"` = 10 MB/s, `"100Mbit"` = 12.5 MB/s. When set, the send side enforces the limit inside the Protocol's stream-writing layer (one fixed-rate token bucket applied per Chunk). Absent means no cap — the transfer uses whatever the network and ZFS provide.
