# Configuration Reference

`zrb` uses two separate TOML config files: one on the **Source** (the host that sends backups) and one on the **Remote** (the server that receives them). Neither file is shared — each host has its own.

Default paths:
- Source: `~/.config/zrb/config.toml` (override with `--config`)
- Remote: `~/.config/zrb/server.toml` (override with `--config`)

---

## Source config (`config.toml`)

Written on the Source host. Controls which datasets to back up, where to send them, and how to prune snapshots locally.

### `[source]`

```toml
[source]
name = "my-laptop"
```

| Key    | Type   | Required | Description                                                                                    |
|--------|--------|----------|------------------------------------------------------------------------------------------------|
| `name` | string | yes      | The Client Name — a stable identifier for this host. Must match the `--client` argument in the Remote's `authorized_keys` `ForceCommand`. |

### `[remotes.<name>]`

One section per Remote. The `<name>` is a local label (e.g. `primary`) used when referencing this remote in `[datasets]` and on the `--remote` flag.

```toml
[remotes.primary]
host = "backup.example.com"
port = 22
user = "zfsbackup"
ssh_key = "/home/user/.ssh/id_zrb"
ssh_opts = ["-o", "ServerAliveInterval=30"]
zfs_send_opts = ["-Lec"]
bandwidth_limit = "10M"
```

| Key               | Type            | Required | Default | Description                                                                                                     |
|-------------------|-----------------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| `host`            | string          | yes      | —       | SSH hostname or IP of the Remote.                                                                               |
| `port`            | integer         | no       | `22`    | SSH port.                                                                                                       |
| `user`            | string          | no       | current user | SSH username on the Remote.                                                                                |
| `ssh_key`         | string (path)   | no       | SSH agent / default key | Path to the private key file.                                                                 |
| `ssh_opts`        | string array    | no       | `[]`    | Extra arguments passed verbatim to `ssh`.                                                                       |
| `zfs_send_opts`   | string array    | no       | `[]`    | Extra arguments passed verbatim to `zfs send` (e.g. `["-Lec"]` for large block / embedded data / compressed).  |
| `bandwidth_limit` | string          | no       | none    | Maximum send throughput. See [Bandwidth format](#bandwidth-format) below.                                       |

#### Bandwidth format

`bandwidth_limit` is a string with an optional SI prefix and an optional unit suffix:

| Suffix           | Meaning                                  | Example           |
|------------------|------------------------------------------|-------------------|
| none / `B` / `b` | bytes/sec                                | `"10M"` = 10 MB/s |
| `bit` / `bits`   | bits/sec (divided by 8)                  | `"100Mbit"` = 12.5 MB/s |

SI prefixes (case-insensitive):

| Prefix | Multiplier    |
|--------|---------------|
| `k`/`K`| × 1 000       |
| `m`/`M`| × 1 000 000   |
| `g`/`G`| × 1 000 000 000 |

Decimal values are accepted. A bare integer is bytes/sec.

```toml
bandwidth_limit = "10M"       # 10 MB/s
bandwidth_limit = "100Mbit"   # 100 Mbit/s = 12.5 MB/s
bandwidth_limit = "1.5G"      # 1.5 GB/s
bandwidth_limit = "512K"      # 512 KB/s
bandwidth_limit = "1048576"   # 1 048 576 bytes/s (bare integer)
```

### `[datasets.<local-dataset>]`

Maps a local ZFS dataset to its destination path on one or more remotes. Each key inside the section is a remote name matching a `[remotes.<name>]` entry.

```toml
[datasets."tank/home"]
primary = "backup/laptop/home"

[datasets."tank/documents"]
primary = "backup/laptop/documents"
```

A dataset can target multiple remotes:

```toml
[datasets."tank/home"]
primary   = "backup/laptop/home"
offsite   = "offsite/laptop/home"
```

### `[retention]`

Controls how many snapshots `zrb prune` keeps on the Source.

```toml
[retention]
recent           = 7
weekly_for_days  = 30
monthly_for_days = 365
```

| Key               | Type    | Required | Description                                                             |
|-------------------|---------|----------|-------------------------------------------------------------------------|
| `recent`          | integer | yes      | Always keep the last N snapshots regardless of age.                     |
| `weekly_for_days` | integer | yes      | Beyond `recent`, keep one snapshot per ISO week for this many days back. |
| `monthly_for_days`| integer | yes      | Beyond the weekly window, keep one per calendar month for this many days back. Anything older keeps one per year. |

---

## Remote config (`server.toml`)

Written on the Remote host. Controls which clients may connect, which datasets they may write to, and how to prune snapshots on the Remote side.

### `[server]`

```toml
[server]
resume_hold_days = 3
```

| Key                | Type    | Required | Description                                                                                                               |
|--------------------|---------|----------|---------------------------------------------------------------------------------------------------------------------------|
| `resume_hold_days` | integer | yes      | Number of days to retain an interrupted-transfer resume token before discarding it. A transfer interrupted longer than this will restart from scratch on the next `zrb send`. |

### `[clients.<name>]`

One section per Source that is permitted to connect. The `<name>` must match the Client Name the Source declares (its `[source].name`) and must be listed in the `ForceCommand --client <name>` in `authorized_keys`.

```toml
[clients.my-laptop]
allow            = ["backup/laptop/home", "backup/laptop/documents"]
zfs_receive_opts = ["-c"]
```

| Key                 | Type         | Required | Default | Description                                                                                              |
|---------------------|--------------|----------|---------|----------------------------------------------------------------------------------------------------------|
| `allow`             | string array | yes      | —       | Dataset paths this client is permitted to receive into. The server rejects any transfer targeting a path not in this list. |
| `zfs_receive_opts`  | string array | no       | `[]`    | Extra arguments passed verbatim to `zfs receive` for this client (e.g. `["-c"]` to force checksum verification). |

### `[retention]`

Same structure as the Source `[retention]` block. The Remote prunes its own snapshots independently.

```toml
[retention]
recent           = 14
weekly_for_days  = 60
monthly_for_days = 730
```

---

## Complete examples

### Source (`~/.config/zrb/config.toml`)

```toml
[source]
name = "my-laptop"

[remotes.primary]
host            = "backup.example.com"
port            = 22
user            = "zfsbackup"
ssh_key         = "/home/user/.ssh/id_zrb"
ssh_opts        = ["-o", "ServerAliveInterval=30"]
zfs_send_opts   = ["-Lec"]
bandwidth_limit = "50M"

[datasets."tank/home"]
primary = "backup/laptop/home"

[datasets."tank/documents"]
primary = "backup/laptop/documents"

[retention]
recent           = 7
weekly_for_days  = 30
monthly_for_days = 365
```

### Remote (`~/.config/zrb/server.toml`)

```toml
[server]
resume_hold_days = 3

[clients.my-laptop]
allow            = ["backup/laptop/home", "backup/laptop/documents"]
zfs_receive_opts = ["-c"]

[retention]
recent           = 14
weekly_for_days  = 60
monthly_for_days = 730
```
