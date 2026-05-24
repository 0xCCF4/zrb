# zrb — ZFS Remote Backup

`zrb` is a lightweight tool for pushing ZFS snapshots from a Source host (e.g. a laptop) to a Remote server over SSH. It
handles snapshot creation, incremental transfers, interrupted-transfer resumption, and snapshot pruning — all driven
from the Source side.

## How it works

```
Source (laptop)  ──SSH──►  Remote (backup server)
  zrb send                    zrb server (ForceCommand)
```

The Source creates a snapshot, opens an SSH connection, performs a structured handshake to negotiate an incremental
base, and streams the snapshot to the Remote via `zfs send | zfs receive`. The Remote never initiates contact. If a
transfer is interrupted mid-stream, the next `zrb send` resumes from where it left off using ZFS native resume tokens.

## Requirements

- Linux with OpenZFS (`zfs` and `zpool` in `PATH`)
- SSH access from Source to Remote
- Rust toolchain (for source builds) or Nix

## Installation

**From crates.io:**

```sh
cargo install zrb
```

**With Nix:**

```sh
nix run github:0xCCF4/zrb
```

## Setup

### 1. Generate an SSH key on the Source

```sh
ssh-keygen -t ed25519 -f ~/.ssh/id_zrb -C "zrb backup key"
```

### 2. Create a dedicated user on the Remote

```sh
useradd -r -m -s /usr/sbin/nologin zfsbackup
```

Copy the public key to the Remote:

```sh
ssh-copy-id -i ~/.ssh/id_zrb.pub zfsbackup@backup.example.com
```

### 3. Configure SSH ForceCommand on the Remote

Edit `/home/zfsbackup/.ssh/authorized_keys` so that the key runs `zrb server` instead of a shell:

```
command="zrb server --client my-laptop",restrict ssh-ed25519 AAAA... zrb backup key
```

The `--client` flag lists which client names this key is permitted to present. A key may serve multiple clients:
`--client laptop --client desktop`.

### 4. Grant ZFS permissions on the Remote

Delegate only the necessary permissions to the `zfsbackup` user on the dataset subtree it will receive into:

```sh
zfs allow -u zfsbackup receive,create,mount backup/laptop
```

Keep the delegation as narrow as possible — per-dataset subtree, not the whole pool.

### 5. Write the Remote config

`~/.config/zrb/server.toml` on the Remote:

```toml
[server]
# Discard stale interrupted-transfer state after this many days.
resume_hold_days = 3

[clients.my-laptop]
# Datasets this client is allowed to receive into.
allow = ["backup/laptop/home", "backup/laptop/documents"]
zfs_receive_opts = ["-c"]

[retention]
recent = 14
weekly_for_days = 60
monthly_for_days = 730
```

### 6. Write the Source config

`~/.config/zrb/config.toml` on the Source (default path; override with `--config`):

```toml
[source]
name = "my-laptop"

[remotes.primary]
host = "backup.example.com"
port = 22
user = "zfsbackup"
ssh_key = "/home/user/.ssh/id_zrb"
ssh_opts = ["-o", "ServerAliveInterval=30"]
zfs_send_opts = ["-Lec"]

# Map each local dataset to its destination path on each remote.
# Key = local dataset, value = map of remote-name → remote dataset.
[datasets."tank/home"]
primary = "backup/laptop/home"

[datasets."tank/documents"]
primary = "backup/laptop/documents"

[retention]
recent = 7
weekly_for_days = 30
monthly_for_days = 365
```

## Usage

All subcommands accept `--verbose` for debug logging and `--config <path>` to override the default config location.

### `zrb send <dataset>...`

Creates a snapshot, connects to all configured Remotes, and transfers incrementally.

```sh
# Send two datasets to all remotes
zrb send tank/home tank/documents

# Restrict to a single named remote
zrb send tank/home --remote primary
```

If the previous transfer was interrupted, `zrb send` resumes it automatically before starting new sends.

### `zrb snapshot <dataset>...`

Creates a `zrb-`-prefixed snapshot locally without transferring it.

```sh
zrb snapshot tank/home tank/documents
```

Useful for taking a local checkpoint before a risky operation.

### `zrb list <dataset>`

Lists all `zrb`-managed snapshots for a dataset.

```sh
zrb list tank/home
```

### `zrb prune <dataset>`

Deletes snapshots that fall outside the Retention Policy. Runs locally — Source and Remote prune independently.

```sh
zrb prune tank/home
```

### `zrb server --client <name>...`

Runs in server mode. Intended to be called only by SSH `ForceCommand`, not directly.

## Automating with systemd

Create a service and timer on the Source to run backups on a schedule.

**`/etc/systemd/system/zrb-send.service`**

```ini
[Unit]
Description=ZFS remote backup
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
ExecStart=/usr/local/bin/zrb send tank/home tank/documents
# Kill and restart if a single 4 MiB chunk takes longer than this to transfer.
WatchdogSec=1m
```

**`/etc/systemd/system/zrb-send.timer`**

```ini
[Unit]
Description=Run ZFS remote backup daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```sh
systemctl enable --now zrb-send.timer
```

Check the last run:

```sh
systemctl status zrb-send.service
journalctl -u zrb-send.service -n 50
```

## NixOS

There are NixOS modules for easy integration of server and/or client.

Both modules are exposed as `nixosModules.server` and `nixosModules.client` from the flake.

Add to your `flake.nix` inputs:

```nix
inputs = {
  nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  
  zrb = {
    url = "github:0xCCF4/zrb";
    inputs.nixpkgs.follows = "nixpkgs";
  };
};
```

### Server

```nix
{
  imports = [ inputs.zrb.nixosModules.server ];

  services.zrb.server.main = {
    enable = true;

    retention = {
      recent = 14;
      weeklyForDays = 60;
      monthlyForDays = 730;
    };

    clients.my-laptop = {
      publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI... zrb backup key";
      allow = [ "backup/laptop/home" "backup/laptop/documents" ];
    };
  };
}
```

The module creates the `zrb` system user, writes `/etc/zrb/main/server.toml`, and adds a `ForceCommand`-restricted entry
to the user's `authorized_keys` for each client key. You still need to grant ZFS permissions manually:

```sh
zfs allow -u zrb receive,create,mount backup/laptop
```

### Client

```nix
{
  imports = [ inputs.zrb.nixosModules.client ];

  services.zrb.client = {
    enable = true;
    sourceName = "my-laptop";

    remotes.primary = {
      host = "backup.example.com";
      user = "zrb";
      sshKey = "/etc/zrb/id_ed25519";
    };

    datasets = {
      "tank/home".primary      = "backup/laptop/home";
      "tank/documents".primary = "backup/laptop/documents";
    };

    retention = {
      recent = 7;
      weeklyForDays = 30;
      monthlyForDays = 365;
    };

    jobs.nightly = {
      onCalendar = "daily";
      datasets = [ "tank/home" "tank/documents" ];
      # Default is "1m". Increase for very slow links; set null to disable.
      watchdogSec = "1m";
    };

    prune.onCalendar = "weekly";
  };
}
```

The module creates the `zrb` system user, writes `/etc/zrb/client.toml`, and registers a `zrb-send-nightly` systemd
service+timer and a `zrb-prune` service+timer. The SSH key at `sshKey` must be provisioned separately (e.g. via
`sops-nix` or `agenix`).

## Retention policy

The same `[retention]` block is used in both Source and Remote configs. Each host prunes independently.

| Window                           | Rule                   |
|----------------------------------|------------------------|
| Last N snapshots                 | Always kept (`recent`) |
| Older, within `weekly_for_days`  | One per ISO week       |
| Older, within `monthly_for_days` | One per calendar month |
| Older still                      | One per year           |

Only snapshots with the `zrb-` prefix are managed. Any snapshots you created manually are left untouched.

## Security model

Three independent layers protect the Remote:

1. **ZFS delegation** (`zfs allow`) — the OS enforces which datasets the backup user may write to, regardless of what
   `zrb` does. This is the authoritative boundary.
2. **SSH key → client name binding** — each `authorized_keys` entry restricts which client names may connect with that
   key. A compromised key cannot impersonate unlisted clients.
3. **Server config allowlist** — `[clients.<name>].allow` maps each client to its permitted receive targets, catching
   misconfiguration early with a clear error message.

## Snapshot naming

All managed snapshots follow the pattern `<dataset>@zrb-<ISO-8601-UTC>`, e.g.:

```
tank/home@zrb-2026-05-22T14:30:00Z
```

`zrb` ignores all snapshots that do not match this prefix.


## On the use of AI

This project was a long-standing concept and half finished prototype before any AI tools were used. When I recently
tried Claude for the first time, I let it read the existing code and documentation, and in a supervised manner,
used it to bring to project into a usable state. Note that every change was carefully reviewed and edited afterward by
myself @0xCCF4.

## License

Due to the use of AI in the development process, I cannot confidently assert that this project is free
of "inspiration" from others' work, but I am not aware of any equivalent project, from which, AI might have copied
major parts. Since the idea, starting code base, and so on where given by myself, I am for now releasing this project
under the GPLv3 license into the open source community.

## Contributions

Contributions are welcome! Please open an issue or submit a pull request.