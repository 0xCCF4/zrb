# zrb — ZFS Remote Backup

`zrb` automates what raw `zfs send` leaves to you: incremental base selection, interrupted-transfer resumption,
snapshot management, and multi-remote delivery. NixOS modules for both server and client included — drop in and go.

One command does the work:

```sh
zrb send tank/home  # sends to all configured remotes
# - creates a new snapshot
# - queries each remote for its existing snapshots
# - selects the base that minimizes transferred data
# - resumes any interrupted transfer, then streams the delta
```

Pruning runs independently on each host according to its own retention policy.

## How it works

```
Source (laptop)  ──SSH──►  Remote (backup server)
  zrb send                    zrb server (ForceCommand)
```

`zrb send` works in two phases over a single SSH connection:

1. **Handshake** — the server reports its most recent snapshot; the client uses it as the
   incremental base (or falls back to a full send if the server has no snapshots). If the
   server's head is absent from the local history, the send fails with a clear error.
2. **Transfer** — the client runs `zfs send` and pipes the stream to `zfs receive` on the
   server. If a previous transfer was interrupted, the client resumes it via ZFS native
   resume tokens before starting the next send.

All connections are push from the client — the server runs only as an SSH `ForceCommand`.

## Requirements

- Linux with ZFS executables (`zfs` and `zpool` in `PATH`)
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

### 2. Grant ZFS permissions on the Source

Delegate the minimum permissions to the user that will run `zrb` on each dataset you intend to back up:

```sh
zfs allow -u <user> snapshot,send,hold,release,destroy,mount tank/home
```

`snapshot` and `send` are required for `zrb send`; `hold` and `release` are required for Transfer Holds (protecting the last-sent snapshot from being pruned); `destroy,mount` is required for `zrb prune`.
Instead of `send` you may grant `send:raw` to prevent encrypted datasets from being send unencrypted.

### 3. Create a dedicated user on the Remote

```sh
useradd -r -m -s /bin/bash zfsbackup
```

Copy the public key to the Remote:

```sh
ssh-copy-id -i ~/.ssh/id_zrb.pub zfsbackup@backup.example.com
```

### 4. Configure SSH ForceCommand on the Remote

Edit `/home/zfsbackup/.ssh/authorized_keys` so that the key always runs `zrb server` instead of a shell:

```
command="zrb server --client my-laptop",restrict ssh-ed25519 AAAA... zrb backup key
```

The `--client` flag lists which client names this key is permitted to present. A key may serve multiple clients:
`--client laptop --client desktop`.

### 5. Grant ZFS permissions on the Remote

Delegate only the necessary permissions to the `zfsbackup` user on the dataset subtree it will receive into:

```sh
zfs allow -u <user> receive:append,create,hold,release,destroy,mount backup/laptop
```

Keep the delegation as narrow as possible — per-dataset subtree, not the whole pool.

After the first backup run has created the target datasets, set `readonly=on` on the parent to prevent
accidental filesystem writes by any process on the Remote:

```sh
zfs set readonly=on backup/laptop
```

This has no effect on `zfs receive` or snapshot pruning — those operate at the ZFS layer, not the
filesystem layer, and are unaffected by the property. Only normal file writes through the mounted
filesystem are blocked.

**Do not create the target datasets manually.** `zrb` creates them automatically on the first transfer via
`zfs receive`. Pre-existing datasets will cause `zfs receive` to fail.

### 6. Write the Remote config

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

### 7. Write the Source config

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

If the previous transfer was interrupted, `zrb send --resume` resumes it automatically.

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

### `zrb prune`

Deletes snapshots that fall outside the Retention Policy on the local host.

```sh
# Prune the datasets declared in the config file (client: datasets map; server: all allow lists)
zrb prune

# Prune specific datasets only
zrb prune tank/home tank/documents

# Preview what would be deleted without touching anything
zrb prune --dry-run

# Abort a stuck in-progress resume transfer and prune anyway
zrb prune --abort-resume
```

`--dry-run` prints each snapshot with its keep reason (`daily`, `weekly`, `monthly`, `yearly`) or a deletion marker.
Snapshots carrying a Transfer Hold (`zrb:*`) are shown with a ⏸ marker — they are protected from deletion until the
next successful send moves the hold. No ZFS mutations are made.

`--abort-resume` overrides the `resume_hold_days` guard on the Remote: it discards the pending resume token and prunes
regardless. Use when a partially-received transfer is no longer worth resuming.

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
User=<user>
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

    # Optional: prune the server's backup datasets on a schedule.
    # Targets the union of all clients' allow lists.
    prune.onCalendar = "weekly";
  };
}
```

The module creates the `zrb` system user, writes `/etc/zrb/main/server.toml`, and adds a `ForceCommand`-restricted entry
to the user's `authorized_keys` for each client that has a `publicKey` set. When `prune.onCalendar` is set, a
`zrb-server-prune-<name>` systemd service and timer are generated. You still need to grant ZFS permissions
imperatively.

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

The module creates the `zrb` system user, writes `/etc/zrb/client.toml`, and registers a `zrb-send-<name>` systemd
service+timer and a `zrb-prune` service+timer. The SSH key at `sshKey` must be provisioned separately (e.g. via
`sops-nix` or `agenix`).

### noxa SSH integration

If you use [noxa](https://github.com/0xCCF4/noxa) for SSH key lifecycle management, the optional `nixosModules.noxa`
module wires up the SSH layer automatically — no manual key generation, no pasting public keys into the server config,
no handwritten `ForceCommand` or manual SSH keys.

Import it instead of the client/server module and set `noxa.enable = true` on the remote:

```nix
{
  imports = [
    inputs.zrb.nixosModules.noxa
  ];

  services.zrb.client = {
    enable = true;
    sourceName = "my-laptop";

    remotes.primary = {
      # host defaults to the noxa SSH alias; set explicitly to override
      noxa = {
        enable = true;
        toNode = "backup-server";   # noxa node name of the Remote
        serverInstance = "main";    # matches services.zrb.server.<name> on the Remote
      };
    };

    datasets."tank/home".primary = "backup/laptop/home";
    
    retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
    jobs.daily = { onCalendar = "daily"; datasets = [ "tank/home" ]; };
  };
}
```

The module declares a noxa SSH grant named `zrb-<remoteName>` for each noxa-enabled remote. noxa then:

- generates the SSH keypair and distributes it via its secrets system
- writes a `ForceCommand`-restricted `authorized_keys` entry on the Remote
- configures the SSH client on the Source so `host` resolves correctly

The server-side zrb user is derived automatically from the Remote's NixOS config. Set `noxa.toUser` explicitly if you
need to override it.

On the Remote, set `noxa.enable = true` on the server instance to have the module auto-discover
clients from other nodes and populate their `allow` lists from the client's dataset mapping.
Add `publicKey` is not needed — noxa owns that `authorized_keys` entry.

```nix
services.zrb.server.main = {
  enable = true;
  noxa.enable = true;   # auto-populates clients from nodes
  retention = { recent = 14; weeklyForDays = 60; monthlyForDays = 730; };
};
```

Additional per-client config (e.g. `zfsReceiveOpts`) merges in via the NixOS module system — set
it alongside the auto-discovered entry:

```nix
services.zrb.server.main.clients.my-laptop = {
  # allow is populated automatically; add extra options here
  zfsReceiveOpts = [ "-c" ];
};
```

## Configuration reference

Full documentation of all config keys for both source and server: [docs/config.md](docs/config.md).

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

**SSH as the transport** — SSH provides an encrypted channel and key-based authentication. The pre-shared SSH key is
the only credential the client needs; no passwords, no tokens.

**`ForceCommand` invokes the remote `zrb` instance** — instead of opening a shell, the SSH key directly starts
`zrb server` on the Remote. The client never gets a shell; the connection is purpose-built for the backup protocol.

**`--client` binds a key to specific source hosts** — each `authorized_keys` entry declares which client names it may
serve. A compromised key cannot impersonate a client (identified by its public key) name that is not listed, limiting
the blast radius to the datasets that client is permitted to write.

**ZFS delegation is the hard boundary** — `zfs allow` enforces at the OS level which datasets the backup user may
receive into, regardless of what `zrb` does.

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