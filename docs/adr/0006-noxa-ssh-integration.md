# ADR-0006: noxa SSH integration — client-side grant ownership

## Status

Accepted

## Context

Setting up zrb backup requires three manual SSH steps that are entirely outside the NixOS
module system: generating a keypair, placing the public key in the server config's
`clients.<name>.publicKey`, and ensuring the resulting `authorized_keys` entry has the correct
`ForceCommand` shape. Any mismatch silently breaks backup.

Users who already run [noxa](https://github.com/0xCCF4/noxa) for SSH key lifecycle management
have no way to hook zrb into that system. noxa can generate keypairs, distribute public keys,
and write `authorized_keys` entries with command restrictions — covering exactly what zrb needs.

The core design question was: which NixOS machine declares the noxa SSH grant?

**Option A — server-side**: The grant lives in the server's NixOS config. Each
`services.zrb.server.clients.<name>` entry gains a `noxa.fromNode` field pointing at the
Source. The server module knows the config path, client name, and package; it only needs the
Source's noxa node name.

**Option B — client-side**: The grant lives in the client's NixOS config. Each
`services.zrb.client.remotes.<name>` entry gains noxa fields pointing at the Remote. The
client module knows the sourceName and package; it needs the Remote's node name and server
instance name.

## Decision

**Option B (client-side).** The grant is declared in the client's NixOS config via the optional
`nix/modules/noxa.nix` module. Importing that module and setting `remotes.<name>.noxa.enable = true`
is sufficient to wire up the full SSH layer for that remote.

The `toUser` field (server-side zrb user) is derived from the Remote's own NixOS config via
`nodes.${toNode}.configuration.services.zrb.server.${serverInstance}.user`, removing the need
to repeat it. The client's `remotes.<name>.host` defaults to the noxa SSH alias (`"zrb-${remoteName}"`).

On the server side, `clients.<name>.publicKey` becomes `nullOr str` (default `null`). When
null, the server module writes no `authorized_keys` entry for that client; noxa owns it instead.

## Consequences

- **Single point of configuration**: enabling a backup remote is entirely expressed in the
  client's NixOS config. The server config only needs `services.zrb.server` with `allow` and
  retention — no key material.
- **noxa owns `authorized_keys` for noxa-integrated clients**: the server module never writes a
  `ForceCommand` entry for clients where `publicKey = null`. If noxa is not imported on the
  server, those clients will simply have no entry and SSH connections will be rejected.
- **Backward compatible**: existing configs with explicit `publicKey` values continue to work
  unchanged. noxa integration is opt-in per remote.
- **`nodes` required**: `noxa.nix` accesses `nodes.${toNode}.configuration` at eval time to
  derive `toUser`. This requires a multi-node evaluation context (deploy-rs, colmena, etc.).
  `toUser` can be set explicitly to bypass the `nodes` lookup if needed.
- **noxa module must be imported on both machines**: noxa distributes the `authorized_keys`
  effect to the server and the SSH config to the client. The grant declaration (in the client's
  config) must be evaluated by both NixOS configurations for both effects to take place.
