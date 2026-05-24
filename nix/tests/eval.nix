{ pkgs, lib, nixosModules, nixosSystem }:
let
  baseModules = [
    {
      boot.loader.grub.enable = false;
      fileSystems."/" = { device = "none"; fsType = "tmpfs"; };
      system.stateVersion = "25.11";
    }
  ];

  mkNixos = modules:
    (nixosSystem {
      inherit (pkgs) system;
      modules = modules ++ baseModules;
    }).config;

  mkNixosWithArgs = specialArgs: modules:
    (nixosSystem {
      inherit (pkgs) system;
      inherit specialArgs;
      modules = modules ++ baseModules;
    }).config;

  # Minimal stub so eval tests can set ssh.grants without
  # importing the real noxa flake.
  stubNoxaModule = { lib, ... }: {
    options.ssh.grants = lib.mkOption {
      type = lib.types.attrsOf lib.types.anything;
      default = { };
    };
  };

  # Fake nodes used to test toUser derivation without a real multi-node eval.
  fakeNodes = {
    backup-server.configuration.services.zrb.server.main = {
      enable = true;
      user = "zrb-remote";
    };
  };

  # Fake nodes used to test server-side client auto-population.
  fakeNodesServer = {
    "my-laptop".configuration.services.zrb.client = {
      enable = true;
      sourceName = "my-laptop";
      remotes."backup-server".noxa = {
        enable = true;
        toNode = "backup-server";
        serverInstance = "main";
        toUser = "zrb";
      };
      datasets."tank/home"."backup-server" = "pool/home";
      datasets."tank/documents"."backup-server" = "pool/docs";
    };
  };

  # ── Server fixtures ────────────────────────────────────────────────────────

  serverCfg = mkNixos [
    nixosModules.server
    {
      services.zrb.server.backup = {
        enable = true;
        clients.myhost = {
          allow = [ "pool/home" ];
          publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI testkey";
        };
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
      };
    }
  ];

  serverTwoCfg = mkNixos [
    nixosModules.server
    {
      services.zrb.server.first = {
        enable = true;
        clients.alice = {
          allow = [ "pool/a" ];
          publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI key1";
        };
        retention = { recent = 3; weeklyForDays = 7; monthlyForDays = 30; };
      };
      services.zrb.server.second = {
        enable = true;
        clients.bob = {
          allow = [ "pool/b" ];
          publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI key2";
        };
        retention = { recent = 3; weeklyForDays = 7; monthlyForDays = 30; };
      };
    }
  ];

  serverNoUserCfg = mkNixos [
    nixosModules.server
    {
      services.zrb.server.backup = {
        enable = true;
        createUser = false;
        clients.myhost = {
          allow = [ "pool/home" ];
          publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI testkey";
        };
        retention = { recent = 3; weeklyForDays = 7; monthlyForDays = 30; };
      };
    }
  ];

  # ── Client fixtures ────────────────────────────────────────────────────────

  clientCfg = mkNixos [
    nixosModules.client
    {
      services.zrb.client = {
        enable = true;
        sourceName = "test-host";
        remotes.backup = {
          host = "backup.example.com";
          user = "zrb";
          sshKey = "/etc/zrb/id_ed25519";
        };
        datasets."tank/home".backup = "pool/home";
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
        jobs.hourly = { onCalendar = "hourly"; datasets = [ "tank/home" ]; };
      };
    }
  ];

  clientNoKeyCfg = mkNixos [
    nixosModules.client
    {
      services.zrb.client = {
        enable = true;
        sourceName = "test-host";
        remotes.backup = {
          host = "backup.example.com";
          user = "zrb";
          # sshKey omitted — relies on SSH config or agent
        };
        datasets."tank/home".backup = "pool/home";
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
        jobs.hourly = { onCalendar = "hourly"; datasets = [ "tank/home" ]; };
      };
    }
  ];

  clientSshConfigCfg = mkNixos [
    nixosModules.client
    {
      services.zrb.client = {
        enable = true;
        sourceName = "test-host";
        remotes.backup = {
          host = "backup.example.com";
          # port and user omitted — resolved from SSH config
        };
        datasets."tank/home".backup = "pool/home";
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
        jobs.hourly = { onCalendar = "hourly"; datasets = [ "tank/home" ]; };
      };
    }
  ];

  clientPruneCfg = mkNixos [
    nixosModules.client
    {
      services.zrb.client = {
        enable = true;
        sourceName = "test-host";
        remotes.backup = {
          host = "backup.example.com";
          user = "zrb";
          sshKey = "/etc/zrb/id_ed25519";
        };
        datasets."tank/home".backup = "pool/home";
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
        jobs.hourly = { onCalendar = "hourly"; datasets = [ "tank/home" ]; };
        prune.onCalendar = "weekly";
      };
    }
  ];

  # ── noxa fixtures ─────────────────────────────────────────────────────────

  # Server with noxa auto-population enabled; clients discovered from fakeNodesServer.
  serverNoxaDiscoveryCfg = mkNixosWithArgs { nodes = fakeNodesServer; nodeName = "backup-server"; } [
    nixosModules.noxa
    stubNoxaModule
    {
      services.zrb.server.main = {
        enable = true;
        noxa.enable = true;
        retention = { recent = 3; weeklyForDays = 7; monthlyForDays = 30; };
      };
    }
  ];

  # Server with a client that has no publicKey (noxa manages the key).
  serverNoxaClientCfg = mkNixos [
    nixosModules.server
    {
      services.zrb.server.main = {
        enable = true;
        clients.my-laptop = {
          allow = [ "pool/home" ];
          # publicKey omitted — noxa manages this key
        };
        retention = { recent = 3; weeklyForDays = 7; monthlyForDays = 30; };
      };
    }
  ];

  # Client with noxa integration enabled for one remote.
  # nixosModules.noxa already imports client.nix transitively.
  noxaCfg = mkNixosWithArgs { nodes = fakeNodes; } [
    nixosModules.noxa
    stubNoxaModule
    {
      services.zrb.client = {
        enable = true;
        sourceName = "my-laptop";
        remotes.backup-server = {
          noxa = {
            enable = true;
            toNode = "backup-server";
            serverInstance = "main";
          };
        };
        datasets."tank/home"."backup-server" = "pool/home";
        retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
        jobs.daily = { onCalendar = "daily"; datasets = [ "tank/home" ]; };
      };
    }
  ];

  # ── Assertions ─────────────────────────────────────────────────────────────

  serverAuthKeys = serverCfg.users.users.zrb.openssh.authorizedKeys.keys;

  checks = [
    # Server: TOML generated at expected path
    (lib.assertMsg
      (serverCfg.environment.etc ? "zrb/backup/server.toml")
      "server TOML not generated at environment.etc.\"zrb/backup/server.toml\"")

    # Server: authorized_keys entry contains ForceCommand pointing to zrb binary
    (lib.assertMsg
      (lib.any (lib.hasInfix "/bin/zrb server --client myhost") serverAuthKeys)
      "authorized_keys missing ForceCommand for client myhost")

    # Server: authorized_keys entry includes restrict
    (lib.assertMsg
      (lib.any (lib.hasInfix ",restrict ") serverAuthKeys)
      "authorized_keys entry missing \",restrict\" keyword")

    # Server: two instances produce distinct config paths
    (lib.assertMsg
      (serverTwoCfg.environment.etc ? "zrb/first/server.toml")
      "first server instance TOML not at zrb/first/server.toml")
    (lib.assertMsg
      (serverTwoCfg.environment.etc ? "zrb/second/server.toml")
      "second server instance TOML not at zrb/second/server.toml")

    # Server: createUser=false does not mark user as system user
    (lib.assertMsg
      (!serverNoUserCfg.users.users.zrb.isSystemUser)
      "createUser=false should not set isSystemUser=true on the zrb user")

    # Client: TOML generated at expected path
    (lib.assertMsg
      (clientCfg.environment.etc ? "zrb/client.toml")
      "client TOML not generated at environment.etc.\"zrb/client.toml\"")

    # Client: sshKey=null still generates TOML (ssh_key omitted from config)
    (lib.assertMsg
      (clientNoKeyCfg.environment.etc ? "zrb/client.toml")
      "client TOML not generated when sshKey is null")

    # Client: port=null and user=null still generates TOML (SSH config delegation)
    (lib.assertMsg
      (clientSshConfigCfg.environment.etc ? "zrb/client.toml")
      "client TOML not generated when port and user are null")

    # Client: jobs.hourly produces zrb-send-hourly service
    (lib.assertMsg
      (clientCfg.systemd.services ? "zrb-send-hourly")
      "zrb-send-hourly service not generated from jobs.hourly")

    # Client: zrb-send-hourly has After=network-online.target
    (lib.assertMsg
      (lib.elem "network-online.target" clientCfg.systemd.services."zrb-send-hourly".after)
      "zrb-send-hourly service missing After=network-online.target")

    # Client: jobs.hourly produces zrb-send-hourly timer
    (lib.assertMsg
      (clientCfg.systemd.timers ? "zrb-send-hourly")
      "zrb-send-hourly timer not generated from jobs.hourly")

    # Client: timer has correct OnCalendar
    (lib.assertMsg
      (clientCfg.systemd.timers."zrb-send-hourly".timerConfig.OnCalendar == "hourly")
      "zrb-send-hourly timer OnCalendar is not \"hourly\"")

    # Client: timer has Persistent=true
    (lib.assertMsg
      clientCfg.systemd.timers."zrb-send-hourly".timerConfig.Persistent
      "zrb-send-hourly timer Persistent is not true")

    # Client: prune.onCalendar=null produces no prune service
    (lib.assertMsg
      (!(clientCfg.systemd.services ? "zrb-prune"))
      "zrb-prune service must not exist when prune.onCalendar=null")

    # Client: prune.onCalendar=null produces no prune timer
    (lib.assertMsg
      (!(clientCfg.systemd.timers ? "zrb-prune"))
      "zrb-prune timer must not exist when prune.onCalendar=null")

    # Client: prune.onCalendar="weekly" produces prune service
    (lib.assertMsg
      (clientPruneCfg.systemd.services ? "zrb-prune")
      "zrb-prune service not generated when prune.onCalendar=\"weekly\"")

    # Client: prune service ExecStart contains "prune --all"
    (lib.assertMsg
      (lib.hasInfix "prune --all" clientPruneCfg.systemd.services."zrb-prune".serviceConfig.ExecStart)
      "zrb-prune ExecStart does not contain \"prune --all\"")

    # Client: prune.onCalendar="weekly" produces prune timer
    (lib.assertMsg
      (clientPruneCfg.systemd.timers ? "zrb-prune")
      "zrb-prune timer not generated when prune.onCalendar=\"weekly\"")

    # Server: client with publicKey=null produces no authorized_keys entry
    (lib.assertMsg
      (serverNoxaClientCfg.users.users.zrb.openssh.authorizedKeys.keys == [ ])
      "authorized_keys must be empty when all clients have publicKey=null")

    # noxa: grant declared with expected name
    (lib.assertMsg
      (noxaCfg.ssh.grants ? "zrb-backup-server")
      "noxa grant \"zrb-backup-server\" not declared")

    # noxa: grant from equals client user
    (lib.assertMsg
      (noxaCfg.ssh.grants."zrb-backup-server".from == "zrb")
      "noxa grant from does not equal client user \"zrb\"")

    # noxa: grant to.node equals toNode
    (lib.assertMsg
      (noxaCfg.ssh.grants."zrb-backup-server".to.node == "backup-server")
      "noxa grant to.node does not equal toNode \"backup-server\"")

    # noxa: grant to.user derived from nodes config
    (lib.assertMsg
      (noxaCfg.ssh.grants."zrb-backup-server".to.user == "zrb-remote")
      "noxa grant to.user not derived from nodes config")

    # noxa: remote host defaulted to grant name
    (lib.assertMsg
      (noxaCfg.services.zrb.client.remotes."backup-server".host == "zrb-backup-server")
      "noxa remote host not defaulted to grant name \"zrb-backup-server\"")

    # noxa server discovery: client auto-populated from other node's noxa config
    (lib.assertMsg
      (serverNoxaDiscoveryCfg.services.zrb.server.main.clients ? "my-laptop")
      "noxa server discovery: client my-laptop not auto-populated")

    # noxa server discovery: allow list derived from client dataset mapping
    (lib.assertMsg
      (lib.sort lib.lessThan
        serverNoxaDiscoveryCfg.services.zrb.server.main.clients."my-laptop".allow
        == [ "pool/docs" "pool/home" ])
      "noxa server discovery: allow list not derived from client datasets")

    # noxa server discovery: disabled instance does not auto-populate
    (lib.assertMsg
      (!(serverNoxaDiscoveryCfg.services.zrb.server ? "other"))
      "noxa server discovery: unexpected instance created")
  ];
in
pkgs.runCommand "module-eval-tests" { } (builtins.deepSeq checks "touch $out\n")
