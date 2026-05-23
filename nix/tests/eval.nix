{ pkgs, lib, nixosModules, nixosSystem }:
let
  mkNixos = modules:
    (nixosSystem {
      inherit (pkgs) system;
      modules = modules ++ [
        {
          boot.loader.grub.enable = false;
          fileSystems."/" = { device = "none"; fsType = "tmpfs"; };
          system.stateVersion = "25.11";
        }
      ];
    }).config;

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
  ];
in
pkgs.runCommand "module-eval-tests" { } (builtins.deepSeq checks "touch $out\n")
