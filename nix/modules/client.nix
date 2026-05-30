{ config, lib, pkgs, ... }:
with lib;
let
  cfg = config.services.zrb.client;
  toml = pkgs.formats.toml { };

  remoteSubmodule = {
    options = with types; {
      host = mkOption {
        type = str;
        description = "Hostname or IP address of the Remote.";
        example = "backup.example.com";
      };
      port = mkOption {
        type = nullOr port;
        default = null;
        description = "SSH port on the Remote. Null to let the SSH config file supply the port.";
        example = 2222;
      };
      user = mkOption {
        type = nullOr str;
        default = null;
        description = "SSH user on the Remote. Null to let the SSH config file supply the user.";
        example = "zrb";
      };
      sshKey = mkOption {
        type = nullOr str;
        default = null;
        description = "Path to SSH private key (managed externally). Null to rely on the SSH config file or agent.";
        example = "/etc/zrb/id_ed25519";
      };
      sshOpts = mkOption {
        type = listOf str;
        default = [ ];
        description = "Extra SSH options passed verbatim to the ssh command.";
        example = [ "-o" "StrictHostKeyChecking=yes" ];
      };
      zfsSendOpts = mkOption {
        type = listOf str;
        default = [ ];
        description = "Extra options passed to zfs send.";
        example = [ "-Lec" ];
      };
    };
  };

  jobSubmodule = {
    options = with types; {
      enable = mkOption {
        type = bool;
        default = true;
        description = "Whether to enable this job.";
      };
      onCalendar = mkOption {
        type = str;
        description = "systemd calendar expression.";
        example = "daily";
      };
      datasets = mkOption {
        type = listOf str;
        description = "Source datasets to send.";
        example = [ "tank/home" "tank/projects" ];
      };
      remotes = mkOption {
        type = listOf str;
        default = [ ];
        description = "Remotes to send to (empty = all).";
        example = [ "backup-server" ];
      };
      persistent = mkOption {
        type = bool;
        default = true;
        description = "Whether the systemd timer is persistent (catches up missed runs after downtime).";
        example = false;
      };
      watchdogSec = mkOption {
        type = nullOr str;
        default = "1m";
        description = "Watchdog timeout for the send service. If the transfer stalls for longer than this, systemd kills and restarts it. Null to disable. Should be set to at least twice the time needed to transfer one 4 MiB chunk.";
        example = "2h";
      };
    };
  };
in
{
  options.services.zrb.client = with types; {
    enable = mkEnableOption "zrb client";
    package = mkOption {
      type = package;
      default = pkgs.callPackage ../packages/zrb.nix { };
      defaultText = literalExpression "pkgs.callPackage ../packages/zrb.nix {}";
      description = "The zrb package to use.";
    };
    createUser = mkOption {
      type = bool;
      default = true;
      description = "Create the zrb system user and group.";
      example = false;
    };
    user = mkOption {
      type = str;
      default = "zrb";
      description = "User to run zrb services.";
      example = "zrb-backup";
    };
    group = mkOption {
      type = str;
      default = "zrb";
      description = "Group to own the config file.";
      example = "zrb-backup";
    };
    sourceName = mkOption {
      type = str;
      description = "Identifier for this source host.";
      example = "my-laptop";
    };
    retention = {
      recent = mkOption {
        type = int;
        description = "Recent snapshots to keep unconditionally.";
        example = 5;
      };
      weeklyForDays = mkOption {
        type = int;
        description = "Days back to keep one per week.";
        example = 30;
      };
      monthlyForDays = mkOption {
        type = int;
        description = "Days back to keep one per month.";
        example = 365;
      };
    };
    remotes = mkOption {
      type = attrsOf (submodule remoteSubmodule);
      default = { };
      description = "Named Remote configurations.";
      example = literalExpression ''
        {
          backup-server = {
            host = "backup.example.com";
            user = "zrb";
            # sshKey omitted — identity resolved from SSH config or agent
          };
        }
      '';
    };
    datasets = mkOption {
      type = attrsOf (attrsOf str);
      default = { };
      description = "datasets.<source-dataset>.<remote-name> = destination-dataset.";
      example = literalExpression ''
        {
          "tank/home" = { backup-server = "backup/laptop/home"; };
          "tank/projects" = { backup-server = "backup/laptop/projects"; };
        }
      '';
    };
    jobs = mkOption {
      type = attrsOf (submodule jobSubmodule);
      default = { };
      description = "Named send jobs with their schedule.";
      example = literalExpression ''
        {
          nightly = {
            onCalendar = "daily";
            datasets = [ "tank/home" "tank/projects" ];
          };
        }
      '';
    };
    prune = {
      onCalendar = mkOption {
        type = nullOr str;
        default = null;
        description = "systemd calendar expression for pruning, or null to disable.";
        example = "weekly";
      };
      persistent = mkOption {
        type = bool;
        default = true;
        description = "Whether the prune timer is persistent (catches up missed runs after downtime).";
        example = false;
      };
    };
  };

  config = mkIf cfg.enable {
    environment.etc."zrb/client.toml".source = toml.generate "zrb-client.toml" {
      source.name = cfg.sourceName;
      remotes = mapAttrs
        (_: r: {
          host = r.host;
          ssh_opts = r.sshOpts;
          zfs_send_opts = r.zfsSendOpts;
        }
        // optionalAttrs (r.port != null) { port = r.port; }
        // optionalAttrs (r.user != null) { user = r.user; }
        // optionalAttrs (r.sshKey != null) { ssh_key = r.sshKey; })
        cfg.remotes;
      datasets = cfg.datasets;
      retention = {
        recent = cfg.retention.recent;
        weekly_for_days = cfg.retention.weeklyForDays;
        monthly_for_days = cfg.retention.monthlyForDays;
      };
    };

    systemd.services = mkMerge [
      (mapAttrs'
        (name: job:
          nameValuePair "zrb-send-${name}" {
            description = "zrb send job '${name}'";
            after = [ "network-online.target" ];
            wants = [ "network-online.target" ];
            serviceConfig = {
              Type = "notify";
              User = cfg.user;
              ExecStart = concatStringsSep " " (
                [ "${cfg.package}/bin/zrb" "send" "--config" "/etc/zrb/client.toml" ]
                  ++ job.datasets
                  ++ concatMap (r: [ "--remote" r ]) job.remotes
              );
            } // optionalAttrs (job.watchdogSec != null) {
              WatchdogSec = job.watchdogSec;
            };
          }
        )
        (filterAttrs (_: data: data.enable) cfg.jobs))
      (mkIf (cfg.prune.onCalendar != null) {
        zrb-prune = {
          description = "zrb prune all";
          serviceConfig = {
            Type = "oneshot";
            User = cfg.user;
            ExecStart = "${cfg.package}/bin/zrb prune --config /etc/zrb/client.toml";
          };
        };
      })
    ];

    systemd.timers = mkMerge [
      (mapAttrs'
        (name: job:
          nameValuePair "zrb-send-${name}" {
            description = "Timer for zrb send job '${name}'";
            wantedBy = [ "timers.target" ];
            timerConfig = {
              OnCalendar = job.onCalendar;
              Persistent = job.persistent;
            };
          }
        )
        (filterAttrs (_: data: data.enable) cfg.jobs))
      (mkIf (cfg.prune.onCalendar != null) {
        zrb-prune = {
          description = "Timer for zrb prune all";
          wantedBy = [ "timers.target" ];
          timerConfig = {
            OnCalendar = cfg.prune.onCalendar;
            Persistent = cfg.prune.persistent;
          };
        };
      })
    ];

    users.users = mkIf cfg.createUser {
      ${cfg.user} = {
        isSystemUser = true;
        group = cfg.group;
      };
    };

    users.groups = mkIf cfg.createUser { ${cfg.group} = { }; };
  };
}
