{ config, lib, pkgs, ... }:
with lib;
let
  cfg = config.services.zrb.server;
  toml = pkgs.formats.toml { };

  clientSubmodule = {
    options = with types; {
      allow = mkOption {
        type = listOf str;
        description = "Dataset paths this client may write.";
        example = [ "tank/backups/laptop" ];
      };
      zfsReceiveOpts = mkOption {
        type = listOf str;
        default = [ ];
        description = "Extra options passed to zfs receive.";
        example = [ "-o" "compression=lz4" ];
      };
      publicKey = mkOption {
        type = nullOr str;
        default = null;
        description = "SSH public key for this client. Null when key management is delegated to an external tool such as noxa.";
        example = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI... user@host";
      };
    };
  };

  instanceSubmodule = { name, config, ... }: {
    options = with types; {
      enable = mkEnableOption "zrb server instance '${name}'";
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
        description = "User to own the config file.";
        example = "zrb-backup";
      };
      group = mkOption {
        type = str;
        default = "zrb";
        description = "Group to own the config file.";
        example = "zrb-backup";
      };
      resumeHoldDays = mkOption {
        type = int;
        default = 3;
        description = "Days to hold a ZFS resume token before discarding.";
        example = 7;
      };
      retention = {
        recent = mkOption {
          type = int;
          description = "Number of recent snapshots to keep unconditionally.";
          example = 5;
        };
        weeklyForDays = mkOption {
          type = int;
          description = "Days back to keep one snapshot per week.";
          example = 30;
        };
        monthlyForDays = mkOption {
          type = int;
          description = "Days back to keep one snapshot per month.";
          example = 365;
        };
      };
      clients = mkOption {
        type = attrsOf (submodule clientSubmodule);
        default = { };
        description = "Client configurations for this server instance.";
        example = literalExpression ''
          {
            laptop = {
              publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI...";
              allow = [ "tank/backups/laptop" ];
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
        user = mkOption {
          type = str;
          default = "${config.user}-prune";
          defaultText = literalExpression ''"''${user}-prune"'';
          description = "User to run the prune service. Must have ZFS destroy and mount delegation on the backup datasets.";
          example = "zrb-prune";
        };
        group = mkOption {
          type = str;
          default = "${config.user}-prune";
          defaultText = literalExpression ''"''${user}-prune"'';
          description = "Group for the prune user.";
          example = "zrb-prune";
        };
        createUser = mkOption {
          type = bool;
          default = config.createUser;
          defaultText = literalExpression "createUser";
          description = "Create the prune system user and group. Mirrors createUser by default.";
          example = false;
        };
      };
    };
  };

  enabledInstances = filterAttrs (_: icfg: icfg.enable) cfg;
in
{
  options.services.zrb.server = mkOption {
    type = types.attrsOf (types.submodule instanceSubmodule);
    default = { };
    description = "Named zrb server instances.";
    example = literalExpression ''
      {
        main = {
          enable = true;
          retention = {
            recent = 5;
            weeklyForDays = 30;
            monthlyForDays = 365;
          };
          clients.laptop = {
            publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI...";
            allow = [ "tank/backups/laptop" ];
          };
        };
      }
    '';
  };

  config = {
    systemd.services = mkMerge (
      mapAttrsToList
        (name: icfg:
          mkIf (icfg.enable && icfg.prune.onCalendar != null) {
            "zrb-server-prune-${name}" = {
              description = "zrb prune for server instance '${name}'";
              serviceConfig = {
                Type = "oneshot";
                User = icfg.prune.user;
                ExecStart = "${icfg.package}/bin/zrb prune --config /etc/zrb/${name}/server.toml";
              };
            };
          }
        )
        enabledInstances
    );

    systemd.timers = mkMerge (
      mapAttrsToList
        (name: icfg:
          mkIf (icfg.enable && icfg.prune.onCalendar != null) {
            "zrb-server-prune-${name}" = {
              description = "Timer for zrb prune on server instance '${name}'";
              wantedBy = [ "timers.target" ];
              timerConfig = {
                OnCalendar = icfg.prune.onCalendar;
                Persistent = icfg.prune.persistent;
              };
            };
          }
        )
        enabledInstances
    );

    environment.etc = mapAttrs'
      (name: icfg:
        nameValuePair "zrb/${name}/server.toml" {
          source = toml.generate "zrb-server-${name}.toml" {
            server.resume_hold_days = icfg.resumeHoldDays;
            retention = {
              recent = icfg.retention.recent;
              weekly_for_days = icfg.retention.weeklyForDays;
              monthly_for_days = icfg.retention.monthlyForDays;
            };
            clients = mapAttrs
              (_: c: {
                allow = c.allow;
                zfs_receive_opts = c.zfsReceiveOpts;
              })
              icfg.clients;
          };
          user = icfg.user;
          group = icfg.group;
          mode = "0644";
        }
      )
      enabledInstances;

    users.users = mkMerge (
      mapAttrsToList
        (name: icfg:
          mkMerge [
            {
              ${icfg.user}.openssh.authorizedKeys.keys =
                mapAttrsToList
                  (clientName: clientCfg:
                    ''command="${icfg.package}/bin/zrb server --client ${clientName} --config /etc/zrb/${name}/server.toml",restrict ${clientCfg.publicKey}''
                  )
                  (filterAttrs (_: c: c.publicKey != null) icfg.clients);
            }
            (mkIf icfg.createUser {
              ${icfg.user} = {
                isSystemUser = true;
                group = icfg.group;
                useDefaultShell = true; # force command override command
              };
            })
            (mkIf icfg.prune.createUser {
              ${icfg.prune.user} = {
                isSystemUser = true;
                group = icfg.prune.group;
              };
            })
          ]
        )
        enabledInstances
    );

    users.groups = mkMerge (
      mapAttrsToList
        (_: icfg:
          mkMerge [
            (mkIf icfg.createUser { ${icfg.group} = { }; })
            (mkIf icfg.prune.createUser { ${icfg.prune.group} = { }; })
          ]
        )
        enabledInstances
    );
  };
}
