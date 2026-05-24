{ config, lib, ... }@args:
with lib;
let
  cfg = config.services.zrb.client;
  nodes = args.nodes or { };
  nodeName = args.nodeName or null;
in
{
  imports = [ ./client.nix ./server.nix ];

  # Extend the remotes submodule with noxa client-side SSH options.
  options.services.zrb.client.remotes = mkOption {
    type = types.attrsOf (types.submodule ({ name, config, ... }: {
      options = with types; {
        noxa = {
          enable = mkEnableOption "noxa SSH integration for this remote";
          toNode = mkOption {
            type = str;
            description = "noxa node name of the Remote.";
            example = "backup-server";
          };
          toUser = mkOption {
            type = str;
            description = "zrb system user on the Remote. Derived from the Remote's own NixOS config by default.";
          };
          serverInstance = mkOption {
            type = str;
            description = "Name of the services.zrb.server instance on the Remote. Determines the config path /etc/zrb/<serverInstance>/server.toml.";
            example = "main";
          };
        };
      };
      config = mkIf config.noxa.enable {
        noxa.toUser = mkDefault
          nodes.${config.noxa.toNode}.configuration.services.zrb.server.${config.noxa.serverInstance}.user;
        host = mkDefault "zrb-${name}";
      };
    }));
  };

  # Extend the server instance submodule with a noxa auto-population toggle and
  # the discovery logic. Keeping both inside the submodule avoids the infinite
  # recursion that would arise from reading config.services.zrb.server in the
  # outer config section while also writing to it.
  options.services.zrb.server = mkOption {
    type = types.attrsOf (types.submodule ({ name, config, ... }: {
      options.noxa.enable = mkEnableOption "noxa client auto-population for server instance '${name}'";

      # When noxa.enable = true, scan every other node for zrb clients whose
      # noxa remote targets this node + instance, and populate clients.<sourceName>.allow
      # from their dataset mapping. Manual per-client config (e.g. zfsReceiveOpts)
      # merges in naturally via the NixOS submodule system.
      config = mkIf (config.noxa.enable && nodeName != null) {
        clients = listToAttrs (concatLists (mapAttrsToList (clientNodeName: clientNode:
          let
            clientEnabled = clientNode.configuration.services.zrb.client.enable or false;
            clientRemotes = clientNode.configuration.services.zrb.client.remotes or { };
            clientDatasets = clientNode.configuration.services.zrb.client.datasets or { };
            clientSourceName = clientNode.configuration.services.zrb.client.sourceName or "";
            matchingRemotes = filterAttrs (_: r:
              (r ? noxa) && r.noxa.enable
              && r.noxa.toNode == nodeName
              && r.noxa.serverInstance == name
            ) clientRemotes;
          in
          if clientNodeName == nodeName || !clientEnabled || matchingRemotes == { }
          then [ ]
          else
            mapAttrsToList (remoteName: _:
              nameValuePair clientSourceName {
                allow = mapAttrsToList (_: remoteMap: remoteMap.${remoteName})
                  (filterAttrs (_: remoteMap: remoteMap ? ${remoteName}) clientDatasets);
              }
            ) matchingRemotes
        ) nodes));
      };
    }));
  };

  config = mkIf cfg.enable {
    # Declare a noxa SSH grant for each noxa-enabled remote. noxa distributes
    # the ForceCommand authorized_keys entry to the Remote and the SSH client
    # config to this host.
    services.noxa.ssh.grants = mapAttrs' (remoteName: remoteCfg:
      nameValuePair "zrb-${remoteName}" {
        name = "zrb-${remoteName}";
        from = cfg.user;
        to = {
          node = remoteCfg.noxa.toNode;
          user = remoteCfg.noxa.toUser;
        };
        commands = { pkgs }: [
          "${pkgs.zrb}/bin/zrb server --client ${cfg.sourceName} --config /etc/zrb/${remoteCfg.noxa.serverInstance}/server.toml"
        ];
      }
    ) (filterAttrs (_: r: r.noxa.enable) cfg.remotes);
  };
}
