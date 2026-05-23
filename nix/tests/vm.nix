{ pkgs, nixosModules }:
pkgs.testers.nixosTest {
  name = "module-vm-tests";

  nodes.server = { ... }: {
    imports = [ nixosModules.server ];
    services.openssh.enable = true;
    services.zrb.server.backup = {
      enable = true;
      clients.myhost = {
        allow = [ "pool/home" ];
        publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI testkey";
      };
      retention = { recent = 7; weeklyForDays = 30; monthlyForDays = 365; };
    };
  };

  nodes.client = { ... }: {
    imports = [ nixosModules.client ];
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
  };

  nodes.clientNoPrune = { ... }: {
    imports = [ nixosModules.client ];
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
  };

  testScript = ''
    start_all()

    # Server: system user exists, config file present, authorized_keys has ForceCommand
    server.wait_for_unit("multi-user.target")
    server.succeed("id zrb")
    server.succeed("test -f /etc/zrb/backup/server.toml")
    server.succeed("grep -q 'zrb server --client myhost' /etc/ssh/authorized_keys.d/zrb")

    # Client: system user exists, config file present, send and prune timers enabled
    client.wait_for_unit("multi-user.target")
    client.succeed("id zrb")
    client.succeed("test -f /etc/zrb/client.toml")
    client.succeed("systemctl is-enabled zrb-send-hourly.timer")
    client.succeed("systemctl is-enabled zrb-prune.timer")

    # clientNoPrune: prune timer must be absent
    clientNoPrune.wait_for_unit("multi-user.target")
    clientNoPrune.fail("systemctl is-enabled zrb-prune.timer")
  '';
}
