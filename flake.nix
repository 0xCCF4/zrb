{
  description = "zrb — ZFS remote backup tool";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];

      perSystem = { pkgs, lib, ... }: {
        packages.default = pkgs.callPackage ./nix/packages/zrb.nix { };

        devShells.default = pkgs.mkShell {
          packages = [ pkgs.rustc pkgs.cargo pkgs.clippy ];
        };

        checks = {
          module-eval-tests = pkgs.callPackage ./nix/tests/eval.nix {
            nixosModules = inputs.self.nixosModules;
            nixosSystem = inputs.nixpkgs.lib.nixosSystem;
          };
        } // lib.optionalAttrs pkgs.stdenv.isLinux {
          module-vm-tests = pkgs.callPackage ./nix/tests/vm.nix {
            nixosModules = inputs.self.nixosModules;
          };
        };
      };

      flake = {
        nixosModules.server = import ./nix/modules/server.nix;
        nixosModules.client = import ./nix/modules/client.nix;
        nixosModules.noxa = import ./nix/modules/noxa.nix;

        nixosConfigurations.test-server = inputs.nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            inputs.self.nixosModules.server
            {
              services.zrb.server.backup = {
                enable = true;
                clients.myhost = {
                  allow = [ "pool/home" ];
                  publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI test";
                };
                retention.recent = 7;
                retention.weeklyForDays = 30;
                retention.monthlyForDays = 365;
              };
              boot.loader.grub.enable = false;
              fileSystems."/" = { device = "none"; fsType = "tmpfs"; };
              system.stateVersion = "25.11";
            }
          ];
        };

        nixosConfigurations.test-client = inputs.nixpkgs.lib.nixosSystem {
          system = "x86_64-linux";
          modules = [
            inputs.self.nixosModules.client
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
                retention.recent = 7;
                retention.weeklyForDays = 30;
                retention.monthlyForDays = 365;
                jobs.daily = {
                  onCalendar = "daily";
                  datasets = [ "tank/home" ];
                };
              };
              boot.loader.grub.enable = false;
              fileSystems."/" = { device = "none"; fsType = "tmpfs"; };
              system.stateVersion = "25.11";
            }
          ];
        };
      };
    };
}
