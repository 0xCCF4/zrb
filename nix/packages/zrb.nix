{ rustPlatform, installShellFiles }:
rustPlatform.buildRustPackage {
  pname = "zrb";
  version = (builtins.fromTOML (builtins.readFile ../../Cargo.toml)).package.version;
  src = ../..;
  cargoLock.lockFile = ../../Cargo.lock;

  nativeBuildInputs = [ installShellFiles ];

  postInstall = ''
    installShellCompletion --cmd zrb \
      --bash <($out/bin/zrb completions bash) \
      --zsh  <($out/bin/zrb completions zsh)  \
      --fish <($out/bin/zrb completions fish)

    mkdir -p $out/share/zrb/completions
    $out/bin/zrb completions nushell > $out/share/zrb/completions/zrb.nu
    $out/bin/zrb completions elvish  > $out/share/zrb/completions/zrb.elv

    $out/bin/zrb man > zrb.1
    installManPage zrb.1
  '';
}
