{ ... }:
{
  nixpkgs.overlays = [
    (final: _prev: {
      zrb = final.callPackage ../packages/zrb.nix { };
    })
  ];
}
