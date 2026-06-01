{ symlinkJoin, makeWrapper, zrb, zfs, ssh }:
symlinkJoin {
  name = "zrb-with-zfs";
  paths = [ zrb ];
  nativeBuildInputs = [ makeWrapper ];
  postBuild = ''
    wrapProgram $out/bin/zrb \
      --prefix PATH : ${zfs}/bin \
      --prefix PATH : ${ssh}/bin
  '';
}
