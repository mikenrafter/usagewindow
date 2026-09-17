{
  description = "usagewindow — harness-agnostic usage-window and resume manager";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      forAllSystems = f: nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] f;
    in
    {
      devShells = forAllSystems (system:
        let pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              rustfmt
              clippy
              pkg-config
              openssl
              sqlite
              stdenv.cc
            ];
          };
        });

      # packages.${system}.default lands once the workspace produces a
      # meaningful top-level binary set (see docs/architecture.md, Phase 9).
    };
}
