{
  description = "usagewindow — harness-agnostic usage-window and resume manager";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { self, nixpkgs }:
    let
      forAllSystems = f: nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] f;
    in
    {
      packages = forAllSystems (system:
        let pkgs = import nixpkgs { inherit system; };
        in {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "usagewindow";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl pkgs.stdenv.cc ];
            cargoBuildFlags = [ "--workspace" "--bins" ];
            installPhase = ''
              runHook preInstall
              install -Dm755 target/release/uw $out/bin/uw
              install -Dm755 target/release/uw-daemon $out/bin/uw-daemon
              install -Dm755 target/release/uw-hook $out/bin/uw-hook
              install -Dm755 target/release/uw-mcp $out/bin/uw-mcp
              runHook postInstall
            '';
          };
        });

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
              stdenv.cc
            ];
          };
          # Prebuilt `uw`/`uw-daemon`/`uw-hook`/`uw-mcp` on PATH, no toolchain
          # or local `cargo build` required — just the finished binaries from
          # `packages.default`. Useful when you want to run the tool without
          # depending on a working Rust build in the shell itself.
          withBins = pkgs.mkShell {
            packages = [ self.packages.${system}.default ];
          };
        });

    };
}
