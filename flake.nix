{
  description = "usagewindow — harness-agnostic usage-window and resume manager";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  inputs.cargo-dyndrv = {
    url = "github:obsidiansystems/cargo-dyndrv";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, ... }@inputs:
    let
      forAllSystems = f: nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] f;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ inputs.cargo-dyndrv.overlays.default ];
          };
          mkUsagewindow = { profile ? "release" }:
          pkgs.rustPlatform.buildRustPackage {
            pname = "usagewindow";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl pkgs.stdenv.cc ];
            cargoBuildType = profile;
            cargoBuildFlags = [ "--workspace" "--bins" ];
            installPhase =
              let
                # cargoBuildHook always passes an explicit --target, so
                # binaries land under target/<triple>/<profile>/, not
                # target/<profile>/.
                targetDir = "target/${pkgs.stdenv.hostPlatform.rust.cargoShortTarget}/${profile}";
              in ''
              runHook preInstall
              install -Dm755 ${targetDir}/uw $out/bin/uw
              install -Dm755 ${targetDir}/uw-daemon $out/bin/uw-daemon
              install -Dm755 ${targetDir}/uw-hook $out/bin/uw-hook
              install -Dm755 ${targetDir}/uw-mcp $out/bin/uw-mcp
              runHook postInstall
            '';
          };
        in {
          # Production and flakelet default: keep optimized release builds.
          default = mkUsagewindow { profile = "release"; };
          release = mkUsagewindow { profile = "release"; };
          debug = mkUsagewindow { profile = "debug"; };
        });

      # Deliberately nonstandard: evaluating this output requires the Nix daemon
      # features used by cargo-dyndrv, so it must not break ordinary flake checks
      # on hosts that have not opted into ca-derivations/dynamic-derivations.
      dynamicPackages = forAllSystems (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ inputs.cargo-dyndrv.overlays.default ];
          };
          dynamicBins = pkgs.buildDynamicCrate {
            pname = "usagewindow";
            version = "0.1.0";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ pkgs.openssl pkgs.stdenv.cc ];
            cargoBuildFlags = [ "--workspace" "--bins" ];
            outputs = [ "uw" "uw-daemon" "uw-hook" "uw-mcp" ];
          };
        in pkgs.symlinkJoin {
          name = "usagewindow-dynamic";
          paths = builtins.map (name: dynamicBins.${name}) [
            "uw"
            "uw-daemon"
            "uw-hook"
            "uw-mcp"
          ];
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
