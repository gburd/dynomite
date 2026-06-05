{
  description = "Dynomite Rust port - reproducible dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        pythonEnv = pkgs.python3.withPackages (ps: with ps; [
          pyyaml
          requests
          pytest
        ]);
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            toolchain

            # Cargo helpers
            cargo-nextest
            cargo-fuzz
            cargo-criterion
            cargo-deny
            cargo-audit
            cargo-machete
            cargo-hack
            cargo-llvm-cov
            cargo-public-api

            # Docs
            mdbook

            # Build deps for native crates (quiche bundles its own BoringSSL)
            clang_18
            cmake
            perl
            pkg-config

            # Build deps for the Netflix dynomite C reference
            # (informational submodule under _/dynomite/, built
            # on demand via scripts/build_cref.sh for the
            # Stage 14 differential rig).
            autoconf
            automake
            libtool
            openssl
            openssl.dev

            # Networking + failure-injection tooling
            iproute2
            iputils
            inetutils
            netcat-gnu
            tcpdump

            # Datastores used in integration tests. Valkey is the
            # open-source Redis fork; it ships valkey-server /
            # valkey-cli / valkey-benchmark and speaks RESP.
            valkey
            memcached

            # Scripting / glue
            pythonEnv
            jq
            ripgrep
            fd
            git

            # Determinism / fault injection
            libfaketime
          ];

          shellHook = ''
            export RUST_BACKTRACE=1
            export DYNOMITE_C_REF="$PWD/_/dynomite"
            echo "dynomite dev shell ready"
            echo "  C reference at: $DYNOMITE_C_REF"
            echo "  run: scripts/check.sh"
          '';
        };
      });
}
