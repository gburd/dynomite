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

            # Build deps for native crates (openssl, quiche)
            clang_18
            cmake
            perl
            pkg-config
            openssl

            # Networking + failure-injection tooling
            iproute2
            iputils
            inetutils
            netcat-gnu
            tcpdump

            # Datastores used in integration tests
            redis
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
            export OPENSSL_NO_VENDOR=1
            echo "dynomite dev shell ready"
            echo "  C reference at: $DYNOMITE_C_REF"
            echo "  run: scripts/check.sh"
          '';
        };
      });
}
