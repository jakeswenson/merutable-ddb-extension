{
  description = "merutable DuckDB extension dev shell";

  inputs = {
    nixpkgs.url     = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url    = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        # Pin to the rust-toolchain.toml in the merutable workspace.
        # The toolchain file is evaluated at flake eval time using an
        # absolute path; set MERUTABLE_WORKSPACE before entering the shell.
        merutableWorkspace = builtins.getEnv "MERUTABLE_WORKSPACE";
        toolchainFile =
          let candidate = "${merutableWorkspace}/rust-toolchain.toml";
          in if merutableWorkspace != "" && builtins.pathExists candidate
             then pkgs.rust-bin.fromRustupToolchainFile candidate
             else pkgs.rust-bin.stable.latest.default;

      in {
        devShells.default = pkgs.mkShell {
          name = "merutable-ddb-extension";

          packages = [
            pkgs.cmake
            pkgs.ninja
            pkgs.pkg-config
            pkgs.sccache
            toolchainFile
          ];

          # macOS frameworks: let the host linker find them via the SDK.
          # cmake and make will use the system clang (on $PATH from Xcode CLT)
          # so no Nix framework wrappers are needed.

          # MERUTABLE_WORKSPACE must be set in the environment before entering
          # the shell (e.g. via direnv or `just shell`). The fallback here is
          # a best-effort relative path hint only — it will be wrong if the
          # repos are not siblings on disk.
          env = pkgs.lib.optionalAttrs (merutableWorkspace != "") {
            MERUTABLE_WORKSPACE = merutableWorkspace;
          };

          shellHook = ''
            # Wire sccache into both C++ and Rust builds
            export SCCACHE_DIR="''${SCCACHE_DIR:-$HOME/.cache/sccache}"
            export CMAKE_C_COMPILER_LAUNCHER=sccache
            export CMAKE_CXX_COMPILER_LAUNCHER=sccache
            export RUSTC_WRAPPER=sccache
            echo "rustc:    $(rustc --version)"
            echo "cargo:    $(cargo --version)"
            echo "cmake:    $(cmake --version | head -1)"
            echo "sccache:  $(sccache --version)"
            echo "workspace: $MERUTABLE_WORKSPACE"
          '';
        };
      });
}
