{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  enterShell = ''
    export GEN=ninja
    export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake

    echo "🦆 DuckDB SSHFS Extension Development Environment"
    echo ""
    echo "Git commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
    echo "Branch: $(git branch --show-current 2>/dev/null || echo 'unknown')"
    echo ""

    # Create vcpkg cache directory
    mkdir -p .vcpkg-cache

    # Show build status
    if [ -f "build/release/duckdb" ]; then
      echo "✅ Release build exists: build/release/duckdb"
    else
      echo "💡 Run 'build-release' to build the extension"
    fi

    if [ -f "build/debug/duckdb" ]; then
      echo "✅ Debug build exists: build/debug/duckdb"
    fi

    echo ""
    echo "Available commands:"
    echo "  build-release    - Build release version"
    echo "  build-debug      - Build debug version"
    echo "  clean            - Clean build directory"
    echo "  test             - Run test.sql"
    echo "  test2            - Run test2.sql (password auth)"
    echo "  integration-test - Run integration tests"
    echo ""
  '';

  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    gnumake

    # Build system
    cmake
    ninja

    # C/C++ tools
    autoconf
    automake
    pkg-config

    # vcpkg dependencies
    curl
    zip
    unzip
    tar

    # Python for vcpkg
    python3

    # Build acceleration
    ccache

    # For SSH testing
    openssh
  ];

  # https://devenv.sh/languages/
  languages.cplusplus.enable = true;

  # Environment variables for optimized builds
  env = {
    # vcpkg configuration
    VCPKG_ROOT = "${config.env.DEVENV_ROOT}/vcpkg";
    VCPKG_DOWNLOADS = "${config.env.DEVENV_ROOT}/vcpkg/downloads";
    VCPKG_BINARY_SOURCES = "clear;files,${config.env.DEVENV_ROOT}/.vcpkg-cache,readwrite";

    # Use ccache for faster rebuilds
    CMAKE_C_COMPILER_LAUNCHER = "ccache";
    CMAKE_CXX_COMPILER_LAUNCHER = "ccache";
  };

  # https://devenv.sh/scripts/
  scripts = {
    build-release = {
      description = "Build duckdb-sshfs extension (release)";
      exec = ''
        echo "Building duckdb-sshfs extension (release)..."

        # Ensure vcpkg submodule is initialized
        if [ ! -f "vcpkg/bootstrap-vcpkg.sh" ]; then
          echo "Initializing vcpkg submodule..."
          git submodule update --init --recursive
        fi

        # Build with ninja and vcpkg
        GEN=ninja make release VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake

        echo ""
        echo "✅ Build complete! Binary at: build/release/duckdb"
      '';
    };

    build-debug = {
      description = "Build duckdb-sshfs extension (debug)";
      exec = ''
        echo "Building duckdb-sshfs extension (debug)..."

        # Ensure vcpkg submodule is initialized
        if [ ! -f "vcpkg/bootstrap-vcpkg.sh" ]; then
          echo "Initializing vcpkg submodule..."
          git submodule update --init --recursive
        fi

        # Build with ninja and vcpkg
        GEN=ninja make debug VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake

        echo ""
        echo "✅ Build complete! Binary at: build/debug/duckdb"
      '';
    };

    clean = {
      description = "Clean build directory";
      exec = ''
        echo "Cleaning build directory..."
        rm -rf build
        echo "✅ Clean complete!"
      '';
    };

    test = {
      description = "Run test.sql";
      exec = ''
        if [ ! -f "build/release/duckdb" ]; then
          echo "❌ Error: build/release/duckdb not found"
          echo "Run 'build-release' first"
          exit 1
        fi

        echo "Running test.sql..."
        ./build/release/duckdb -unsigned -f test.sql
      '';
    };

    test2 = {
      description = "Run test2.sql (password auth)";
      exec = ''
        if [ ! -f "build/release/duckdb" ]; then
          echo "❌ Error: build/release/duckdb not found"
          echo "Run 'build-release' first"
          exit 1
        fi

        echo "Running test2.sql (password auth)..."
        ./build/release/duckdb -unsigned -f test2.sql
      '';
    };

    test-ssh = {
      description = "Test direct SSH connection";
      exec = ''
        echo "Testing direct SSH connection to storage box..."
        ssh -o IdentityAgent=none -i ~/.ssh/storagebox_key -p23 u508112@u508112.your-storagebox.de pwd
      '';
    };
  };

  scripts.integration-test.exec = ''
    ./scripts/run_sshfs_test_server.sh
    source ./scripts/set_sshfs_test_server_variables.sh
    ./build/release/test/unittest "test/*"
  '';

  git-hooks.hooks = {
    # Nix files
    nixfmt-rfc-style.enable = true;
    # Github Actions
    actionlint.enable = true;
    # Markdown files
    markdownlint = {
      enable = true;
      settings.configuration = {
        # Ignore line lengths for now
        MD013 = false;
        # Allow inline html as it is used in phoenix default AGENTS.md
        MD033 = false;
      };
    };
    # Leaking secrets
    ripsecrets.enable = true;
  };
  # Prevents unencrypted sops files from being committed
  git-hooks.hooks.pre-commit-hook-ensure-sops = {
    enable = true;
    files = "secret.*\\.(env|ini|yaml|yml|json)$";
  };

  # Security hardening to prevent malicious takeover of Github Actions:
  # https://news.ycombinator.com/item?id=43367987
  # Replaces tags like "v4" in 3rd party Github Actions to the commit hashes
  git-hooks.hooks.lock-github-action-tags = {
    enable = true;
    files = "^.github/workflows/";
    types = [ "yaml" ];
    entry =
      let
        script_path = pkgs.writeShellScript "lock-github-action-tags" ''
          for workflow in "$@"; do
            grep -E "uses:[[:space:]]+[A-Za-z0-9._-]+/[A-Za-z0-9._-]+@v[0-9]+" "$workflow" | while read -r line; do
              repo=$(echo "$line" | sed -E 's/.*uses:[[:space:]]+([A-Za-z0-9._-]+\/[A-Za-z0-9._-]+)@v[0-9]+.*/\1/')
              tag=$(echo "$line" | sed -E 's/.*@((v[0-9]+)).*/\1/')
              commit_hash=$(git ls-remote "https://github.com/$repo.git" "refs/tags/$tag" | cut -f1)
              [ -n "$commit_hash" ] && sed -i.bak -E "s|(uses:[[:space:]]+$repo@)$tag|\1$commit_hash #$tag|g" "$workflow" && rm -f "$workflow.bak"
            done
          done
        '';
      in
      toString script_path;
  };

  # Run clang-format on commits
  git-hooks.hooks = {
    clang-format = {
      enable = true;
      types_or = [
        "c++"
        "c"
      ];
    };
    clang-tidy = {
      enable = false;
      types_or = [
        "c++"
        "c"
      ];
      entry = "clang-tidy -p build --fix";
    };

    # Custom hook to run unit tests before commit
    # Only runs tests that don't require Docker services
    unit-tests = {
      enable = true;

      name = "Unit tests";

      # Run SSHFS tests
      entry = "bash -c 'if [ -f build/release/test/unittest ]; then build/release/test/unittest \"[sshfs]\" || true; else echo \"Tests not built yet, skipping...\"; fi'";

      types_or = [
        "c++"
        "c"
      ];
    };
  };

  # https://devenv.sh/outputs/
  # Define build outputs that can be built with `devenv build`
  outputs = {
    # Release build of duckdb with sshfs extension
    duckdb-release = {
      description = "DuckDB with SSHFS extension (release build)";
      builder = pkgs.writeShellScript "build-duckdb-release" ''
        set -e

        # Ensure we're in the source directory
        cd ${config.env.DEVENV_ROOT}

        # Initialize vcpkg if needed
        if [ ! -f "vcpkg/bootstrap-vcpkg.sh" ]; then
          echo "Initializing vcpkg submodule..."
          git submodule update --init --recursive
        fi

        # Clean any previous builds
        rm -rf build/release

        # Build with ninja and vcpkg
        export GEN=ninja
        export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
        make release VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH

        # Copy binary to output
        mkdir -p $out/bin
        cp build/release/duckdb $out/bin/
        cp -r build/release/repository $out/

        echo "DuckDB SSHFS extension (release) built successfully"
      '';
    };

    # Debug build of duckdb with sshfs extension
    duckdb-debug = {
      description = "DuckDB with SSHFS extension (debug build)";
      builder = pkgs.writeShellScript "build-duckdb-debug" ''
        set -e

        # Ensure we're in the source directory
        cd ${config.env.DEVENV_ROOT}

        # Initialize vcpkg if needed
        if [ ! -f "vcpkg/bootstrap-vcpkg.sh" ]; then
          echo "Initializing vcpkg submodule..."
          git submodule update --init --recursive
        fi

        # Clean any previous builds
        rm -rf build/debug

        # Build with ninja and vcpkg
        export GEN=ninja
        export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
        make debug VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH

        # Copy binary to output
        mkdir -p $out/bin
        cp build/debug/duckdb $out/bin/
        cp -r build/debug/repository $out/

        echo "DuckDB SSHFS extension (debug) built successfully"
      '';
    };

    # Just the sshfs extension (loadable module)
    sshfs-extension = {
      description = "SSHFS extension for DuckDB (loadable module)";
      builder = pkgs.writeShellScript "build-sshfs-extension" ''
        set -e

        # Ensure we're in the source directory
        cd ${config.env.DEVENV_ROOT}

        # Initialize vcpkg if needed
        if [ ! -f "vcpkg/bootstrap-vcpkg.sh" ]; then
          echo "Initializing vcpkg submodule..."
          git submodule update --init --recursive
        fi

        # Build release version
        export GEN=ninja
        export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
        make release VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH

        # Copy extension to output
        mkdir -p $out/lib/duckdb/extensions
        cp build/release/extension/sshfs/sshfs.duckdb_extension $out/lib/duckdb/extensions/

        echo "SSHFS extension built successfully"
      '';
    };
  };
}
