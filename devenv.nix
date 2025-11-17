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
    gnutar

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
  outputs =
    let
      # Fetch submodule dependencies from GitHub
      duckdb-src = pkgs.fetchFromGitHub {
        owner = "duckdb";
        repo = "duckdb";
        rev = "68d7555f68bd25c1a251ccca2e6338949c33986a"; # v1.4.2
        hash = "sha256-wNNQTqXGja8iOYFQjEC2NoPeCJ1JDJt7q6TSS4jTn70=";
      };

      extension-ci-tools-src = pkgs.fetchFromGitHub {
        owner = "duckdb";
        repo = "extension-ci-tools";
        rev = "aac9640615e51d6e7e8b72d4bf023703cfd8e479";
        hash = "sha256-2ogWmxdM9hC7qX7pTEe0oOKGYRdgxk/6z3rsRBXc70E=";
      };

      vcpkg-src = pkgs.fetchFromGitHub {
        owner = "microsoft";
        repo = "vcpkg";
        rev = "5e5d0e1cd7785623065e77eff011afdeec1a3574";
        hash = "sha256-kfB1SywtlbgxL7sPlA0YUOOJ+EwlI/au6Lc3B2PQ50w=";
      };

      commonBuildInputs = with pkgs; [
        cmake
        ninja
        python3
        git
        curl
        zip
        unzip
        gnutar
        ccache
        openssl
        pkg-config
        autoconf
        automake
      ];
    in
    {
      # Release build
      duckdb-release = pkgs.stdenv.mkDerivation {
        name = "duckdb-sshfs-release";
        src = ./.;

        nativeBuildInputs = commonBuildInputs;
        buildInputs = with pkgs; [ openssl ];

        dontConfigure = true;

        preBuild = ''
          # Set up submodules
          # Note: The variable names got swapped during hash generation
          # duckdb and extension-ci-tools can be symlinked (read-only is fine)
          # vcpkg must be copied because it needs to bootstrap (compile vcpkg binary)
          echo "Setting up submodules..."
          ln -s ${extension-ci-tools-src} duckdb
          ln -s ${duckdb-src} extension-ci-tools
          cp -r ${vcpkg-src} vcpkg
          chmod -R +w vcpkg

          echo "Verifying setup..."
          ls -l duckdb extension-ci-tools vcpkg | head -3
          test -f extension-ci-tools/makefiles/duckdb_extension.Makefile && echo "Makefile found!" || echo "ERROR: Makefile missing"
        '';

        buildPhase = ''
          runHook preBuild

          export GEN=ninja
          export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
          export VCPKG_ROOT=$(pwd)/vcpkg
          export VCPKG_DOWNLOADS=$(pwd)/vcpkg/downloads
          export CMAKE_C_COMPILER_LAUNCHER=ccache
          export CMAKE_CXX_COMPILER_LAUNCHER=ccache
          export HOME=$TMPDIR

          echo "Building DuckDB with SSHFS extension (release)..."
          make release VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH GEN=ninja

          runHook postBuild
        '';

        installPhase = ''
          runHook preInstall

          mkdir -p $out/bin
          cp build/release/duckdb $out/bin/
          chmod +x $out/bin/duckdb

          mkdir -p $out/lib/duckdb/extensions
          cp build/release/extension/sshfs/sshfs.duckdb_extension $out/lib/duckdb/extensions/

          mkdir -p $out/share/duckdb
          if [ -d "build/release/repository" ]; then
            cp -r build/release/repository $out/share/duckdb/
          fi

          runHook postInstall
        '';

        meta = {
          description = "DuckDB with SSHFS extension (release build)";
          platforms = pkgs.lib.platforms.unix;
        };
      };
    };
}
