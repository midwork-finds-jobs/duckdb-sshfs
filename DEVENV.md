# Development Environment Setup with devenv

This project uses [devenv](https://devenv.sh/) to provide a reproducible development environment with all necessary build tools, including vcpkg, ninja, and C++ toolchain.

## Prerequisites

1. **Install Nix** (if not already installed):

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install
   ```

2. **Install devenv**:

   ```bash
   nix-env -iA devenv -f https://github.com/NixOS/nixpkgs/archive/nixpkgs-unstable.tar.gz
   ```

3. **(Optional) Install direnv** for automatic environment activation:

   ```bash
   nix-env -iA direnv -f https://github.com/NixOS/nixpkgs/archive/nixpkgs-unstable.tar.gz
   ```

## Quick Start

### Entering the Environment

```bash
# Enter the devenv shell
devenv shell

# The shell will show available commands and build status
```

### Building the Extension

```bash
# Build release version (recommended)
build-release

# Build debug version (for development)
build-debug

# Clean build directory
clean
```

### Running Tests

```bash
# Test direct SSH connection
test-ssh

# Run integration tests
integration-test
```

## Why `devenv build` Is Not Recommended

While `devenv build` outputs have been implemented, they are experimental and not recommended because:

1. **vcpkg bootstrap issues**: vcpkg needs to download packages and compile in the Nix sandbox, which has limitations
2. **Long build times**: Full DuckDB + extensions build can take 30+ minutes
3. **Complexity**: The build requires network access and dynamic compilation not suited for pure Nix derivations

**The recommended way to build is using the devenv shell scripts** (see "Building the Extension" section above), which work reliably and leverage caching.

## Build Artifacts

After running `build-release` or `build-debug`, the following artifacts are created:

1. **DuckDB binary** - Full DuckDB with SSHFS extension built-in
   - Release: `build/release/duckdb`
   - Debug: `build/debug/duckdb`

2. **SSHFS extension** - Loadable extension module
   - Release: `build/release/extension/sshfs/sshfs.duckdb_extension`
   - Debug: `build/debug/extension/sshfs/sshfs.duckdb_extension`

3. **Extension repository** - Local extension registry
   - Release: `build/release/repository/`
   - Debug: `build/debug/repository/`

## Automatic Environment with direnv

If you have direnv installed, the environment will activate automatically when you enter the directory:

1. Allow direnv for this directory:

   ```bash
   direnv allow
   ```

2. The environment will now activate automatically when you `cd` into the project directory.

## Environment Features

### Packages Included

- **Build System**: cmake, ninja, make
- **C++ Toolchain**: gcc/clang, autoconf, automake, pkg-config
- **vcpkg Dependencies**: curl, zip, unzip, tar, python3
- **Build Acceleration**: ccache
- **Development Tools**: git, openssh

### Environment Variables

- `GEN=ninja` - Use Ninja build system
- `VCPKG_TOOLCHAIN_PATH` - Path to vcpkg CMake toolchain
- `VCPKG_ROOT` - vcpkg installation directory
- `VCPKG_BINARY_SOURCES` - Local binary cache for faster rebuilds
- `CMAKE_C_COMPILER_LAUNCHER=ccache` - C compiler caching
- `CMAKE_CXX_COMPILER_LAUNCHER=ccache` - C++ compiler caching

### Git Hooks

The environment includes several pre-commit hooks:

- **clang-format** - Automatic C++ code formatting
- **nixfmt-rfc-style** - Format Nix files
- **actionlint** - Lint GitHub Actions workflows
- **markdownlint** - Lint Markdown files
- **ripsecrets** - Prevent committing secrets
- **lock-github-action-tags** - Security hardening for Actions
- **unit-tests** - Run SSHFS tests before commit

## vcpkg Integration

The environment is configured to use vcpkg for managing C++ dependencies:

- Dependencies are defined in `vcpkg.json`
- Binary cache is stored in `.vcpkg-cache/` for faster rebuilds
- vcpkg submodule is automatically initialized on first build

### Current Dependencies

- **openssl** - SSL/TLS support
- **libssh2** (with openssl and zlib features) - SSH protocol implementation

## Troubleshooting

### vcpkg Not Found

If vcpkg is not initialized:

```bash
git submodule update --init --recursive
```

### Build Cache

To clear the vcpkg binary cache:

```bash
rm -rf .vcpkg-cache
```

To clear the ccache:

```bash
ccache --clear
```

### Clean Rebuild

For a complete clean rebuild:

```bash
clean
build-release
```

## Performance Tips

1. **Use ccache**: Already configured automatically
2. **vcpkg binary cache**: Already configured in `.vcpkg-cache/`
3. **Parallel builds**: Ninja automatically uses all CPU cores
4. **Incremental builds**: Only changed files are recompiled

## Comparison to Manual Setup

### Without devenv

```bash
# Install dependencies manually
brew install cmake ninja pkg-config openssl libssh2

# Configure environment variables
export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig
export LDFLAGS="-L/opt/homebrew/lib"
export CPPFLAGS="-I/opt/homebrew/include"

# Build
make release
```

### With devenv

```bash
# Everything is automatically configured
devenv shell
build-release
```

## Benefits of devenv

1. **Reproducible**: Same build environment on any machine
2. **Isolated**: Doesn't interfere with system packages
3. **Declarative**: Environment defined in code (devenv.nix)
4. **Fast**: Binary cache and ccache for quick rebuilds
5. **Complete**: All dependencies included automatically
6. **Versioned**: Environment configuration is version controlled

## More Information

- [devenv Documentation](https://devenv.sh/)
- [Nix Package Manager](https://nixos.org/manual/nix/stable/)
- [vcpkg Documentation](https://vcpkg.io/)
