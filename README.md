# DuckDB SSHFS Extension

A DuckDB extension that enables reading and writing files over SSH/SFTP with support for streaming multipart uploads.

Coded almost exclusively with Claude Code but tested manually

## Features

- **Streaming Uploads**: Write data directly from DuckDB to remote servers via SSH
- **Multipart Support**: Automatically chunks large files for efficient transfer
- **Persistent Connections**: Maintains SSH connections to minimize handshake overhead
- **Authentication**: Supports both SSH key and password authentication

## Installation

### Prerequisites

- DuckDB (development headers)
- CMake 3.5 or higher
- libssh2
- OpenSSL

### Build from Source

```bash
git clone https://github.com/yourusername/duckdb-sshfs.git
cd duckdb-sshfs
mkdir build && cd build
cmake ..
make
```

## Usage

### Basic Example

```sql
-- Load the extension
LOAD 'sshfs';

-- Create a secret for authentication
CREATE SECRET ssh_storage (
    TYPE SSH,
    USERNAME 'u123456',
    KEY_PATH '/Users/' || getenv('USER') || '/.ssh/storagebox_key',
    PORT 23
);

-- Write data to remote server
COPY (SELECT * FROM large_table)
TO 'sshfs://u123456.your-storagebox.de/data.csv';
```

### URL Format

```text
ssh://[username@]hostname[:port]/path/to/file
sshfs://[username@]hostname[:port]/path/to/file
```

Both `ssh://` and `sshfs://` prefixes are supported. Username is optional in the URL - it can be provided via a secret instead:

```sql
-- With username in URL
COPY data TO 'ssh://user@host/path/file.csv';

-- Without username in URL (provided via secret)
CREATE SECRET ssh_creds (TYPE SSH, USERNAME 'user', KEY_PATH '~/.ssh/id_rsa');
COPY data TO 'ssh://host/path/file.csv';
```

### Configuration Options

- `sshfs_chunk_size`: Size of each chunk in bytes (default: 50MB)
- `sshfs_timeout`: Connection timeout in seconds (default: 30)

## How It Works

1. **Data Generation**: DuckDB generates data and writes it to the SSHFS file handle
2. **Buffering**: Data accumulates in memory until reaching the chunk size threshold
3. **Chunk Upload**: Each chunk is uploaded to the server as a temporary file
4. **Assembly**: chunks are appended to each other with the `LIBSSH2_FXF_APPEND` on the server side.
5. **Cleanup**: Temporary chunk files are removed from the server

## Development

### Building with vcpkg

This extension uses vcpkg for dependency management (libssh2 and OpenSSL). The build process automatically handles vcpkg integration.

#### Development Prerequisites

- Git
- CMake 3.5+
- Ninja build system
- C++17 compatible compiler

#### Build Steps

1. **Clone the repository with submodules**

   ```bash
   git clone --recurse-submodules https://github.com/yourusername/duckdb-sshfs.git
   cd duckdb-sshfs
   ```

2. **Set up vcpkg**

   ```bash
   # Clone vcpkg (if not already done)
   git clone https://github.com/microsoft/vcpkg.git

   # Checkout the specific version used by DuckDB extension CI
   cd vcpkg
   git checkout 5e5d0e1cd7785623065e77eff011afdeec1a3574

   # Bootstrap vcpkg
   ./bootstrap-vcpkg.sh  # On macOS/Linux
   # OR
   .\bootstrap-vcpkg.bat  # On Windows

   cd ..
   ```

3. **Install dependencies**

   ```bash
   # vcpkg will automatically install libssh2 and openssl based on vcpkg.json
   ./vcpkg/vcpkg install --triplet=arm64-osx  # macOS ARM64
   # OR
   ./vcpkg/vcpkg install --triplet=x64-linux  # Linux x64
   # OR
   ./vcpkg/vcpkg install --triplet=x64-windows  # Windows x64
   ```

4. **Build the extension**

   ```bash
   # Set environment variables
   export GEN=ninja
   export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
   export VCPKG_TARGET_TRIPLET=arm64-osx  # Or x64-linux, x64-windows, etc.

   # Build release version
   make release

   # The built extension will be at:
   # build/release/extension/sshfs/sshfs.duckdb_extension
   ```

### Testing

#### Run all tests

```bash
make test
```

#### Run specific test files

```bash
./build/release/test/unittest 'test/sql/sshfs/*'
```

#### Run SSHFS integration tests with Docker

```bash
# Start SSH test server
./scripts/run_sshfs_test_server.sh

# Set test environment variables
export SSHFS_TEST_SERVER_AVAILABLE=1
export SSHFS_TEST_USERNAME=duckdb_sshfs_user
export SSHFS_TEST_PORT=2222

# Run tests
./build/release/test/unittest 'test/sql/sshfs/*'

# Stop test server
./scripts/stop_sshfs_test_server.sh
```

### vcpkg Dependencies

The extension requires the following packages (defined in `vcpkg.json`):

- **libssh2**: SSH2 protocol implementation
- **openssl**: Cryptography and SSL/TLS toolkit

These are automatically installed when building with the vcpkg toolchain.

## Contributing

Contributions are welcome! This was created with Claude Code so using generative AI is most welcome.

## License

MIT License

## Related Projects

- [duckdb-webdav](https://github.com/yourusername/duckdb-webdav) - WebDAV extension for DuckDB
- [DuckDB](https://github.com/duckdb/duckdb) - The DuckDB database

## Support

For issues and questions, please open an issue on GitHub.
