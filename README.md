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
INSTALL sshfs FROM community;
LOAD sshfs;

-- Create a secret for authentication with host alias and scope
CREATE SECRET ssh_storage (
    TYPE SSH,
    USERNAME 'u123456',
    KEY_PATH '/Users/' || getenv('USER') || '/.ssh/storagebox_key',
    -- Or use: PASSWORD 'password'
    PORT 23,
    HOST 'storagebox',
    HOSTNAME 'u123456.your-storagebox.de',
    SCOPE 'ssh://storagebox'
);

-- Write data to remote server using the host alias
COPY (SELECT * FROM large_table)
TO 'ssh://storagebox:~/data.csv';

-- Read data from remote server
SELECT * FROM 'ssh://storagebox:~/data.csv';
```

### URL Format

```text
ssh://[username@]hostname[:port]/path/to/file
sshfs://[username@]hostname[:port]/path/to/file
sftp://[username@]hostname[:port]/path/to/file
```

All three prefixes (`ssh://`, `sshfs://`, `sftp://`) are supported and behave identically. Username is optional in the URL - it can be provided via a secret instead:

```sql
-- With username in URL
COPY data TO 'ssh://user@host/path/file.csv';

-- Without username in URL (provided via secret)
CREATE SECRET ssh_creds (TYPE SSH, USERNAME 'user', KEY_PATH '~/.ssh/id_rsa');
COPY data TO 'ssh://host/path/file.csv';
```

### SSH Config Support

The extension automatically reads SSH config files (`~/.ssh/config` and `/etc/ssh/ssh_config`) to resolve host aliases and default connection parameters. This allows you to use familiar SSH aliases without creating DuckDB secrets.

#### Example SSH Config

Add to `~/.ssh/config`:

```text
Host storagebox
    HostName u123456.your-storagebox.de
    User u123456
    Port 23
    IdentityFile ~/.ssh/storagebox_key
```

#### Usage with SSH Config

```sql
LOAD sshfs;

-- Option 1: Use SSH config for everything (key-based auth)
-- No secret needed if SSH key is configured
SELECT * FROM 'sftp://storagebox/data.parquet';

-- Option 2: Use SSH config + secret for password auth
CREATE SECRET storagebox_pwd (
    TYPE SSH,
    PASSWORD 'your-password',
    SCOPE 'sftp://storagebox'
);

SELECT * FROM 'sftp://storagebox/data.parquet';

-- Option 3: Override SSH config settings with secret
CREATE SECRET storagebox_custom (
    TYPE SSH,
    PASSWORD 'your-password',
    PORT 22,  -- Override port from SSH config
    SCOPE 'sftp://storagebox'
);

SELECT * FROM 'sftp://storagebox/data.parquet';
```

#### Configuration Precedence

When resolving connection parameters, the extension follows this precedence (highest to lowest):

1. **URL parameters**: `sftp://user@host:2222/path`
2. **DuckDB secret**: `CREATE SECRET ... (USERNAME 'user', PORT 2222)`
3. **SSH config**: `~/.ssh/config` settings for the host alias
4. **Defaults**: Standard SSH defaults (port 22, etc.)

This means you can:

- Define common settings in SSH config
- Override specific values in secrets when needed
- Use URL parameters for one-off connections

#### Supported SSH Config Keywords

- `Host`: Alias pattern (supports wildcards like `*.example.com`)
- `HostName`: Actual hostname to connect to
- `User`: Username for authentication
- `Port`: SSH port number
- `IdentityFile`: Path to private key file
- `Include`: Include other SSH config files

### SSH Agent Support

The extension automatically uses SSH agent for authentication if the `SSH_AUTH_SOCK` environment variable is set. This allows you to use keys loaded in your SSH agent without specifying them in secrets or config files.

#### Authentication Priority

The extension tries authentication methods in this order:

1. **SSH Agent**: If `SSH_AUTH_SOCK` is set, tries all identities from the agent
2. **Key File**: If `KEY_PATH` is specified in secret or SSH config
3. **Password**: If `PASSWORD` is specified in secret

#### Example: SSH Agent Authentication

```bash
# Add your key to SSH agent
ssh-add ~/.ssh/storagebox_key

# Verify key is loaded
ssh-add -l
```

```sql
LOAD sshfs;

-- Option 1: SSH agent only (no secret needed if username in SSH config)
SELECT * FROM 'sftp://storagebox/data.parquet';

-- Option 2: SSH agent with username in secret
CREATE SECRET storagebox_agent (
    TYPE SSH,
    USERNAME 'u123456',
    SCOPE 'sftp://u123456.your-storagebox.de'
);

SELECT * FROM 'sftp://u123456.your-storagebox.de/data.parquet';
```

#### Benefits of SSH Agent

- **Security**: Keys remain in the agent; not exposed to applications
- **Convenience**: No need to specify key paths in every secret
- **Multiple identities**: Agent automatically tries all loaded keys
- **Centralized management**: Use `ssh-add` to manage keys for all applications

#### Troubleshooting

If SSH agent authentication isn't working:

```bash
# Check if SSH agent is running
echo $SSH_AUTH_SOCK

# List loaded identities
ssh-add -l

# Start SSH agent if needed (bash/zsh)
eval $(ssh-agent)

# Add your key
ssh-add ~/.ssh/your_key
```

### Configuration Options

- `sshfs_chunk_size`: Size of each chunk in bytes (default: 50MB)
- `sshfs_timeout`: Connection timeout in seconds (default: 30)

## Server Compatibility

The extension automatically detects and supports two types of SSH/SFTP servers:

### Full SSH Servers (with command execution)

Servers that support both SFTP protocol **and** SSH command execution (like `dd`, `mkdir -p`, `truncate`).

#### Example: Hetzner Storage Box on Port 23

```sql
CREATE SECRET hetzner_ssh (
    TYPE SSH,
    USERNAME 'u123456',
    PASSWORD 'your-password',
    PORT 23,  -- Full SSH support on port 23
    HOSTNAME 'u123456.your-storagebox.de'
);
```

**Operations:**

- ✅ Directory creation: Uses `mkdir -p` command (faster)
- ✅ File reads: Uses `dd` command for efficient random access (~115ms for small reads)
- ✅ File writes: Uses SFTP protocol
- ✅ File truncate: Uses `truncate` command
- ✅ Directory removal: Uses `rmdir` command

### SFTP-Only Servers (without command execution)

Servers that only support the SFTP protocol without SSH command execution capability.

#### Example: Hetzner Storage Box on Port 22

```sql
CREATE SECRET hetzner_sftp (
    TYPE SSH,
    USERNAME 'u123456',
    PASSWORD 'your-password',
    PORT 22,  -- SFTP-only on port 22
    HOSTNAME 'u123456.your-storagebox.de'
);
```

**Operations:**

- ✅ Directory creation: Uses SFTP `mkdir` recursively
- ✅ File reads: Uses SFTP `read` with seeking (~195ms for small reads)
- ✅ File writes: Uses SFTP protocol
- ✅ File truncate: Uses SFTP `fsetstat`
- ✅ Directory removal: Uses SFTP `rmdir`

### Automatic Detection

The extension automatically detects server capabilities when connecting:

```text
[DETECT] Server supports SSH command execution        # Full SSH server
[DETECT] Server does not support command execution     # SFTP-only server
```

**No configuration needed!** The extension automatically uses the most efficient method available for each server.

### Performance Comparison

For small file operations on Hetzner Storage Box:

| Operation | Full SSH (Port 23) | SFTP-Only (Port 22) | Difference |
|-----------|-------------------|---------------------|------------|
| Read 53 bytes | 115ms (dd command) | 195ms (SFTP read) | ~40% slower |
| Directory creation | Fast (mkdir -p) | Slightly slower (recursive SFTP mkdir) | Minimal |
| Large file reads/writes | Similar performance (both use SFTP) | Similar performance | Negligible |

**Recommendation:** Use port 23 (full SSH) for Hetzner Storage Boxes when available for better read performance. Port 22 (SFTP-only) works perfectly but reads are slightly slower.

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
