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

### Testing

```bash
# Run unit tests
./build/test/unittest
```

## Contributing

Contributions are welcome! This was created with Claude Code so using generative AI is most welcome.

## License

MIT License

## Related Projects

- [duckdb-webdav](https://github.com/yourusername/duckdb-webdav) - WebDAV extension for DuckDB
- [DuckDB](https://github.com/duckdb/duckdb) - The DuckDB database

## Support

For issues and questions, please open an issue on GitHub.
