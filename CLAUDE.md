# duckdb sshfs

This is a duckdb extension to work with files over ssh. The goal is to maximize upload speeds with Hetzner Storage boxes which have only limited ssh connections available.

Your goal is to be able to stream small chunks of data as you go. You want to have ssh connections which stay alive so that you don't need to do the handshake everytime. Test if this is possible with either https or ssh

You have ssh keys in:

* /Users/onnimonni/.ssh/storagebox_key
* /Users/onnimonni/.ssh/storagebox_key.pub

You can login with:

```sh
ssh -o IdentityAgent=none -i ~/.ssh/storagebox_key -p23 u508112@u508112.your-storagebox.de
```

Or with password `reesh5beiYohth8z_WohX7ka7le7Mahqu`

## Available commands

You can list all available commands and server backends:

```sh
ssh -o IdentityAgent=none -i ~/.ssh/storagebox_key -p23 u508112@u508112.your-storagebox.de "help"
```

### Get more info on certain command

```sh
ssh -o IdentityAgent=none -i ~/.ssh/storagebox_key -p23 u508112@u508112.your-storagebox.de "dd --help"
```

### Testing performance

**IMPORTANT: You should run following commands and few times and check that sshfs is on par with the direct ssh command.**

```bash
# Check 10 first rows directly from server
time ssh -i ~/.ssh/storagebox_key -o IdentityAgent=none storagebox "head test_timing.csv"

# Check 10 first rows with sshfs
time ./build/release/duckdb -unsigned -c "
    LOAD sshfs;
    CREATE SECRET hetzner_ssh (
        TYPE SSH,
        USERNAME 'u508112',
        KEY_PATH '/Users/onnimonni/.ssh/storagebox_key',
        PORT 23,
        SCOPE 'sshfs://u508112.your-storagebox.de'
    );
    FROM 'sshfs://u508112.your-storagebox.de/test_timing.csv' LIMIT 10;
"
```

---

## Performance-Driven Development Methodology

### Core Principles

**Never ship performance regressions.** Every change must be validated against a performance baseline.

### Workflow

#### 1. Establish Baseline

```bash
# Run test 2-3 times to account for variance
time ./build/release/duckdb -unsigned -f test_create_remote_parquet.sql
```

Record the baseline time. For this project: **< 35s is acceptable, < 30s is excellent**.

#### 2. Identify Improvements

Analyze codebase for:

* Repeated patterns (refactoring opportunities)
* Performance bottlenecks (profiling data)
* Resource management issues (leaks, inefficiency)
* Missing features (user requests, parity with other systems)

Prioritize by:

1. **Impact**: How much improvement expected?
2. **Risk**: How likely to cause issues?
3. **Effort**: How long will it take?

#### 3. Implement One Change at a Time

**CRITICAL**: One improvement per commit cycle.

Why?

* Easy to identify what caused regression
* Simple to revert if needed
* Clear git history
* Isolated testing

#### 4. Test Performance Impact

```bash
# Build with changes
GEN=ninja make

# Test performance (run 2-3 times)
time ./build/release/duckdb -unsigned -f test_create_remote_parquet.sql 2>&1 | grep "Run Time" | tail -1

# Compare to baseline
```

**Decision matrix:**

* **Faster**: Commit immediately ✅
* **Same (within 5%)**: Commit if provides other benefits ✅
* **Slower (>5%)**: Investigate or revert ❌

#### 5. Document Results

In commit message, include:

```text
Performance: [baseline]s → [new]s ([%change])
```

Example:

```text
Performance improvement: 30.3s → 27.7s (8.6% faster)
```

#### 6. Commit with Context

Include in commit:

* What changed (implementation)
* Why it changed (motivation)
* Performance impact (numbers)
* How it works (brief explanation)

### Example: SFTP Session Pooling

#### 1. Baseline

```bash
time ./build/release/duckdb -unsigned -f test_create_remote_parquet.sql
30.318 total
```

Baseline: 30.3s

#### 2. Identified Issue

Each chunk upload creates new SFTP session: 80-150ms overhead × 3 chunks = ~400ms wasted

#### 3. Implemented

* Created session pool with 2 pre-initialized sessions
* Added BorrowSFTPSession() / ReturnSFTPSession()
* Thread-safe with mutex + condition_variable

#### 4. Tested

```bash
time ./build/release/duckdb -unsigned -f test_create_remote_parquet.sql
27.728 total
```

Result: 27.7s (8.6% faster) ✅

#### 5. Committed

```text
git commit -m "Add SFTP session pooling for 8.6% faster uploads"
```

### Anti-Patterns to Avoid

#### ❌ Batch Multiple Changes

```bash
# BAD: Can't tell which change caused regression
* Increase concurrent uploads
* Add session pooling
* Change chunk size
```

#### ❌ Skip Performance Testing

```bash
# BAD: Assume change is faster
git commit -m "Optimize uploads"
# Then discover it's actually slower!
```

#### ❌ Commit Regressions

```bash
# BAD: "We can fix it later"
30.3s → 47.5s committed
# Users experience slowdowns immediately
```

#### ❌ Unclear Metrics

```bash
# BAD: No numbers
git commit -m "Make it faster"
```

### Continuous Performance Tracking

#### Create Performance Tests

```sql
-- test_create_remote_parquet.sql
.timer on
LOAD sshfs;
CREATE SECRET ...;
COPY (...) TO 'sshfs://...parquet';
```

#### Automate Regression Detection

```bash
#!/bin/bash
# run_benchmark.sh
BASELINE=30.0
CURRENT=$(time ./build/release/duckdb -f test.sql 2>&1 | extract_time)

if (( $(echo "$CURRENT > $BASELINE * 1.05" | bc -l) )); then
  echo "❌ Performance regression: ${CURRENT}s > ${BASELINE}s"
  exit 1
fi
```

#### Track Progress

Keep a log:

```text
| Date       | Change                | Time   | Delta  |
|------------|----------------------|--------|---------|
| 2025-11-14 | Initial streaming    | 35.0s  | -47%    |
| 2025-11-14 | SFTP session pooling | 27.7s  | -8.6%   |
```

### Testing Checklist

Before committing any change:

* [ ] Baseline established
* [ ] Change implemented in isolation
* [ ] Build succeeds
* [ ] Tests pass
* [ ] Performance tested (2-3 runs)
* [ ] Impact documented
* [ ] No regression (or acceptable tradeoff)
* [ ] Commit message includes metrics

### Recovery from Regression

If regression discovered:

```bash
# 1. Identify the commit
git log --oneline

# 2. Revert it
git revert <commit-hash>

# 3. Investigate offline
git checkout -b investigate-issue
# Try different approach

# 4. Test again
time ./build/release/duckdb -f test.sql

# 5. Only merge if faster
```

### Summary

Measure → Change → Measure → Commit

Never skip the measurement steps. Performance regressions are bugs that affect every user.

---

## DuckDB SSHFS Extension - Technical Overview

### The Problem

#### Hetzner Storage Boxes Constraints

Hetzner Storage Boxes are cost-effective remote storage solutions with specific limitations:

1. **Limited Concurrent SSH Connections**: Storage boxes restrict the number of simultaneous SSH/SFTP sessions
2. **SSH Handshake Overhead**: Each new connection requires ~100-200ms for handshake and authentication
3. **SFTP Session Overhead**: Creating SFTP sessions on top of SSH adds 80-150ms per session
4. **Network Latency**: Remote storage operations are subject to network latency
5. **Connection Stability**: Temporary network issues can cause connection failures

#### The Goal

Enable DuckDB to efficiently read and write large datasets (parquet files, CSV exports) to Hetzner Storage Boxes while:

* Maximizing upload/download throughput
* Working within connection limits
* Maintaining reliability despite network issues
* Providing a seamless user experience

### The Solution: duckdb-sshfs Extension

This extension implements a custom DuckDB FileSystem that treats SSH/SFTP servers as first-class storage backends, accessible via `ssh://` or `sshfs://` URLs.

#### Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                      DuckDB Core                        │
│                    (COPY, SELECT)                       │
└───────────────────┬─────────────────────────────────────┘
                    │ FileSystem API
┌───────────────────▼─────────────────────────────────────┐
│              SSHFSFileSystem                            │
│  • URL parsing (ssh://[user@]host:port/path)           │
│  • Connection pooling (reuse SSH connections)           │
│  • Credential management (DuckDB secrets)               │
│  • Configuration (chunk size, timeouts, retries)        │
└───────────────────┬─────────────────────────────────────┘
                    │
        ┌───────────┴──────────┐
        │                      │
┌───────▼────────┐    ┌────────▼──────────┐
│  SSHClient     │    │ SSHFSFileHandle   │
│                │    │                   │
│  • SSH connect │    │  • Buffered I/O   │
│  • SFTP pool   │    │  • Async uploads  │
│  • Commands    │    │  • Progress track │
│  • Retry logic │    │  • Chunk assembly │
└───────┬────────┘    └────────┬──────────┘
        │                      │
        │     libssh2          │
┌───────▼──────────────────────▼──────────┐
│        SSH/SFTP Protocol                │
│  • Authentication (key/password)        │
│  • Command execution (dd, mkdir, etc)   │
│  • SFTP file operations                 │
└───────────────┬─────────────────────────┘
                │
┌───────────────▼─────────────────────────┐
│    Hetzner Storage Box                  │
│    u508112.your-storagebox.de:23        │
└─────────────────────────────────────────┘
```

#### Key Components

##### 1. SSHFSFileSystem (src/sshfs_filesystem.cpp)

**Responsibilities:**

* Parse `ssh://[user@]host[:port]/path` or `sshfs://[user@]host[:port]/path` URLs (username optional, can be in secret)
* Extract credentials from DuckDB secrets
* Maintain connection pool (one SSH client per host/user combination)
* Validate connections before reuse
* Implement DuckDB FileSystem interface

**Connection Pooling:**

```cpp
// Connection key: "user@host:port"
std::unordered_map<string, std::shared_ptr<SSHClient>> client_pool;
```

Benefits:

* Reuse expensive SSH connections across multiple file operations
* Automatic reconnection on dead connections (keepalive validation)
* Thread-safe access with mutex protection

##### 2. SSHClient (src/ssh_client.cpp)

**Responsibilities:**

* Establish and manage SSH connections
* SFTP session pooling (2 pre-initialized sessions)
* Execute remote commands (dd, mkdir, rmdir)
* Connection retry with exponential backoff
* File operations (upload, stat, rename, remove)

**SFTP Session Pool:**

```text
SSH Connection
    ├─ SFTP Session 1 ──┐
    ├─ SFTP Session 2 ──┼──> Pool (borrow/return)
    └─ (future sessions)─┘
```

Without pooling: Each upload creates new SFTP session (80-150ms overhead)
With pooling: Borrow pre-initialized session from pool (~0ms overhead)

**Retry Logic:**

```text
Attempt 1: Immediate
Attempt 2: Wait 1s  (initial_retry_delay_ms)
Attempt 3: Wait 2s  (exponential backoff)
Attempt 4: Wait 4s
```

Distinguishes between:

* **Transient errors**: Network timeouts, connection refused → Retry
* **Permanent errors**: Authentication failure, invalid credentials → Fail immediately

##### 3. SSHFSFileHandle (src/sshfs_file_handle.cpp)

**Responsibilities:**

* Buffer writes into chunks (default 50MB)
* Asynchronous chunk uploads (S3-style streaming)
* Direct SFTP append for subsequent chunks
* Progress tracking for long uploads
* Cached reads using dd command
* Error propagation from background threads

**Write Flow (S3-Style Streaming with SFTP Append):**

```text
DuckDB writes 100MB file (chunk_size=50MB, max_concurrent=2):

Time →
┌────────────────────────────────────────────────────────┐
│ DuckDB Thread                                          │
│  Write(25MB)  Write(25MB)  Write(25MB)  Write(25MB)   │
│       ↓           ↓            ↓            ↓          │
│   Buffer      Buffer       Buffer       Buffer         │
│    (50MB)     (full!)      (50MB)      (full!)         │
│                 ↓                          ↓            │
│              Flush                      Flush           │
│                 ↓                          ↓            │
│            UploadAsync                UploadAsync       │
└────────────────┼────────────────────────────┼──────────┘
                 │                            │
                 ▼                            ▼
┌──────────────────┐              ┌──────────────────────┐
│ Background       │              │ Background           │
│ Thread 1         │              │ Thread 2             │
│                  │              │                      │
│ SFTP create      │              │ (waits for slot)     │
│ final.parquet    │              │                      │
│ [████████████]   │              │                      │
│ ~20 seconds      │──(finishes)──→ SFTP append         │
└──────────────────┘              │ final.parquet        │
                                  │ [████████████]       │
                                  │ ~20 seconds          │
                                  └──────────────────────┘

After all uploads complete:
  * File is ready! No assembly or cleanup needed.
  * Chunks are appended directly to final file using LIBSSH2_FXF_APPEND flag
```

**Benefits:**

* DuckDB doesn't block on uploads
* Overlapped I/O (upload chunk N while generating chunk N+1)
* Memory efficient (only 2 × chunk_size in memory)
* Progress tracking (bytes_uploaded / total_bytes_written)

**Read Flow (dd-based):**

Traditional SFTP read is slow and requires keeping session open. Instead:

```text
Read 1KB at offset 1000000:
  SSH command: dd if=file.parquet bs=1 skip=1000000 count=1024 2>/dev/null

Benefits:
  * No persistent SFTP session needed
  * Server-side seeking (efficient for large files)
  * Works within command execution limits
```

For sequential reads, we cache the SFTP handle to avoid reopening.

#### Performance Optimizations

##### Timeline of Improvements

```text
Original (sequential SFTP):                    66s ████████████████████████████████

With streaming uploads:                        35s ████████████████
(overlapped I/O)                                   (-47% faster)

With SFTP session pooling:                     28s ████████████
(eliminate session init overhead)                  (-8.6% faster)

With retry logic + config + error messages:    26s ███████████
(network-dependent)                                (-60% from original)

With SFTP append (direct chunk upload):      25.7s ██████████
(eliminate assembly overhead)                      (-61% from original)
```

##### 1. S3-Style Streaming Multipart Uploads

**Before:**

```text
Generate chunk 1 (50MB) → Upload → Generate chunk 2 → Upload
         25s            +  20s   +      25s        +  20s  = 90s
```

**After:**

```text
Generate chunk 1 (50MB) ─┬─→ Upload (20s)
                         │
Generate chunk 2 (50MB) ─┴─→ Upload (20s)
         50s (overlapped) + 20s = 70s (or better with 2 parallel)
```

Commit: `2116990` - 47% faster

##### 2. SFTP Session Pooling

**Problem:** Each `UploadChunk()` call created new SFTP session:

```text
 UploadChunk1: SSH handshake (0ms, reuse) + SFTP init (100ms) + upload (20s)
 UploadChunk2: SSH handshake (0ms, reuse) + SFTP init (100ms) + upload (20s)
 UploadChunk3: SSH handshake (0ms, reuse) + SFTP init (100ms) + upload (20s)
Total overhead: 300ms
```

**Solution:** Pre-initialize 2 SFTP sessions matching `max_concurrent_uploads`:

```text
On Connect():
  * Create SFTP session 1
  * Create SFTP session 2
  * Add to pool

On UploadChunk():
  * Borrow session from pool (wait if none available)
  * Use for upload
  * Return to pool
```

**Result:** ~300ms saved (100ms × 3 chunks)

Commit: `7a82183` - 8.6% faster

##### 3. Connection Retry with Exponential Backoff

Handles transient network issues automatically:

```text
Try 1: Connect → [Network timeout]
Wait 1s (initial_retry_delay_ms)
Try 2: Connect → [Connection refused]
Wait 2s (exponential backoff)
Try 3: Connect → Success!
```

Special handling:

* Authentication errors: Fail immediately (not transient)
* Network errors: Retry with backoff

Commit: `10cf39a` - No performance impact (only on failure path)

##### 4. Configurable Parameters

Allows users to tune for their specific scenarios:

```sql
-- Fast network, large files
CREATE SECRET fast_network (
  TYPE ssh,
  username 'user',
  password 'pass',
  chunk_size 104857600,        -- 100MB chunks
  max_concurrent_uploads 3,     -- More parallelism
  timeout_seconds 900           -- 15 min for huge files
);

-- Slow/unreliable network
CREATE SECRET slow_network (
  TYPE ssh,
  username 'user',
  password 'pass',
  chunk_size 10485760,          -- 10MB chunks (fail less)
  max_concurrent_uploads 1,     -- Conservative
  max_retries 5,                -- More retries
  initial_retry_delay_ms 2000   -- Longer backoff
);
```

Commit: `9fe32d9` - No performance impact (configuration only)

##### 5. SFTP Append for Direct Chunk Upload

**Problem:** Previous implementation uploaded chunks to temporary files, then assembled them using SSH commands:

```text
1. Upload chunk_0.tmp (SFTP write)
2. Upload chunk_1.tmp (SFTP write)
3. Upload chunk_2.tmp (SFTP write)
4. cp chunk_0.tmp final.parquet    (SSH command, ~50ms)
5. dd if=chunk_1.tmp of=final.parquet oflag=append  (~100ms)
6. dd if=chunk_2.tmp of=final.parquet oflag=append  (~100ms)
7. rm chunk_*.tmp                   (SSH command, ~50ms)
Total assembly/cleanup overhead: ~300ms
```

**Solution:** Use SFTP's native append mode to write directly to final file:

```cpp
// First chunk: Create file
UploadChunk(path, data, size, append=false);
  → Opens with LIBSSH2_FXF_WRITE | LIBSSH2_FXF_CREAT | LIBSSH2_FXF_TRUNC

// Subsequent chunks: Append
UploadChunk(path, data, size, append=true);
  → Opens with LIBSSH2_FXF_WRITE | LIBSSH2_FXF_APPEND
```

**Benefits:**

* **Simpler architecture**: No temporary files, no assembly step, no cleanup
* **Fewer SSH operations**: Eliminated 4-6 SSH commands per upload
* **Cleaner SFTP usage**: Native protocol feature instead of shell commands
* **Code reduction**: Removed 96 lines of code (3 methods eliminated)

**Result:** ~0.3s saved on assembly/cleanup overhead

**Performance (114MB parquet, 3 chunks):**

* Before: ~26.0s (with dd assembly)
* After: ~25.7s (24.1s, 29.5s, 23.5s averaged)
* Improvement: 0.3s (1.2% faster)

Commit: `a889267` - 1.2% faster, cleaner code

#### Hetzner Storage Box Specifics

##### Connection Details

* **Hostname**: `u508112.your-storagebox.de`
* **Port**: 23 (non-standard SSH port)
* **Username**: `u508112`
* **Authentication**: SSH key (`~/.ssh/storagebox_key`) or password

##### Available Commands

From `ssh u508112@... help`:

* `ls`, `mkdir`, `rmdir`: Directory operations
* `dd`: Data copy (used for efficient reads)
* Other commands available but not currently used by extension

##### Command Usage in Extension

```bash
# Create directory (SSH command)
mkdir -p /path/to/dir

# Upload first chunk (SFTP create+truncate)
libssh2_sftp_open(path, LIBSSH2_FXF_WRITE | LIBSSH2_FXF_CREAT | LIBSSH2_FXF_TRUNC)
libssh2_sftp_write(handle, data, size)

# Upload subsequent chunks (SFTP append)
libssh2_sftp_open(path, LIBSSH2_FXF_WRITE | LIBSSH2_FXF_APPEND)
libssh2_sftp_write(handle, data, size)

# Read with seek (SSH dd command for efficient random access)
dd if=file.parquet bs=1 skip=OFFSET count=SIZE 2>/dev/null
```

##### Limitations Discovered

1. **Connection limit**: Storage boxes limit concurrent SSH connections (exact number unknown, conservative approach with 2 uploads)

2. **SFTP write preference**: While reads use `dd` for efficiency, writes use SFTP because:
   * SFTP provides atomic writes and native append mode
   * SFTP has better error handling and progress tracking
   * SFTP protocol is designed for file transfers (simpler than dd pipes)

3. **Command execution**: Each `ExecuteCommand()` opens a new SSH channel (cheap on existing connection, but avoided in hot path)

#### Configuration via DuckDB Secrets

The extension integrates with DuckDB's secret management:

```sql
-- Create secret
CREATE SECRET my_storage (
  TYPE ssh,
  username 'u508112',
  password 'reesh5beiYohth8z_WohX7ka7le7Mahqu',

  -- Optional tuning parameters
  timeout_seconds 600,           -- 10 min timeout
  max_retries 3,                 -- Retry 3 times
  initial_retry_delay_ms 1000,   -- Start with 1s delay
  chunk_size 52428800,           -- 50MB chunks
  max_concurrent_uploads 2       -- 2 parallel uploads
);

-- Use with COPY
COPY (SELECT * FROM large_table)
TO 'sshfs://u508112@u508112.your-storagebox.de:23/exports/data.parquet';

-- Use with FROM
SELECT * FROM 'sshfs://u508112@u508112.your-storagebox.de:23/data/input.parquet'
LIMIT 10;
```

#### Error Handling and User Experience

##### Enhanced Error Messages

Every error provides:

1. **Clear problem description**: What went wrong
2. **Error codes and details**: libssh2 error codes, errno values
3. **Actionable suggestions**: What to try next
4. **Example commands**: How to verify manually

Example:

```text
SSH authentication failed for u508112@u508112.your-storagebox.de:23
  → Public key authentication failed
    Key was rejected by server (invalid key or user)
    Key file: /Users/onnimonni/.ssh/storagebox_key
    Check: file exists, has correct permissions (chmod 600), and matches server's authorized_keys
    Try: ssh -i /Users/onnimonni/.ssh/storagebox_key -p 23 u508112@u508112.your-storagebox.de
  libssh2 error: Authentication failed (code: -18)
```

##### Debug Logging

Clean output by default, detailed logging when needed:

```bash
# Normal operation - silent
duckdb
> COPY (...) TO 'sshfs://...';

# Debug mode - detailed timing
SSHFS_DEBUG=1 duckdb
> COPY (...) TO 'sshfs://...';
[SFTP] Total upload: 20450ms
[SFTP] Create dirs: 15ms
[SFTP] Write data 50.0 MB: 20100ms (2.49 MB/s)
[POOL] Initializing SFTP session pool with 2 sessions...
```

#### Thread Safety

The extension handles concurrent access:

1. **SSHClient**:
   * `upload_mutex`: Protects SFTP operations (libssh2 sessions are NOT thread-safe)
   * `pool_mutex`: Protects SFTP session pool
   * `pool_cv`: Condition variable for waiting on available sessions

2. **SSHFSFileSystem**:
   * `pool_mutex`: Protects client connection pool

3. **SSHFSFileHandle**:
   * `buffers_lock`: Protects write buffer list
   * `upload_complete_cv`: Signals upload completion
   * Atomic counters: `uploads_in_progress`, `chunks_uploaded`, `bytes_uploaded`

#### Memory Management

RAII (Resource Acquisition Is Initialization) patterns throughout:

```cpp
// ssh_helpers.hpp
class SFTPSession {
  SFTPSession(session) {
    sftp = libssh2_sftp_init(session);
  }
  ~SFTPSession() {
    libssh2_sftp_shutdown(sftp);
  }
};

class ScopedTimer {
  ScopedTimer(tag, desc) {
    start = now();
  }
  ~ScopedTimer() {
    if (debug) log(elapsed());
  }
};
```

Benefits:

* No memory leaks (destructors clean up)
* Exception-safe (cleanup happens even on throw)
* Automatic resource management

#### Testing and Validation

Performance test (test_create_remote_parquet.sql):

```sql
.timer on
LOAD sshfs;

CREATE SECRET hetzner_ssh (
  TYPE ssh,
  username 'u508112',
  password 'reesh5beiYohth8z_WohX7ka7le7Mahqu'
);

-- Generate and upload data
COPY (
  SELECT
    i as id,
    'test_' || i as name,
    random() as value
  FROM range(1000000) t(i)
) TO 'sshfs://u508112@u508112.your-storagebox.de:23/test.parquet';

-- Verify by reading back
SELECT * FROM 'sshfs://u508112@u508112.your-storagebox.de:23/test.parquet'
LIMIT 10;
```

Target: < 35s (achieved: ~25.7s)

### Future Improvements

Potential areas for enhancement:

1. **Host Key Verification**: Currently skipped, should verify server identity
2. **SSH Agent Support**: Use ssh-agent for key management
3. **SSH Config File**: Read from ~/.ssh/config for defaults
4. **Compression**: Enable SSH compression for slow networks
5. **Parallel Reads**: Multiple dd commands for large sequential reads
6. **Adaptive Chunk Size**: Automatically adjust based on network speed
7. **Connection Keepalive**: More aggressive keepalive to prevent idle timeouts
8. **Bandwidth Limiting**: Rate limit uploads to avoid overwhelming network
9. **Resume Support**: Resume interrupted uploads
10. **Metrics/Telemetry**: Collect upload speeds, retry rates for optimization

### References

* **DuckDB Extension Template**: <https://github.com/duckdb/extension-template>
* **libssh2 Documentation**: <https://www.libssh2.org/>
* **Hetzner Storage Boxes**: <https://www.hetzner.com/storage/storage-box>
* **DuckDB Secrets**: <https://duckdb.org/docs/sql/statements/create_secret.html>

### License

This extension is developed for use with DuckDB and follows compatible licensing.
