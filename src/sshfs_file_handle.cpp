#include "sshfs_file_handle.hpp"
#include "duckdb/common/exception.hpp"
#include "ssh_helpers.hpp"
#include "sshfs_filesystem.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

namespace duckdb {

// Global mutex to serialize all SFTP reads across all file handles
// Critical for servers with strict SFTP session limits (e.g., Hetzner)
// Ensures only one file handle reads at a time, sharing the single SFTP session
static std::mutex g_sftp_read_mutex;

// Global cache of SFTP file handles (one per file path)
// Allows multiple DuckDB file handles to share one SFTP file handle
// Key: file path, Value: {sftp_session, file_handle, read_mutex}
struct GlobalFileHandle {
  LIBSSH2_SFTP *sftp;
  LIBSSH2_SFTP_HANDLE *handle;
  std::shared_ptr<SSHClient> client;
  std::shared_ptr<std::mutex>
      read_mutex; // Protects concurrent reads on this handle
};
static std::unordered_map<std::string, GlobalFileHandle> g_file_handles;
static std::mutex g_file_handles_mutex;

SSHFSFileHandle::SSHFSFileHandle(FileSystem &file_system, std::string path,
                                 FileOpenFlags flags,
                                 std::shared_ptr<SSHClient> client,
                                 const SSHConnectionParams &params)
    : FileHandle(file_system, path, flags), path(params.remote_path),
      ssh_client(std::move(client)), connection_params(params),
      chunk_size(params.chunk_size),
      max_concurrent_uploads(params.max_concurrent_uploads) {
  // Initialize write buffer
  write_buffer.reserve(chunk_size);

  SSHFS_LOG("  [HANDLE] Created file handle for " << params.remote_path);
}

SSHFSFileHandle::~SSHFSFileHandle() {
  try {
    Close();
  } catch (...) {
    // Destructor should not throw
  }
}

void SSHFSFileHandle::Close() {
  auto close_start = std::chrono::steady_clock::now();

  // No read handle cleanup needed - we open/close per read operation

  if (!write_buffer.empty() || buffer_dirty) {
    try {
      auto flush_start = std::chrono::steady_clock::now();
      Flush();
      auto flush_end = std::chrono::steady_clock::now();
      auto flush_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          flush_end - flush_start)
                          .count();
      if (IsDebugLoggingEnabled()) {
        std::cerr << "[TIMING] Final Flush: " << flush_ms << "ms" << std::endl;
      }
    } catch (std::exception &e) {
      // Rethrow - no cleanup needed with direct append
      throw;
    }
  }

  // Wait for all async uploads to complete
  if (chunk_count > 0) {
    auto wait_start = std::chrono::steady_clock::now();
    size_t initial_uploads = uploads_in_progress.load();
    if (IsDebugLoggingEnabled()) {
      std::cerr << "[TIMING] Waiting for " << initial_uploads
                << " async uploads to complete..." << std::endl;
    }

    std::unique_lock<std::mutex> lock(buffers_lock);
    upload_complete_cv.wait(
        lock, [this]() { return uploads_in_progress.load() == 0; });

    auto wait_end = std::chrono::steady_clock::now();
    auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       wait_end - wait_start)
                       .count();
    if (IsDebugLoggingEnabled()) {
      std::cerr << "[TIMING] All uploads completed in " << wait_ms << "ms"
                << std::endl;
    }

    // Check for errors after all uploads complete
    CheckUploadErrors();
  }

  // No assembly or cleanup needed - chunks appended directly to final file

  auto close_end = std::chrono::steady_clock::now();
  auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      close_end - close_start)
                      .count();
  if (IsDebugLoggingEnabled()) {
    std::cerr << "[TIMING] Total Close: " << close_ms << "ms" << std::endl;
  }
}

int64_t SSHFSFileHandle::Write(void *buffer, int64_t nr_bytes) {
  if (nr_bytes <= 0) {
    return 0;
  }

  const char *data = static_cast<const char *>(buffer);
  size_t bytes_written = 0;

  while (bytes_written < static_cast<size_t>(nr_bytes)) {
    // Calculate how much we can write to current buffer
    size_t space_left = chunk_size - write_buffer.size();
    size_t to_write =
        std::min(space_left, static_cast<size_t>(nr_bytes) - bytes_written);

    // Append to buffer
    write_buffer.insert(write_buffer.end(), data + bytes_written,
                        data + bytes_written + to_write);
    buffer_dirty = true;
    bytes_written += to_write;

    // If buffer is full, flush it
    if (write_buffer.size() >= chunk_size) {
      FlushChunk();
    }
  }

  // Track total bytes written for progress reporting
  total_bytes_written += nr_bytes;

  return nr_bytes;
}

void SSHFSFileHandle::Flush() {
  if (!write_buffer.empty() && buffer_dirty) {
    FlushChunk();
  }
}

int64_t SSHFSFileHandle::Read(void *buffer, int64_t nr_bytes) {
  if (nr_bytes <= 0) {
    return 0;
  }

  if (!buffer) {
    throw IOException("SSHFSFileHandle::Read: buffer is null");
  }

  if (!ssh_client) {
    throw IOException("SSHFSFileHandle::Read: ssh_client is null");
  }

  auto read_start = std::chrono::steady_clock::now();

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [READ-REQUEST] DuckDB requesting " << nr_bytes
              << " bytes at position " << file_position << std::endl;
  }

  // Serialize ALL SFTP operations globally (critical for thread safety)
  // libssh2 SFTP sessions share the same SSH socket and are NOT thread-safe
  // We borrow single SFTP session, open file, read, close file, return session
  // - all within this lock
  std::lock_guard<std::mutex> lock(g_sftp_read_mutex);

  if (!ssh_client->IsConnected()) {
    ssh_client->Connect();
  }

  // Borrow the single SFTP session from pool (reused across all reads)
  // This is safe because we close the file handle immediately after reading
  auto sftp_borrow_start = std::chrono::steady_clock::now();
  LIBSSH2_SFTP *sftp = ssh_client->BorrowSFTPSession();
  auto sftp_borrow_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - sftp_borrow_start)
          .count();

  // Open file for this read
  auto file_open_start = std::chrono::steady_clock::now();
  LIBSSH2_SFTP_HANDLE *handle =
      libssh2_sftp_open(sftp, path.c_str(), LIBSSH2_FXF_READ, 0);
  if (!handle) {
    int sftp_error = libssh2_sftp_last_error(sftp);
    ssh_client->ReturnSFTPSession(sftp);
    throw IOException(
        "Failed to open remote file for reading: %s (SFTP error: %d)",
        path.c_str(), sftp_error);
  }
  auto file_open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - file_open_start)
                          .count();

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [READ-OPERATION] Opened file (borrow: " << sftp_borrow_ms
              << "ms, open: " << file_open_ms << "ms)" << std::endl;
  }

  // Seek to position
  libssh2_sftp_seek64(handle, file_position);

  // Read in chunks (64KB chunks)
  const size_t CHUNK_SIZE = 65536;
  size_t total_read = 0;
  char *buf = static_cast<char *>(buffer);

  while (total_read < static_cast<size_t>(nr_bytes)) {
    size_t bytes_remaining = static_cast<size_t>(nr_bytes) - total_read;
    size_t bytes_to_read = std::min(bytes_remaining, CHUNK_SIZE);

    ssize_t nread = libssh2_sftp_read(handle, buf + total_read, bytes_to_read);

    if (nread < 0) {
      int sftp_error = libssh2_sftp_last_error(sftp);
      if (IsDebugLoggingEnabled()) {
        std::cerr << "  [READ-ERROR] libssh2_sftp_read failed: " << nread
                  << ", SFTP error: " << sftp_error
                  << ", total_read so far: " << total_read << std::endl;
      }
      libssh2_sftp_close(handle);
      ssh_client->ReturnSFTPSession(sftp);
      throw IOException("Failed to read from SFTP file: %s (libssh2 error: "
                        "%zd, SFTP error: %d, read %zu/%lld bytes)",
                        path, nread, sftp_error, total_read, nr_bytes);
    } else if (nread == 0) {
      break; // EOF
    }

    total_read += nread;
  }

  // Close file and return session to pool
  libssh2_sftp_close(handle);
  ssh_client->ReturnSFTPSession(sftp);

  // Update file position
  file_position += total_read;

  auto read_end = std::chrono::steady_clock::now();
  auto read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     read_end - read_start)
                     .count();
  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [READ-COMPLETE] Read " << total_read << " bytes in "
              << read_ms << "ms (pooled session, closed file handle)"
              << std::endl;
  }

  return static_cast<int64_t>(total_read);
}

void SSHFSFileHandle::Seek(idx_t location) { file_position = location; }

void SSHFSFileHandle::FlushChunk() {
  if (write_buffer.empty()) {
    return;
  }

  auto chunk_start = std::chrono::steady_clock::now();

  // Check for previous upload errors
  CheckUploadErrors();

  // Create buffer object for async upload
  auto buffer = std::make_shared<SSHFSWriteBuffer>();
  buffer->part_no = chunk_count;
  buffer->data = std::move(write_buffer); // Move to avoid copy

  if (IsDebugLoggingEnabled()) {
    double mb_size = buffer->data.size() / (1024.0 * 1024.0);
    std::cerr << "[TIMING] FlushChunk #" << chunk_count << " (" << mb_size
              << " MB) - queueing for async "
              << (chunk_count == 0 ? "upload" : "append") << std::endl;
  }

  // Wait if too many uploads in progress
  {
    std::unique_lock<std::mutex> lock(buffers_lock);
    upload_complete_cv.wait(lock, [this]() {
      return uploads_in_progress < max_concurrent_uploads ||
             has_upload_error.load();
    });

    // Check for errors again after wait
    CheckUploadErrors();

    // Add buffer to active list
    write_buffers.push_back(buffer);
  }

  // Start async upload
  uploads_in_progress.fetch_add(1);
  UploadChunkAsync(buffer);

  // Reset for next chunk
  write_buffer.clear();
  buffer_dirty = false;
  chunk_count++;

  auto chunk_end = std::chrono::steady_clock::now();
  auto chunk_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      chunk_end - chunk_start)
                      .count();
  if (IsDebugLoggingEnabled()) {
    std::cerr << "[TIMING] FlushChunk #" << (chunk_count - 1) << " queued in "
              << chunk_ms << "ms (upload continues in background)" << std::endl;
  }
}

void SSHFSFileHandle::OpenForRead() {
  if (read_initialized) {
    return; // Already open
  }

  auto open_start = std::chrono::steady_clock::now();

  if (!ssh_client->IsConnected()) {
    ssh_client->Connect();
  }

  // Borrow SFTP session from pool (shared across file handles)
  // This allows multiple file handles to share one SFTP session efficiently
  // Critical for servers with strict session limits (e.g., Hetzner)
  auto sftp_init_start = std::chrono::steady_clock::now();
  read_sftp = ssh_client->BorrowSFTPSession();
  auto sftp_init_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - sftp_init_start)
                          .count();

  // Open file for reading (kept open for entire file handle lifetime)
  auto file_open_start = std::chrono::steady_clock::now();
  read_handle = libssh2_sftp_open(read_sftp, path.c_str(), LIBSSH2_FXF_READ, 0);
  if (!read_handle) {
    int sftp_error = libssh2_sftp_last_error(read_sftp);
    char *err_msg = nullptr;
    int session_error = libssh2_session_last_error(ssh_client->GetSession(),
                                                   &err_msg, nullptr, 0);
    libssh2_sftp_shutdown(read_sftp);
    read_sftp = nullptr;
    throw IOException("Failed to open remote file for reading: %s\n"
                      "  SFTP error: %d\n"
                      "  Session error: %d (%s)",
                      path.c_str(), sftp_error, session_error,
                      err_msg ? err_msg : "Unknown error");
  }
  auto file_open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - file_open_start)
                          .count();

  read_initialized = true;

  auto open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - open_start)
                     .count();

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [READ-INIT] SFTP init: " << sftp_init_ms
              << "ms, file open: " << file_open_ms << "ms, total: " << open_ms
              << "ms (CACHED for file lifetime)" << std::endl;
  }
}

void SSHFSFileHandle::CloseReadHandle() {
  if (!read_initialized) {
    return; // Nothing to close
  }

  if (read_handle) {
    libssh2_sftp_close(read_handle);
    read_handle = nullptr;
  }

  if (read_sftp) {
    // Return SFTP session to pool for reuse (shared across file handles)
    ssh_client->ReturnSFTPSession(read_sftp);
    read_sftp = nullptr;
  }

  read_initialized = false;
}

void SSHFSFileHandle::CheckUploadErrors() {
  if (has_upload_error.load()) {
    if (first_upload_error) {
      std::rethrow_exception(first_upload_error);
    }
    throw IOException("Upload error occurred but no exception was captured");
  }
}

void SSHFSFileHandle::UploadChunkAsync(
    std::shared_ptr<SSHFSWriteBuffer> buffer) {
  // Mark as uploading
  bool expected = false;
  if (!buffer->uploading.compare_exchange_strong(expected, true)) {
    return; // Already uploading
  }

  // Launch detached thread for upload
  std::thread upload_thread([this, buffer]() {
    auto upload_start = std::chrono::steady_clock::now();

    try {
      bool is_first_chunk = (buffer->part_no == 0);
      if (IsDebugLoggingEnabled()) {
        std::cerr << "  [ASYNC] Starting background "
                  << (is_first_chunk ? "upload" : "append") << " of chunk #"
                  << buffer->part_no << " ("
                  << buffer->data.size() / (1024.0 * 1024.0) << " MB)"
                  << std::endl;
      }

      // Upload directly to final file (append for chunks after first)
      ssh_client->UploadChunk(path, buffer->data.data(), buffer->data.size(),
                              !is_first_chunk);

      // Mark as successfully uploaded
      buffer->uploaded.store(true);
      chunks_uploaded.fetch_add(1);
      bytes_uploaded.fetch_add(buffer->data.size());

      auto upload_end = std::chrono::steady_clock::now();
      auto upload_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           upload_end - upload_start)
                           .count();
      if (IsDebugLoggingEnabled()) {
        double mb_per_sec =
            buffer->data.size() / (1024.0 * 1024.0) / (upload_ms / 1000.0);
        std::cerr << "  [ASYNC] Completed chunk #" << buffer->part_no << " in "
                  << upload_ms << "ms (" << mb_per_sec << " MB/s)" << std::endl;
      }

    } catch (...) {
      if (IsDebugLoggingEnabled()) {
        std::cerr << "  [ASYNC] ERROR uploading chunk #" << buffer->part_no
                  << std::endl;
      }

      // Capture first error
      bool expected_error = false;
      if (has_upload_error.compare_exchange_strong(expected_error, true)) {
        first_upload_error = std::current_exception();
      }
      buffer->upload_error = std::current_exception();
    }

    // Decrement in-progress counter and notify
    uploads_in_progress.fetch_sub(1);
    upload_complete_cv.notify_all();
  });

  upload_thread.detach();
}

LIBSSH2_SFTP_ATTRIBUTES SSHFSFileHandle::GetCachedFileStats() {
  // No caching - always fetch fresh stats per user request
  return ssh_client->GetFileStats(path);
}

idx_t SSHFSFileHandle::GetProgress() {
  // Return bytes uploaded + bytes in current write buffer
  // This provides accurate progress during both writing and uploading phases
  return bytes_uploaded.load() + write_buffer.size();
}

} // namespace duckdb
