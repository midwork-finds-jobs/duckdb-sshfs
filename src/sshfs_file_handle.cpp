#include "sshfs_file_handle.hpp"
#include "duckdb/common/exception.hpp"
#include "sshfs_filesystem.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>

namespace duckdb {

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

  // Close read handle if it was opened
  CloseReadHandle();

  if (!write_buffer.empty() || buffer_dirty) {
    try {
      auto flush_start = std::chrono::steady_clock::now();
      Flush();
      auto flush_end = std::chrono::steady_clock::now();
      auto flush_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          flush_end - flush_start)
                          .count();
      std::cerr << "[TIMING] Final Flush: " << flush_ms << "ms" << std::endl;
    } catch (std::exception &e) {
      // Rethrow - no cleanup needed with direct append
      throw;
    }
  }

  // Wait for all async uploads to complete
  if (chunk_count > 0) {
    auto wait_start = std::chrono::steady_clock::now();
    size_t initial_uploads = uploads_in_progress.load();
    std::cerr << "[TIMING] Waiting for " << initial_uploads
              << " async uploads to complete..." << std::endl;

    std::unique_lock<std::mutex> lock(buffers_lock);
    upload_complete_cv.wait(
        lock, [this]() { return uploads_in_progress.load() == 0; });

    auto wait_end = std::chrono::steady_clock::now();
    auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       wait_end - wait_start)
                       .count();
    std::cerr << "[TIMING] All uploads completed in " << wait_ms << "ms"
              << std::endl;

    // Check for errors after all uploads complete
    CheckUploadErrors();
  }

  // No assembly or cleanup needed - chunks appended directly to final file

  auto close_end = std::chrono::steady_clock::now();
  auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      close_end - close_start)
                      .count();
  std::cerr << "[TIMING] Total Close: " << close_ms << "ms" << std::endl;
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

  auto read_start = std::chrono::steady_clock::now();

  std::cerr << "  [READ-REQUEST] DuckDB requesting " << nr_bytes
            << " bytes at position " << file_position << std::endl;

  // Use SSH dd command for efficient range reads (like HTTP range requests)
  // This only transfers the exact bytes needed over the network
  size_t bytes_read =
      ssh_client->ReadBytes(path, static_cast<char *>(buffer), file_position,
                            static_cast<size_t>(nr_bytes));

  // Update file position
  file_position += bytes_read;

  auto read_end = std::chrono::steady_clock::now();
  auto read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     read_end - read_start)
                     .count();
  std::cerr << "  [READ-COMPLETE] Read " << bytes_read << " bytes in "
            << read_ms << "ms" << std::endl;

  return static_cast<int64_t>(bytes_read);
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

  double mb_size = buffer->data.size() / (1024.0 * 1024.0);
  std::cerr << "[TIMING] FlushChunk #" << chunk_count << " (" << mb_size
            << " MB) - queueing for async "
            << (chunk_count == 0 ? "upload" : "append") << std::endl;

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
  std::cerr << "[TIMING] FlushChunk #" << (chunk_count - 1) << " queued in "
            << chunk_ms << "ms (upload continues in background)" << std::endl;
}

void SSHFSFileHandle::OpenForRead() {
  if (read_initialized) {
    return; // Already open
  }

  auto open_start = std::chrono::steady_clock::now();

  if (!ssh_client->IsConnected()) {
    ssh_client->Connect();
  }

  // Initialize SFTP session (kept open for entire file handle lifetime)
  auto sftp_init_start = std::chrono::steady_clock::now();
  read_sftp = libssh2_sftp_init(ssh_client->GetSession());
  if (!read_sftp) {
    throw IOException("Failed to initialize SFTP session for reading");
  }
  auto sftp_init_end = std::chrono::steady_clock::now();
  auto sftp_init_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          sftp_init_end - sftp_init_start)
                          .count();

  // Open file for reading (kept open for entire file handle lifetime)
  auto file_open_start = std::chrono::steady_clock::now();
  read_handle = libssh2_sftp_open(read_sftp, path.c_str(), LIBSSH2_FXF_READ, 0);
  if (!read_handle) {
    libssh2_sftp_shutdown(read_sftp);
    read_sftp = nullptr;
    throw IOException("Failed to open remote file for reading: %s",
                      path.c_str());
  }
  auto file_open_end = std::chrono::steady_clock::now();
  auto file_open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          file_open_end - file_open_start)
                          .count();

  read_initialized = true;

  auto open_end = std::chrono::steady_clock::now();
  auto open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     open_end - open_start)
                     .count();

  std::cerr << "  [READ-INIT] SFTP init: " << sftp_init_ms
            << "ms, file open: " << file_open_ms << "ms, total: " << open_ms
            << "ms (CACHED for file lifetime)" << std::endl;
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
    libssh2_sftp_shutdown(read_sftp);
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
      std::cerr << "  [ASYNC] Starting background "
                << (is_first_chunk ? "upload" : "append") << " of chunk #"
                << buffer->part_no << " ("
                << buffer->data.size() / (1024.0 * 1024.0) << " MB)"
                << std::endl;

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
      double mb_per_sec =
          buffer->data.size() / (1024.0 * 1024.0) / (upload_ms / 1000.0);
      std::cerr << "  [ASYNC] Completed chunk #" << buffer->part_no << " in "
                << upload_ms << "ms (" << mb_per_sec << " MB/s)" << std::endl;

    } catch (...) {
      std::cerr << "  [ASYNC] ERROR uploading chunk #" << buffer->part_no
                << std::endl;

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
  if (!stats_cached) {
    std::cerr << "  [CACHE] First GetFileStats - caching for file lifetime"
              << std::endl;
    cached_file_stats = ssh_client->GetFileStats(path);
    stats_cached = true;
  } else {
    std::cerr << "  [CACHE] Using cached file stats (avoiding SFTP call)"
              << std::endl;
  }
  return cached_file_stats;
}

idx_t SSHFSFileHandle::GetProgress() {
  // Return bytes uploaded + bytes in current write buffer
  // This provides accurate progress during both writing and uploading phases
  return bytes_uploaded.load() + write_buffer.size();
}

} // namespace duckdb
