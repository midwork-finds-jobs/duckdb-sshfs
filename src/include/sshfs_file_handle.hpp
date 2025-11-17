#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "ssh_client.hpp"
#include <atomic>
#include <condition_variable>
#include <exception>
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <memory>
#include <mutex>
#include <vector>

namespace duckdb {

class SSHFSFileSystem;

// Buffer for async chunk uploads (similar to S3WriteBuffer)
struct SSHFSWriteBuffer {
  size_t part_no;
  std::vector<char> data;
  std::atomic<bool> uploading{false};
  std::atomic<bool> uploaded{false};
  std::exception_ptr upload_error;
};

class SSHFSFileHandle : public FileHandle {
public:
  SSHFSFileHandle(FileSystem &file_system, std::string path,
                  FileOpenFlags flags, std::shared_ptr<SSHClient> client,
                  const SSHConnectionParams &params);
  ~SSHFSFileHandle() override;

  void Close() override;

  // Write operations
  int64_t Write(void *buffer, int64_t nr_bytes);
  void Flush();

  // Read operations
  int64_t Read(void *buffer, int64_t nr_bytes);

  // Seek operations
  void Seek(idx_t location);
  idx_t GetPosition() const { return file_position; }

  // Getters for stats
  std::shared_ptr<SSHClient> GetClient() const { return ssh_client; }
  const std::string &GetRemotePath() const { return path; }

  // Get cached file stats (initializes cache on first call)
  LIBSSH2_SFTP_ATTRIBUTES GetCachedFileStats();

  // Override GetProgress for better progress indication during uploads
  idx_t GetProgress() override;

private:
  std::string path;
  std::shared_ptr<SSHClient> ssh_client;
  SSHConnectionParams connection_params;

  // File position tracking
  idx_t file_position = 0;

  // Buffering for chunked writes
  std::vector<char> write_buffer;
  bool buffer_dirty = false;
  size_t chunk_size = 50 * 1024 * 1024; // 50MB default
  size_t chunk_count = 0;

  // Read handle caching - keep SFTP session open across reads
  LIBSSH2_SFTP *read_sftp = nullptr;
  LIBSSH2_SFTP_HANDLE *read_handle = nullptr;
  bool read_initialized = false;

  // Streaming upload members (async chunk uploads like S3)
  std::vector<std::shared_ptr<SSHFSWriteBuffer>> write_buffers;
  std::mutex buffers_lock;
  std::condition_variable upload_complete_cv;
  std::atomic<size_t> uploads_in_progress{0};
  std::atomic<size_t> chunks_uploaded{0};
  std::atomic<size_t> bytes_uploaded{
      0}; // Track bytes actually uploaded for progress
  std::atomic<bool> has_upload_error{false};
  std::exception_ptr first_upload_error;
  size_t max_concurrent_uploads = 2; // Conservative for SFTP
  size_t total_bytes_written =
      0; // Total bytes written by DuckDB (for progress)

  void FlushChunk();
  void OpenForRead();     // Lazily open SFTP and file handle
  void CloseReadHandle(); // Close read handle

  // Streaming upload methods
  void UploadChunkAsync(std::shared_ptr<SSHFSWriteBuffer> buffer);
  void CheckUploadErrors();
};

} // namespace duckdb
