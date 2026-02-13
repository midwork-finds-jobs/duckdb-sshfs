#pragma once

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include <condition_variable>
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

namespace duckdb {

struct SSHConnectionParams {
  std::string hostname;
  int port = 22;
  std::string username;
  std::string password;
  std::string key_path;
  std::string remote_path; // Path on the remote server

  // Authentication
  bool use_agent = false; // Explicitly use SSH agent for authentication

  // DuckDB logger (optional — no-op if null)
  shared_ptr<Logger> logger;

  // Crypto policy
  bool strict_crypto = false; // Restrict to non-NIST algorithms only

  // Connection tuning
  int timeout_seconds = 300; // 5 minutes for long uploads
  int max_retries = 3;       // Maximum connection retry attempts
  int initial_retry_delay_ms =
      1000; // Initial delay between retries (exponential backoff)
  int keepalive_interval = 60; // Send keepalive every 60 seconds (0 = disabled)

  // Upload performance tuning
  size_t chunk_size = 50 * 1024 * 1024; // 50MB default chunk size
  size_t max_concurrent_uploads = 2;    // Conservative for SFTP
};

class SSHClient {
public:
  explicit SSHClient(const SSHConnectionParams &params);
  ~SSHClient();

  // Connection management
  void Connect();
  void Disconnect();
  bool IsConnected() const { return connected; }
  bool ValidateConnection();
  LIBSSH2_SESSION *GetSession() const { return session; }

  // Capability detection
  bool SupportsCommands() const { return supports_commands; }
  void DetectCapabilities();

  // Command execution
  std::string ExecuteCommand(const std::string &command);

  // File operations
  void UploadChunk(const std::string &remote_path, const char *data,
                   size_t size, bool append = false);
  void RemoveFile(const std::string &remote_path);
  void RenameFile(const std::string &source_path,
                  const std::string &target_path);
  LIBSSH2_SFTP_ATTRIBUTES GetFileStats(const std::string &remote_path);

  // Read operations using dd
  size_t ReadBytes(const std::string &remote_path, char *buffer, size_t offset,
                   size_t length);

  // SFTP session pooling for efficient uploads
  LIBSSH2_SFTP *BorrowSFTPSession();
  void ReturnSFTPSession(LIBSSH2_SFTP *sftp);

  // SFTP-only operations (no SSH command execution)
  void CreateDirectorySFTP(const std::string &remote_path);
  void RemoveDirectorySFTP(const std::string &remote_path);
  void TruncateFileSFTP(const std::string &remote_path, int64_t new_size);
  size_t ReadBytesSFTP(const std::string &remote_path, char *buffer,
                       size_t offset, size_t length);

private:
  SSHConnectionParams params;
  int sock = -1;
  LIBSSH2_SESSION *session = nullptr;
  bool connected = false;
  bool supports_commands = false; // Auto-detected: can execute SSH commands
  bool dd_disabled =
      false;               // Disabled after channel failures (use SFTP instead)
  std::mutex upload_mutex; // Protect concurrent SFTP operations

  // SFTP session pool
  std::queue<LIBSSH2_SFTP *> sftp_pool;
  std::mutex pool_mutex;
  std::condition_variable pool_cv;
  size_t pool_size = 1; // Single SFTP session - reused across reads
  bool pool_initialized = false;

  void InitializeSession();
  void Authenticate();
  void CleanupSession();
  void InitializeSFTPPool();
  void CleanupSFTPPool();
};

} // namespace duckdb
