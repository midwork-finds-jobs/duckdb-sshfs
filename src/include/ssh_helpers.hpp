#pragma once

#include "duckdb/common/exception.hpp"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <string>

namespace duckdb {

// Thread-local debug flag (set from sshfs_debug_logging setting)
extern thread_local bool g_sshfs_debug_enabled;

// Simple logging configuration
// Checks both sshfs_debug_logging setting (via thread_local) and SSHFS_DEBUG
// env var (for backward compatibility)
inline bool IsDebugLoggingEnabled() {
  // Check thread-local flag first (from setting)
  if (g_sshfs_debug_enabled) {
    return true;
  }

  // Fall back to environment variable for backward compatibility
  static bool checked = false;
  static bool enabled = false;

  if (!checked) {
    const char *env = std::getenv("SSHFS_DEBUG");
    enabled = (env != nullptr && std::string(env) == "1");
    checked = true;
  }

  return enabled;
}

#define SSHFS_LOG(msg)                                                         \
  do {                                                                         \
    if (IsDebugLoggingEnabled()) {                                             \
      std::cerr << msg << std::endl;                                           \
    }                                                                          \
  } while (0)

// RAII helper for timing operations with automatic logging
class ScopedTimer {
public:
  ScopedTimer(const std::string &tag, const std::string &description)
      : tag(tag), description(description),
        start(std::chrono::steady_clock::now()) {}

  ~ScopedTimer() {
    if (IsDebugLoggingEnabled()) {
      auto end = std::chrono::steady_clock::now();
      auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
              .count();
      std::cerr << "  [" << tag << "] " << description << ": " << ms << "ms"
                << std::endl;
    }
  }

  // Get elapsed time without destroying the timer
  int64_t ElapsedMs() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
        .count();
  }

private:
  std::string tag;
  std::string description;
  std::chrono::steady_clock::time_point start;
};

// RAII helper for timing with throughput calculation
class ThroughputTimer {
public:
  ThroughputTimer(const std::string &tag, const std::string &description,
                  size_t bytes)
      : tag(tag), description(description), bytes(bytes),
        start(std::chrono::steady_clock::now()) {}

  ~ThroughputTimer() {
    if (IsDebugLoggingEnabled()) {
      auto end = std::chrono::steady_clock::now();
      auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
              .count();
      double mb = bytes / (1024.0 * 1024.0);
      double mb_per_sec = ms > 0 ? (mb / (ms / 1000.0)) : 0;
      std::cerr << "  [" << tag << "] " << description << " " << mb
                << " MB: " << ms << "ms (" << mb_per_sec << " MB/s)"
                << std::endl;
    }
  }

private:
  std::string tag;
  std::string description;
  size_t bytes;
  std::chrono::steady_clock::time_point start;
};

// RAII helper for SFTP session management
class SFTPSession {
public:
  SFTPSession(LIBSSH2_SESSION *session) : session(session), sftp(nullptr) {
    sftp = libssh2_sftp_init(session);
    if (!sftp) {
      throw IOException("Failed to initialize SFTP session");
    }
  }

  ~SFTPSession() {
    if (sftp) {
      libssh2_sftp_shutdown(sftp);
    }
  }

  // Non-copyable
  SFTPSession(const SFTPSession &) = delete;
  SFTPSession &operator=(const SFTPSession &) = delete;

  LIBSSH2_SFTP *Get() { return sftp; }

private:
  LIBSSH2_SESSION *session;
  LIBSSH2_SFTP *sftp;
};

} // namespace duckdb
