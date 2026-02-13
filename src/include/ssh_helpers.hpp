#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include <chrono>
#include <libssh2.h>
#include <libssh2_sftp.h>
#include <sstream>
#include <string>

namespace duckdb {

// Custom log type for SSHFS extension — filterable via duckdb_logs() WHERE type
// = 'SSHFS'
class SSHFSLogType : public LogType {
public:
  static constexpr const char *NAME = "SSHFS";
  static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

  SSHFSLogType() : LogType(NAME, LEVEL) {}

  static string ConstructLogMessage(const string &msg) { return msg; }
};

// Log via DuckDB logger (no-op if logger is null)
#define SSHFS_LOG(LOGGER, msg)                                                 \
  do {                                                                         \
    if (LOGGER) {                                                              \
      if ((LOGGER)->ShouldLog(SSHFSLogType::NAME, SSHFSLogType::LEVEL)) {      \
        std::ostringstream oss_;                                               \
        oss_ << msg;                                                           \
        (LOGGER)->WriteLog(SSHFSLogType::NAME, SSHFSLogType::LEVEL,            \
                           oss_.str());                                        \
      }                                                                        \
    }                                                                          \
  } while (0)

// Shell-quote a string for safe use in SSH command arguments.
// Wraps in single quotes and escapes embedded single quotes: ' → '\''
inline std::string ShellQuote(const std::string &s) {
  std::string result = "'";
  for (char c : s) {
    if (c == '\'') {
      result += "'\\''";
    } else {
      result += c;
    }
  }
  result += "'";
  return result;
}

// RAII helper for timing operations with automatic logging
class ScopedTimer {
public:
  ScopedTimer(shared_ptr<Logger> logger, const std::string &tag,
              const std::string &description)
      : logger(std::move(logger)), tag(tag), description(description),
        start(std::chrono::steady_clock::now()) {}

  ~ScopedTimer() {
    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
    SSHFS_LOG(logger,
              "  [" << tag << "] " << description << ": " << ms << "ms");
  }

  // Get elapsed time without destroying the timer
  int64_t ElapsedMs() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
        .count();
  }

private:
  shared_ptr<Logger> logger;
  std::string tag;
  std::string description;
  std::chrono::steady_clock::time_point start;
};

// RAII helper for timing with throughput calculation
class ThroughputTimer {
public:
  ThroughputTimer(shared_ptr<Logger> logger, const std::string &tag,
                  const std::string &description, size_t bytes)
      : logger(std::move(logger)), tag(tag), description(description),
        bytes(bytes), start(std::chrono::steady_clock::now()) {}

  ~ThroughputTimer() {
    auto end = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
    double mb = bytes / (1024.0 * 1024.0);
    double mb_per_sec = ms > 0 ? (mb / (ms / 1000.0)) : 0;
    SSHFS_LOG(logger, "  [" << tag << "] " << description << " " << mb
                            << " MB: " << ms << "ms (" << mb_per_sec
                            << " MB/s)");
  }

private:
  shared_ptr<Logger> logger;
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
