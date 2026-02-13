#define DUCKDB_EXTENSION_MAIN

#include "sshfs_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "ssh_helpers.hpp"
#include "ssh_secrets.hpp"
#include "sshfs_filesystem.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
  // Register SSHFS file system
  auto &db = loader.GetDatabaseInstance();
  auto &config = DBConfig::GetConfig(db);

  // Register custom SSHFS log type for structured logging
  // Users can query: SELECT * FROM duckdb_logs() WHERE type = 'SSHFS';
  auto &log_manager = db.GetLogManager();
  log_manager.RegisterLogType(make_uniq<SSHFSLogType>());

  // Register SSHFS-specific configuration settings
  config.AddExtensionOption("sshfs_timeout_seconds",
                            "Timeout in seconds for SSH operations (default: "
                            "300 = 5 minutes)",
                            LogicalType::BIGINT, Value::BIGINT(300));

  config.AddExtensionOption(
      "sshfs_max_retries",
      "Maximum number of connection retry attempts (default: 3)",
      LogicalType::BIGINT, Value::BIGINT(3));

  config.AddExtensionOption(
      "sshfs_initial_retry_delay_ms",
      "Initial delay in milliseconds between retries, with exponential backoff "
      "(default: 1000)",
      LogicalType::BIGINT, Value::BIGINT(1000));

  config.AddExtensionOption(
      "sshfs_chunk_size_mb",
      "Chunk size in MB for uploads (default: 50MB, larger chunks may improve "
      "throughput but use more memory)",
      LogicalType::BIGINT, Value::BIGINT(50));

  config.AddExtensionOption(
      "sshfs_max_concurrent_uploads",
      "Maximum number of concurrent chunk uploads (default: 2, higher values "
      "may improve speed but use more connections)",
      LogicalType::BIGINT, Value::BIGINT(2));

  config.AddExtensionOption(
      "ssh_keepalive",
      "SSH keepalive interval in seconds (default: 60, set to 0 to disable). "
      "Prevents idle connection timeouts and improves performance.",
      LogicalType::BIGINT, Value::BIGINT(60));

  config.AddExtensionOption(
      "sshfs_strict_crypto",
      "Restrict SSH to non-NIST algorithms only (curve25519, ed25519, DH "
      "group14+). Removes ecdh-sha2-nistp*, ecdsa-sha2-nistp*, and legacy "
      "algorithms (default: false)",
      LogicalType::BOOLEAN, Value(false));

  auto &fs = db.GetFileSystem();
  fs.RegisterSubSystem(make_uniq<SSHFSFileSystem>());

  // Register SSH secrets
  CreateSSHSecretFunctions::Register(loader);
}

void SshfsExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string SshfsExtension::Name() { return "sshfs"; }

std::string SshfsExtension::Version() const {
#ifdef EXT_VERSION_SSHFS
  return EXT_VERSION_SSHFS;
#else
  return "0.1.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sshfs, loader) { duckdb::LoadInternal(loader); }
}
