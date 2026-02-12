#include "sshfs_filesystem.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "ssh_config.hpp"
#include "ssh_helpers.hpp"
#include "sshfs_file_handle.hpp"
#include <ctime>
#include <regex>

namespace duckdb {

SSHFSFileSystem::SSHFSFileSystem() {}

unique_ptr<FileHandle>
SSHFSFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                          optional_ptr<FileOpener> opener) {
  // Parse URL and get connection parameters
  auto params = ParseURL(path, opener.get());

  // Get or create SSH client
  auto client = GetOrCreateClient(path, opener.get());

  // Ensure connection
  if (!client->IsConnected()) {
    client->Connect();
  }

  // Create and return file handle
  return make_uniq<SSHFSFileHandle>(*this, path, flags, client, params);
}

void SSHFSFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes,
                            idx_t location) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  sshfs_handle.Write(buffer, nr_bytes);
}

int64_t SSHFSFileSystem::Write(FileHandle &handle, void *buffer,
                               int64_t nr_bytes) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  return sshfs_handle.Write(buffer, nr_bytes);
}

int64_t SSHFSFileSystem::Read(FileHandle &handle, void *buffer,
                              int64_t nr_bytes) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  return sshfs_handle.Read(buffer, nr_bytes);
}

void SSHFSFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                           idx_t location) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  sshfs_handle.Seek(location);
  sshfs_handle.Read(buffer, nr_bytes);
}

void SSHFSFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  sshfs_handle.Seek(location);
}

void SSHFSFileSystem::Reset(FileHandle &handle) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  sshfs_handle.Seek(0);
}

idx_t SSHFSFileSystem::SeekPosition(FileHandle &handle) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  return sshfs_handle.GetPosition();
}

void SSHFSFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  auto client = sshfs_handle.GetClient();
  const auto &remote_path = sshfs_handle.GetRemotePath();

  if (!client->IsConnected()) {
    client->Connect();
  }

  // Flush any pending writes before truncating
  sshfs_handle.Flush();

  // Always use SFTP for truncate (avoids command injection via remote_path)
  client->TruncateFileSFTP(remote_path, new_size);
}

void SSHFSFileSystem::FileSync(FileHandle &handle) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  sshfs_handle.Flush();
}

bool SSHFSFileSystem::FileExists(const string &filename,
                                 optional_ptr<FileOpener> opener) {
  try {
    auto params = ParseURL(filename, opener.get());
    SSHFS_LOG("  [EXISTS] Checking if file exists: " << filename);
    SSHFS_LOG("  [EXISTS] Remote path: " << params.remote_path);

    auto client = GetOrCreateClient(filename, opener.get());

    if (!client->IsConnected()) {
      SSHFS_LOG("  [EXISTS] Client not connected, connecting...");
      client->Connect();
    }

    // Use SFTP to check if file exists by getting stats
    SSHFS_LOG("  [EXISTS] Calling GetFileStats for: " << params.remote_path);
    client->GetFileStats(params.remote_path);
    SSHFS_LOG("  [EXISTS] File exists!");
    return true;
  } catch (const std::exception &e) {
    SSHFS_LOG("  [EXISTS] Exception: " << e.what());
    return false;
  } catch (...) {
    SSHFS_LOG("  [EXISTS] Unknown exception");
    return false;
  }
}

void SSHFSFileSystem::RemoveFile(const string &filename,
                                 optional_ptr<FileOpener> opener) {
  auto params = ParseURL(filename, opener.get());
  auto client = GetOrCreateClient(filename, opener.get());

  if (!client->IsConnected()) {
    client->Connect();
  }

  client->RemoveFile(params.remote_path);
}

void SSHFSFileSystem::MoveFile(const string &source, const string &target,
                               optional_ptr<FileOpener> opener) {
  auto params = ParseURL(source, opener.get());
  auto client = GetOrCreateClient(source, opener.get());

  if (!client->IsConnected()) {
    client->Connect();
  }

  // Extract remote paths from URLs
  auto source_params = ParseURL(source, opener.get());
  auto target_params = ParseURL(target, opener.get());

  client->RenameFile(source_params.remote_path, target_params.remote_path);
}

void SSHFSFileSystem::CreateDirectory(const string &directory,
                                      optional_ptr<FileOpener> opener) {
  auto params = ParseURL(directory, opener.get());
  auto client = GetOrCreateClient(directory, opener.get());

  if (!client->IsConnected()) {
    client->Connect();
  }

  // Always use SFTP for directory creation (avoids command injection via path)
  client->CreateDirectorySFTP(params.remote_path);
}

bool SSHFSFileSystem::DirectoryExists(const string &directory,
                                      optional_ptr<FileOpener> opener) {
  try {
    auto params = ParseURL(directory, opener.get());
    auto client = GetOrCreateClient(directory, opener.get());

    if (!client->IsConnected()) {
      client->Connect();
    }

    // Use SFTP to check if directory exists by getting stats
    auto attrs = client->GetFileStats(params.remote_path);
    // Check if it's actually a directory
    return (attrs.flags & LIBSSH2_SFTP_ATTR_PERMISSIONS) &&
           LIBSSH2_SFTP_S_ISDIR(attrs.permissions);
  } catch (...) {
    return false;
  }
}

void SSHFSFileSystem::RemoveDirectory(const string &directory,
                                      optional_ptr<FileOpener> opener) {
  auto params = ParseURL(directory, opener.get());
  auto client = GetOrCreateClient(directory, opener.get());

  if (!client->IsConnected()) {
    client->Connect();
  }

  // Always use SFTP for directory removal (avoids command injection via path)
  client->RemoveDirectorySFTP(params.remote_path);
}

vector<OpenFileInfo> SSHFSFileSystem::Glob(const string &path,
                                           FileOpener *opener) {
  // Basic glob implementation - just return the path if it exists
  vector<OpenFileInfo> result;
  if (FileExists(path, opener)) {
    result.push_back(OpenFileInfo(path));
  }
  return result;
}

bool SSHFSFileSystem::CanHandleFile(const string &fpath) {
  return StringUtil::StartsWith(fpath, "sshfs://") ||
         StringUtil::StartsWith(fpath, "ssh://") ||
         StringUtil::StartsWith(fpath, "sftp://");
}

timestamp_t SSHFSFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);
  try {
    auto attrs =
        sshfs_handle.GetClient()->GetFileStats(sshfs_handle.GetRemotePath());
    if (attrs.flags & LIBSSH2_SFTP_ATTR_ACMODTIME) {
      return Timestamp::FromEpochSeconds(attrs.mtime);
    }
  } catch (...) {
    // If we can't get stats, return current time
  }
  return Timestamp::FromEpochSeconds(std::time(nullptr));
}

int64_t SSHFSFileSystem::GetFileSize(FileHandle &handle) {
  auto &sshfs_handle = dynamic_cast<SSHFSFileHandle &>(handle);

  // Ensure client is connected before getting file stats
  auto client = sshfs_handle.GetClient();
  if (!client->IsConnected()) {
    client->Connect();
  }

  try {
    // Use cached file stats to avoid repeated SFTP sessions
    auto attrs = sshfs_handle.GetCachedFileStats();
    if (attrs.flags & LIBSSH2_SFTP_ATTR_SIZE) {
      return static_cast<int64_t>(attrs.filesize);
    }

    // If size attribute is not set, return 0 (might be during write)
    return 0;
  } catch (...) {
    // If we can't get stats (file doesn't exist yet during write), return
    // current progress This handles temporary files created during COPY
    // operations
    return static_cast<int64_t>(sshfs_handle.GetProgress());
  }
}

bool SSHFSFileSystem::CanSeek() {
  // SSHFS files can seek using dd command
  return true;
}

bool SSHFSFileSystem::OnDiskFile(FileHandle &handle) {
  // SSHFS files are remote, not on local disk
  return false;
}

SSHConnectionParams SSHFSFileSystem::ParseURL(const string &path,
                                              FileOpener *opener) {
  SSHConnectionParams params;

  // Parse ssh://[username@]hostname[:port]/path/to/file or sshfs://... or
  // sftp://... Support ssh://, sshfs://, and sftp:// prefixes Username is
  // optional - can be provided via secret Support both URL-style (/path) and
  // SCP-style (:path) separators
  std::regex url_regex(
      R"((?:ssh|sshfs|sftp)://(?:([^@]+)@)?([^:/]+)(?::(\d+))?([:/].*))");
  std::smatch matches;

  if (!std::regex_match(path, matches, url_regex)) {
    throw IOException("Invalid SSH/SSHFS/SFTP URL format: %s. Expected: "
                      "ssh://[username@]hostname[:port]/path or "
                      "ssh://[username@]hostname:path (SCP-style)",
                      path);
  }

  // Extract the URL prefix (ssh://, sshfs://, or sftp://) for secret lookups
  std::string url_prefix = StringUtil::StartsWith(path, "ssh://") ? "ssh://"
                           : StringUtil::StartsWith(path, "sshfs://")
                               ? "sshfs://"
                               : "sftp://";

  // Username is optional in URL - can be provided via secret
  if (matches[1].matched) {
    params.username = matches[1].str();
  }
  params.hostname = matches[2].str();
  if (matches[3].matched) {
    params.port = std::stoi(matches[3].str());
  } else {
    params.port = 22; // Default SFTP port
  }
  // Extract the remote path (match[4] is the /path/to/file or :path part)
  if (matches[4].matched) {
    params.remote_path = matches[4].str();
    // Handle SCP-style paths like scp:
    // - ssh://host:file = relative to home directory (file)
    // - ssh://host:/path/file = absolute path (/path/file)
    if (!params.remote_path.empty() && params.remote_path[0] == ':') {
      // SCP-style: remove leading colon, keep rest as-is
      // If path after colon starts with /, it's absolute
      // If path after colon doesn't start with /, it's relative to home
      params.remote_path = params.remote_path.substr(1);
    }
    // Note: If path starts with / without colon (old URL style), keep it
    // as-is for backwards compatibility, but this should be deprecated
  }

  // Look up SSH config (~/.ssh/config) to get default connection parameters
  // This allows using SSH config host aliases without creating secrets
  std::string original_host_alias = params.hostname;
  SSHHostConfig ssh_config = SSHConfigParser::LookupHost(original_host_alias);

  // Track if hostname was resolved (so secret lookups use the original alias)
  bool host_alias_resolved = false;

  // Apply SSH config defaults (URL parameters and secrets will override)
  if (ssh_config.found) {
    // HostName from SSH config
    if (!ssh_config.hostname.empty()) {
      params.hostname = ssh_config.hostname;
      // Mark that we resolved the hostname from an alias
      host_alias_resolved = true;
    }

    // User from SSH config (if not provided in URL)
    if (params.username.empty() && !ssh_config.user.empty()) {
      params.username = ssh_config.user;
    }

    // Port from SSH config (if not provided in URL)
    if (params.port == 22 && ssh_config.port != 22) {
      params.port = ssh_config.port;
    }

    // IdentityFile from SSH config (will be used later if no password in
    // secret)
    if (!ssh_config.identity_file.empty()) {
      params.key_path = ssh_config.identity_file;
    }
  }

  // Resolve host alias to actual hostname (from DuckDB secrets)
  // Check if the hostname from URL matches a "host" in any secret

  if (opener) {
    try {
      auto secret_manager = FileOpener::TryGetSecretManager(opener);
      if (secret_manager) {
        auto transaction = FileOpener::TryGetCatalogTransaction(opener);
        if (transaction) {
          // Look up secret by the host alias using the same prefix as the URL
          auto secret_match = secret_manager->LookupSecret(
              *transaction, url_prefix + original_host_alias, "ssh");

          if (secret_match.HasMatch()) {
            auto &base_secret = secret_match.GetSecret();
            auto *secret = dynamic_cast<const KeyValueSecret *>(&base_secret);
            if (secret) {
              Value value;
              // Check if this secret has a "host" parameter matching our alias
              if (secret->TryGetValue("host", value)) {
                std::string secret_host = value.ToString();
                if (secret_host == original_host_alias) {
                  // This secret matches! Now check for "hostname" to resolve
                  if (secret->TryGetValue("hostname", value)) {
                    params.hostname = value.ToString();
                    host_alias_resolved = true;
                  }
                }
              }
            }
          }
        }
      }
    } catch (...) {
      // Ignore errors during host alias resolution
    }
  }

  // Try to get credentials from secrets if opener is provided
  if (opener) {
    try {
      auto secret_manager = FileOpener::TryGetSecretManager(opener);
      if (secret_manager) {
        auto transaction = FileOpener::TryGetCatalogTransaction(opener);
        if (transaction) {
          // Look up secret using the same prefix as the URL for SCOPE matching
          // Use the original host alias (not resolved hostname) so SCOPE
          // matches
          std::string lookup_key = host_alias_resolved
                                       ? url_prefix + original_host_alias
                                       : url_prefix + params.hostname;
          auto secret_match =
              secret_manager->LookupSecret(*transaction, lookup_key, "ssh");

          if (secret_match.HasMatch()) {
            auto &base_secret = secret_match.GetSecret();
            // Try to cast to KeyValueSecret to access the secret values
            auto *secret = dynamic_cast<const KeyValueSecret *>(&base_secret);
            if (secret) {
              Value value;
              if (secret->TryGetValue("username", value)) {
                params.username = value.ToString();
              }
              if (secret->TryGetValue("password", value)) {
                params.password = value.ToString();
              }
              if (secret->TryGetValue("key_path", value)) {
                params.key_path = value.ToString();
              }
              if (secret->TryGetValue("use_agent", value)) {
                params.use_agent = value.GetValue<bool>();
              }
              if (secret->TryGetValue("port", value)) {
                params.port = value.GetValue<int>();
              }
              // Note: Performance tuning parameters (timeout, retries,
              // chunk_size, etc.) are configured via SET statements, not
              // secrets
            }
          }
        }
      }
    } catch (...) {
      // If secret lookup fails, continue with URL parameters
    }

    // Read performance tuning settings (via SET statements)
    // These are global or session-level settings, not tied to specific secrets
    Value value;

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_debug_logging",
                                         value)) {
      params.debug_logging = value.GetValue<bool>();
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_timeout_seconds",
                                         value)) {
      // Only apply if not set by secret
      if (params.timeout_seconds == 300) { // default value
        params.timeout_seconds = static_cast<int>(value.GetValue<int64_t>());
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_max_retries", value)) {
      if (params.max_retries == 3) { // default value
        params.max_retries = static_cast<int>(value.GetValue<int64_t>());
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_initial_retry_delay_ms",
                                         value)) {
      if (params.initial_retry_delay_ms == 1000) { // default value
        params.initial_retry_delay_ms =
            static_cast<int>(value.GetValue<int64_t>());
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "ssh_keepalive", value)) {
      if (params.keepalive_interval == 60) { // default value
        params.keepalive_interval = static_cast<int>(value.GetValue<int64_t>());
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_chunk_size_mb",
                                         value)) {
      if (params.chunk_size == 50 * 1024 * 1024) { // default value
        params.chunk_size =
            static_cast<size_t>(value.GetValue<int64_t>()) * 1024 * 1024;
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_max_concurrent_uploads",
                                         value)) {
      if (params.max_concurrent_uploads == 2) { // default value
        params.max_concurrent_uploads =
            static_cast<size_t>(value.GetValue<int64_t>());
      }
    }

    if (FileOpener::TryGetCurrentSetting(opener, "sshfs_strict_crypto",
                                         value)) {
      params.strict_crypto = value.GetValue<bool>();
    }
  }

  // Validate that we have required authentication info
  // Username can be provided in URL or in secret
  if (params.username.empty()) {
    throw IOException("SSHFS requires username (provide in URL or secret)");
  }

  if (params.password.empty() && params.key_path.empty()) {
    throw IOException("SSHFS requires either password or key_path in secret");
  }

  return params;
}

std::shared_ptr<SSHClient>
SSHFSFileSystem::GetOrCreateClient(const string &path, FileOpener *opener) {
  auto params = ParseURL(path, opener);
  string connection_key = ExtractConnectionKey(params);

  std::lock_guard<std::mutex> lock(pool_mutex);

  // Check if client already exists
  auto it = client_pool.find(connection_key);
  if (it != client_pool.end()) {
    // Validate the pooled connection is still alive
    if (it->second->ValidateConnection()) {
      return it->second;
    }
    // Connection is dead, remove it from pool and create new one
    client_pool.erase(it);
  }

  // Create new client
  auto client = std::make_shared<SSHClient>(params);
  client_pool[connection_key] = client;

  return client;
}

string
SSHFSFileSystem::ExtractConnectionKey(const SSHConnectionParams &params) {
  return params.username + "@" + params.hostname + ":" +
         std::to_string(params.port);
}

} // namespace duckdb
