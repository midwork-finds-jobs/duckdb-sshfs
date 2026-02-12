#include "ssh_client.hpp"
#include "duckdb/common/exception.hpp"
#include "ssh_helpers.hpp"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sstream>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace duckdb {

// Thread-local debug flag
thread_local bool g_sshfs_debug_enabled = false;

// Global mutex to serialize dd command execution (SSH channels)
// Hetzner Storage Boxes limit concurrent SSH channels
// Serialize all dd reads to avoid overwhelming the server
static std::mutex g_dd_command_mutex;

SSHClient::SSHClient(const SSHConnectionParams &params) : params(params) {
  // Set thread-local debug flag from params
  g_sshfs_debug_enabled = params.debug_logging;
  // Initialize libssh2 (refcounted internally, safe to call multiple times)
  int rc = libssh2_init(0);
  if (rc != 0) {
    throw IOException("Failed to initialize libssh2");
  }
}

SSHClient::~SSHClient() {
  Disconnect();
  libssh2_exit();
}

void SSHClient::Connect() {
  // Detect Hetzner Storage Boxes and disable dd upfront
  // Hetzner has very strict SSH channel limits that make dd unusable for large
  // queries
  if (params.hostname.find("storagebox.de") != std::string::npos ||
      params.hostname.find("your-storagebox.de") != std::string::npos) {
    SSHFS_LOG("  [DETECT] Detected Hetzner Storage Box - disabling dd reads");
    dd_disabled = true;
  }

  if (connected) {
    return;
  }

  int retry_delay_ms = params.initial_retry_delay_ms;
  int attempt = 0;
  std::string last_error;

  while (attempt <= params.max_retries) {
    try {
      if (attempt > 0) {
        SSHFS_LOG("  [RETRY] Attempt " << attempt << "/" << params.max_retries
                                       << " after " << retry_delay_ms
                                       << "ms delay...");
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
      }

      // Resolve hostname
      struct addrinfo hints, *res;
      memset(&hints, 0, sizeof(hints));
      hints.ai_family = AF_UNSPEC;
      hints.ai_socktype = SOCK_STREAM;

      std::string port_str = std::to_string(params.port);
      int rc =
          getaddrinfo(params.hostname.c_str(), port_str.c_str(), &hints, &res);
      if (rc != 0) {
        throw IOException(
            "Failed to resolve hostname '%s': %s\n"
            "  → Check that the hostname is correct and DNS is configured\n"
            "  → Try: ping %s",
            params.hostname.c_str(), gai_strerror(rc), params.hostname.c_str());
      }

      // Create socket
      sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
      if (sock == -1) {
        freeaddrinfo(res);
        throw IOException("Failed to create socket for %s:%d (errno: %d, %s)\n"
                          "  → This usually indicates a system resource limit",
                          params.hostname.c_str(), params.port, errno,
                          strerror(errno));
      }

      // Connect
      if (connect(sock, res->ai_addr, res->ai_addrlen) != 0) {
        int err = errno;
        close(sock);
        sock = -1;
        freeaddrinfo(res);

        // Provide helpful error messages based on errno
        const char *suggestion = "";
        if (err == ECONNREFUSED) {
          suggestion =
              "\n  → SSH server may not be running or port is blocked\n"
              "  → Try: ssh -p %d %s@%s";
        } else if (err == ETIMEDOUT || err == EHOSTUNREACH) {
          suggestion = "\n  → Network unreachable or firewall blocking "
                       "connection\n  → Check firewall rules and network "
                       "connectivity";
        } else if (err == ENETUNREACH) {
          suggestion = "\n  → No route to host\n  → Check network "
                       "configuration and routing";
        }

        throw IOException("Failed to connect to %s:%d: %s (errno: %d)%s",
                          params.hostname.c_str(), params.port, strerror(err),
                          err, suggestion);
      }

      freeaddrinfo(res);

      // Initialize SSH session
      InitializeSession();
      Authenticate();

      // Detect server capabilities (command execution support)
      DetectCapabilities();

      connected = true;
      if (attempt > 0) {
        SSHFS_LOG("  [RETRY] Connection successful on attempt " << attempt + 1);
      }
      return; // Success!

    } catch (const IOException &e) {
      last_error = e.what();

      // Check if this is an authentication error (don't retry these)
      if (std::string(e.what()).find("authentication failed") !=
          std::string::npos) {
        SSHFS_LOG("  [RETRY] Authentication failed - not retrying");
        throw; // Re-throw authentication errors immediately
      }

      // Cleanup on failure
      if (sock != -1) {
        close(sock);
        sock = -1;
      }
      if (session) {
        CleanupSession();
      }

      // If this was the last attempt, give up
      if (attempt >= params.max_retries) {
        throw IOException("Failed to connect after %d attempts. Last error: %s",
                          params.max_retries + 1, last_error.c_str());
      }

      // Exponential backoff for next retry
      retry_delay_ms *= 2;
      attempt++;
    }
  }
}

void SSHClient::InitializeSession() {
  session = libssh2_session_init();
  if (!session) {
    close(sock);
    throw IOException(
        "Failed to create SSH session for %s@%s:%d\n"
        "  → This usually indicates a libssh2 initialization problem\n"
        "  → Check that libssh2 is properly installed",
        params.username.c_str(), params.hostname.c_str(), params.port);
  }

  // Set blocking mode
  libssh2_session_set_blocking(session, 1);

  // Set timeout
  libssh2_session_set_timeout(session, params.timeout_seconds * 1000);

  // Configure keepalive to prevent idle connection timeouts
  // Send keepalive packets every N seconds to keep connection alive
  // want_reply=0 means we don't wait for server response (faster)
  if (params.keepalive_interval > 0) {
    libssh2_keepalive_config(session, 0, params.keepalive_interval);
    SSHFS_LOG("  [KEEPALIVE] Configured keepalive interval: "
              << params.keepalive_interval << " seconds");
  }

  // Set preferred KEX algorithms — modern ECDH/curve25519 first, DH fallbacks.
  // Removes insecure group1-sha1 and group-exchange-sha1.
  // See: https://github.com/midwork-finds-jobs/duckdb-sshfs/issues/7
  const char *kex_algorithms =
      "curve25519-sha256,curve25519-sha256@libssh.org,"
      "ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521,"
      "diffie-hellman-group14-sha256,"
      "diffie-hellman-group-exchange-sha256,"
      "diffie-hellman-group16-sha512,"
      "diffie-hellman-group18-sha512,"
      "diffie-hellman-group14-sha1";
  int kex_rc =
      libssh2_session_method_pref(session, LIBSSH2_METHOD_KEX, kex_algorithms);
  if (kex_rc != 0) {
    SSHFS_LOG("  [KEX] Warning: Could not set KEX preferences (rc=" << kex_rc
                                                                    << ")");
  } else {
    SSHFS_LOG("  [KEX] Set KEX preferences: " << kex_algorithms);
  }

  // Set preferred host key algorithms
  const char *hostkey_algorithms =
      "ssh-ed25519,"
      "ecdsa-sha2-nistp256,ecdsa-sha2-nistp384,ecdsa-sha2-nistp521,"
      "rsa-sha2-256,rsa-sha2-512,ssh-rsa";
  int hk_rc = libssh2_session_method_pref(session, LIBSSH2_METHOD_HOSTKEY,
                                          hostkey_algorithms);
  if (hk_rc != 0) {
    SSHFS_LOG("  [HOSTKEY] Warning: Could not set host key preferences (rc="
              << hk_rc << ")");
  } else {
    SSHFS_LOG("  [HOSTKEY] Set host key preferences: " << hostkey_algorithms);
  }

  // Enable libssh2 protocol-level trace when debug logging is on
  if (IsDebugLoggingEnabled()) {
    libssh2_trace(session, ~0);
    libssh2_trace_sethandler(
        session, nullptr,
        [](LIBSSH2_SESSION *, void *, const char *data, size_t len) {
          fprintf(stderr, "  [SSH2] %.*s\n", (int)len, data);
        });
  }

  // Wait for SSH server banner before handshake.
  // After connect(), the server sends its version string (e.g. "SSH-2.0-...").
  // libssh2_session_handshake() expects this banner to be available on the
  // socket. In tight execution contexts (like DuckDB's engine), the handshake
  // can be called before the banner arrives, causing KEX error -8.
  // poll() is used instead of select() to avoid the FD_SETSIZE limit.
  {
    struct pollfd pfd = {sock, POLLIN, 0};
    int poll_rc = poll(&pfd, 1, params.timeout_seconds * 1000);
    if (poll_rc < 0) {
      CleanupSession();
      throw IOException(
          "poll() failed waiting for SSH banner from %s:%d (errno: %d, %s)",
          params.hostname.c_str(), params.port, errno, strerror(errno));
    }
    if (poll_rc == 0) {
      CleanupSession();
      throw IOException(
          "SSH server at %s:%d did not send banner within %d seconds",
          params.hostname.c_str(), params.port, params.timeout_seconds);
    }
    SSHFS_LOG("  [HANDSHAKE] Server banner ready, proceeding with handshake");
  }

  // Perform SSH handshake
  SSHFS_LOG("  [HANDSHAKE] Starting SSH handshake...");
  int rc = libssh2_session_handshake(session, sock);
  if (rc != 0) {
    char *err_msg;
    int err_len;
    libssh2_session_last_error(session, &err_msg, &err_len, 0);

    SSHFS_LOG("  [HANDSHAKE] Failed with error code: " << rc);
    SSHFS_LOG(
        "  [HANDSHAKE] Error message: " << (err_msg ? err_msg : "Unknown"));

    CleanupSession();

    // Provide specific guidance based on error code
    const char *suggestion = "";
    if (rc == LIBSSH2_ERROR_TIMEOUT) {
      suggestion = "\n  → Connection timed out during handshake\n"
                   "  → Server may be slow or overloaded";
    } else if (rc == LIBSSH2_ERROR_KEY_EXCHANGE_FAILURE) {
      suggestion =
          "\n  → SSH key exchange failed\n"
          "  → Server and client may have incompatible encryption "
          "algorithms\n"
          "  → libssh2 may not support the server's preferred algorithms\n"
          "  → Try upgrading libssh2 or use OpenSSH command-line tools";
    }

    throw IOException("SSH handshake failed for %s@%s:%d\n"
                      "  Error code: %d\n"
                      "  Message: %s%s",
                      params.username.c_str(), params.hostname.c_str(),
                      params.port, rc, err_msg ? err_msg : "Unknown error",
                      suggestion);
  }

  SSHFS_LOG("  [HANDSHAKE] SSH handshake successful!");

  // Configure keepalive to detect dead connections (after handshake succeeds)
  libssh2_keepalive_config(session, 1, 60); // Send keepalive every 60s
}

void SSHClient::Authenticate() {
  int rc = -1;
  std::string auth_details;

  // Try password authentication if password is provided
  // Password takes priority - if explicitly provided, use it exclusively
  if (!params.password.empty()) {
    rc = libssh2_userauth_password(session, params.username.c_str(),
                                   params.password.c_str());

    if (rc == 0) {
      SSHFS_LOG("  [AUTH] Password authentication succeeded");
      return; // Success
    }

    auth_details += "  → Password authentication failed\n"
                    "    Check username and password are correct\n";

    // Password was provided but failed - don't try other methods
    char *err_msg;
    libssh2_session_last_error(session, &err_msg, nullptr, 0);
    CleanupSession();

    throw IOException("SSH authentication failed for %s@%s:%d\n"
                      "%s"
                      "  libssh2 error: %s (code: %d)",
                      params.username.c_str(), params.hostname.c_str(),
                      params.port, auth_details.c_str(),
                      err_msg ? err_msg : "Unknown error", rc);
  }

  // Try public key authentication if key path is provided
  // Key takes priority over SSH agent - if explicitly provided, use it
  // exclusively
  if (!params.key_path.empty()) {
    std::string public_key = params.key_path + ".pub";
    rc = libssh2_userauth_publickey_fromfile(session, params.username.c_str(),
                                             public_key.c_str(),
                                             params.key_path.c_str(),
                                             nullptr // No passphrase
    );

    if (rc == 0) {
      SSHFS_LOG("  [AUTH] Public key authentication succeeded");
      return; // Success
    }

    // Build detailed error message
    auth_details = "  → Public key authentication failed\n";
    if (rc == LIBSSH2_ERROR_PUBLICKEY_UNVERIFIED) {
      auth_details += "    Key was rejected by server (invalid key or user)\n";
    } else if (rc == LIBSSH2_ERROR_FILE) {
      auth_details += "    Could not read key file\n";
    }
    auth_details +=
        "    Key file: " + params.key_path +
        "\n"
        "    Check: file exists, has correct permissions (chmod 600), and "
        "matches server's authorized_keys\n"
        "    Try: ssh -i " +
        params.key_path + " -p " + std::to_string(params.port) + " " +
        params.username + "@" + params.hostname + "\n";

    // Key was provided but failed - don't try other methods
    char *err_msg;
    libssh2_session_last_error(session, &err_msg, nullptr, 0);
    CleanupSession();

    throw IOException("SSH authentication failed for %s@%s:%d\n"
                      "%s"
                      "  libssh2 error: %s (code: %d)",
                      params.username.c_str(), params.hostname.c_str(),
                      params.port, auth_details.c_str(),
                      err_msg ? err_msg : "Unknown error", rc);
  }

  // Try SSH agent authentication if explicitly enabled
  // Only used when use_agent is true and neither password nor key_path is
  // provided
  if (params.use_agent) {
    const char *ssh_auth_sock = getenv("SSH_AUTH_SOCK");
    if (!ssh_auth_sock || strlen(ssh_auth_sock) == 0) {
      CleanupSession();
      throw IOException(
          "SSH authentication failed for %s@%s:%d\n"
          "  SSH agent authentication requested but SSH_AUTH_SOCK is not set\n"
          "  → Start SSH agent: eval $(ssh-agent -s)\n"
          "  → Add key to agent: ssh-add ~/.ssh/id_rsa",
          params.username.c_str(), params.hostname.c_str(), params.port);
    }

    LIBSSH2_AGENT *agent = libssh2_agent_init(session);
    if (!agent) {
      CleanupSession();
      throw IOException("SSH authentication failed for %s@%s:%d\n"
                        "  Failed to initialize SSH agent",
                        params.username.c_str(), params.hostname.c_str(),
                        params.port);
    }

    if (libssh2_agent_connect(agent) != 0) {
      libssh2_agent_free(agent);
      CleanupSession();
      throw IOException("SSH authentication failed for %s@%s:%d\n"
                        "  Failed to connect to SSH agent",
                        params.username.c_str(), params.hostname.c_str(),
                        params.port);
    }

    if (libssh2_agent_list_identities(agent) != 0) {
      libssh2_agent_disconnect(agent);
      libssh2_agent_free(agent);
      CleanupSession();
      throw IOException("SSH authentication failed for %s@%s:%d\n"
                        "  Failed to list identities from SSH agent",
                        params.username.c_str(), params.hostname.c_str(),
                        params.port);
    }

    struct libssh2_agent_publickey *identity = nullptr;
    struct libssh2_agent_publickey *prev = nullptr;
    bool agent_auth_success = false;

    // Try each identity from the agent
    while (libssh2_agent_get_identity(agent, &identity, prev) == 0) {
      if (libssh2_agent_userauth(agent, params.username.c_str(), identity) ==
          0) {
        // Success! Clean up agent and return
        libssh2_agent_disconnect(agent);
        libssh2_agent_free(agent);
        SSHFS_LOG("  [AUTH] SSH agent authentication succeeded");
        agent_auth_success = true;
        break;
      }
      prev = identity;
    }

    if (agent_auth_success) {
      return;
    }

    // All agent identities failed
    libssh2_agent_disconnect(agent);
    libssh2_agent_free(agent);

    char *err_msg;
    libssh2_session_last_error(session, &err_msg, nullptr, 0);
    CleanupSession();

    throw IOException(
        "SSH authentication failed for %s@%s:%d\n"
        "  → SSH agent authentication failed (tried all identities)\n"
        "  libssh2 error: %s",
        params.username.c_str(), params.hostname.c_str(), params.port,
        err_msg ? err_msg : "Unknown error");
  }

  // Deprecated: Fall back to SSH agent if SSH_AUTH_SOCK is set (for backward
  // compatibility) This behavior will be removed in a future version - use
  // use_ssh_agent=true instead
  const char *ssh_auth_sock = getenv("SSH_AUTH_SOCK");
  if (ssh_auth_sock && strlen(ssh_auth_sock) > 0) {
    LIBSSH2_AGENT *agent = libssh2_agent_init(session);
    if (agent) {
      if (libssh2_agent_connect(agent) == 0) {
        if (libssh2_agent_list_identities(agent) == 0) {
          struct libssh2_agent_publickey *identity = nullptr;
          struct libssh2_agent_publickey *prev = nullptr;

          // Try each identity from the agent
          while (libssh2_agent_get_identity(agent, &identity, prev) == 0) {
            if (libssh2_agent_userauth(agent, params.username.c_str(),
                                       identity) == 0) {
              // Success! Clean up agent and return
              libssh2_agent_disconnect(agent);
              libssh2_agent_free(agent);
              SSHFS_LOG("  [AUTH] SSH agent authentication succeeded");
              return;
            }
            prev = identity;
          }
        }
        libssh2_agent_disconnect(agent);
      }
      libssh2_agent_free(agent);
      auth_details +=
          "  → SSH agent authentication failed (tried all identities)\n";
    }
  }

  // No auth methods available or all failed
  if (params.key_path.empty() && params.password.empty() && !params.use_agent &&
      (!ssh_auth_sock || strlen(ssh_auth_sock) == 0)) {
    CleanupSession();
    throw IOException(
        "SSH authentication failed for %s@%s:%d\n"
        "  No authentication method available\n"
        "  → Specify 'password' for password authentication\n"
        "  → Specify 'key_path' for public key authentication\n"
        "  → Set 'use_agent=true' to use SSH agent (requires SSH_AUTH_SOCK)",
        params.username.c_str(), params.hostname.c_str(), params.port);
  }

  char *err_msg;
  libssh2_session_last_error(session, &err_msg, nullptr, 0);
  CleanupSession();

  throw IOException("SSH authentication failed for %s@%s:%d\n"
                    "%s"
                    "  libssh2 error: %s (code: %d)",
                    params.username.c_str(), params.hostname.c_str(),
                    params.port, auth_details.c_str(),
                    err_msg ? err_msg : "Unknown error", rc);
}

void SSHClient::Disconnect() {
  if (!connected) {
    return;
  }

  CleanupSFTPPool();
  CleanupSession();
  connected = false;
}

bool SSHClient::ValidateConnection() {
  if (!connected || !session) {
    return false;
  }

  // Use keepalive to verify the connection is still alive
  int seconds_to_next = 0;
  int rc = libssh2_keepalive_send(session, &seconds_to_next);

  // If keepalive succeeds, connection is valid
  return (rc == 0);
}

void SSHClient::CleanupSession() {
  if (session) {
    libssh2_session_disconnect(session, "Normal shutdown");
    libssh2_session_free(session);
    session = nullptr;
  }
  if (sock != -1) {
    close(sock);
    sock = -1;
  }
}

std::string SSHClient::ExecuteCommand(const std::string &command) {
  if (!connected) {
    throw IOException(
        "Not connected to SSH server\n"
        "  → Connection may have been closed or timed out\n"
        "  → Try reconnecting or check keepalive_interval setting\n"
        "  → Check: ssh -p %d %s@%s",
        params.port, params.username.c_str(), params.hostname.c_str());
  }

  // Open channel
  LIBSSH2_CHANNEL *channel = libssh2_channel_open_session(session);
  if (!channel) {
    char *err_msg;
    int err_code = libssh2_session_last_error(session, &err_msg, nullptr, 0);
    throw IOException("Failed to open SSH channel for command execution\n"
                      "  → Command: %s\n"
                      "  → libssh2 error: %s (code: %d)\n"
                      "  → Server may have reached maximum channel limit\n"
                      "  → Try reducing concurrent operations",
                      command.c_str(), err_msg, err_code);
  }

  // Execute command
  int rc = libssh2_channel_exec(channel, command.c_str());
  if (rc != 0) {
    char *err_msg;
    libssh2_session_last_error(session, &err_msg, nullptr, 0);
    libssh2_channel_free(channel);
    throw IOException("Failed to execute SSH command\n"
                      "  → Command: %s\n"
                      "  → libssh2 error: %s (code: %d)\n"
                      "  → Server may not support this command\n"
                      "  → Try: ssh -p %d %s@%s \"%s\"",
                      command.c_str(), err_msg, rc, params.port,
                      params.username.c_str(), params.hostname.c_str(),
                      command.c_str());
  }

  // Read output
  std::stringstream output;
  char buffer[4096];
  ssize_t nbytes;

  while ((nbytes = libssh2_channel_read(channel, buffer, sizeof(buffer))) > 0) {
    output.write(buffer, nbytes);
  }

  // Get exit status
  int exit_status = libssh2_channel_get_exit_status(channel);

  // Close channel
  libssh2_channel_close(channel);
  libssh2_channel_wait_closed(channel);
  libssh2_channel_free(channel);

  if (exit_status != 0) {
    throw IOException("Command failed with exit status %d: %s", exit_status,
                      command);
  }

  return output.str();
}

void SSHClient::UploadChunk(const std::string &remote_path, const char *data,
                            size_t size, bool append) {
  if (!connected) {
    throw IOException("Not connected to SSH server");
  }

  ScopedTimer total_timer("SFTP", append ? "Append data" : "Total upload");

  // Lock per-client mutex to serialize SFTP operations (libssh2 session is not
  // thread-safe)
  std::lock_guard<std::mutex> lock(upload_mutex);

  // Borrow SFTP session from pool
  LIBSSH2_SFTP *sftp = BorrowSFTPSession();

  // Create parent directories if needed
  {
    ScopedTimer mkdir_timer("SFTP", "Create dirs");
    size_t last_slash = remote_path.find_last_of('/');
    if (last_slash != std::string::npos) {
      std::string dir_path = remote_path.substr(0, last_slash);
      if (!dir_path.empty()) {
        // Create directories recursively
        std::string current_path;
        size_t pos = 0;
        while (pos < dir_path.length()) {
          size_t next_slash = dir_path.find('/', pos);
          if (next_slash == std::string::npos) {
            next_slash = dir_path.length();
          }

          if (next_slash > pos) {
            if (!current_path.empty()) {
              current_path += "/";
            }
            current_path += dir_path.substr(pos, next_slash - pos);

            // Try to create this directory (ignore errors if it exists)
            libssh2_sftp_mkdir(sftp, current_path.c_str(),
                               LIBSSH2_SFTP_S_IRWXU | LIBSSH2_SFTP_S_IRGRP |
                                   LIBSSH2_SFTP_S_IXGRP | LIBSSH2_SFTP_S_IROTH |
                                   LIBSSH2_SFTP_S_IXOTH);
          }

          pos = next_slash + 1;
        }
      }
    }
  }

  // Open remote file for writing
  LIBSSH2_SFTP_HANDLE *sftp_handle;
  {
    ScopedTimer open_timer("SFTP", append ? "Open for append" : "Open file");

    // Choose flags based on append mode
    unsigned long flags;
    if (append) {
      // Append to existing file (fail if file doesn't exist)
      flags = LIBSSH2_FXF_WRITE | LIBSSH2_FXF_APPEND;
    } else {
      // Create new file or truncate existing
      flags = LIBSSH2_FXF_WRITE | LIBSSH2_FXF_CREAT | LIBSSH2_FXF_TRUNC;
    }

    sftp_handle =
        libssh2_sftp_open(sftp, remote_path.c_str(), flags,
                          LIBSSH2_SFTP_S_IRUSR | LIBSSH2_SFTP_S_IWUSR |
                              LIBSSH2_SFTP_S_IRGRP | LIBSSH2_SFTP_S_IROTH);

    if (!sftp_handle) {
      ReturnSFTPSession(sftp);
      throw IOException("Failed to open remote file for %s: %s",
                        append ? "appending" : "writing", remote_path);
    }
  }

  // Write all data - let libssh2 handle internal buffering
  {
    ThroughputTimer write_timer("SFTP", "Write", size);
    size_t total_written = 0;

    // Write in a loop until all data is sent
    while (total_written < size) {
      ssize_t written = libssh2_sftp_write(sftp_handle, data + total_written,
                                           size - total_written);

      if (written < 0) {
        char *err_msg;
        libssh2_session_last_error(session, &err_msg, nullptr, 0);
        libssh2_sftp_close(sftp_handle);
        ReturnSFTPSession(sftp);
        throw IOException(
            "Failed to write to remote file: %s (libssh2 error %d: %s)",
            remote_path, (int)written, err_msg ? err_msg : "Unknown error");
      }

      if (written == 0) {
        // No progress - this shouldn't happen in blocking mode
        libssh2_sftp_close(sftp_handle);
        ReturnSFTPSession(sftp);
        throw IOException("SFTP write stalled at %zu/%zu bytes for: %s",
                          total_written, size, remote_path);
      }

      total_written += written;
    }
  }

  // Close file and return SFTP session to pool
  {
    ScopedTimer close_timer("SFTP", "Close handle");
    libssh2_sftp_close(sftp_handle);
  }

  // Return SFTP session to pool for reuse
  ReturnSFTPSession(sftp);
}

void SSHClient::AppendChunk(const std::string &remote_path,
                            const std::string &chunk_path) {
  if (!connected) {
    throw IOException("Not connected to SSH server");
  }

  // Use dd command to append chunk to final file
  std::string command = "dd if=" + chunk_path + " of=" + remote_path +
                        " oflag=append conv=notrunc 2>/dev/null";

  ExecuteCommand(command);
}

void SSHClient::RemoveFile(const std::string &remote_path) {
  if (!connected) {
    throw IOException(
        "Not connected to SSH server\n"
        "  → Connection may have been closed or timed out\n"
        "  → Try reconnecting or check keepalive_interval setting");
  }

  // Initialize SFTP session
  LIBSSH2_SFTP *sftp = libssh2_sftp_init(session);
  if (!sftp) {
    // If SFTP fails, try using rm command
    try {
      ExecuteCommand("rm " + remote_path);
      return;
    } catch (...) {
      char *err_msg;
      int err_code = libssh2_session_last_error(session, &err_msg, nullptr, 0);
      throw IOException("Failed to remove remote file: %s\n"
                        "  → libssh2 error: %s (code: %d)\n"
                        "  → File may not exist or you may lack permissions\n"
                        "  → Try: ssh -p %d %s@%s 'ls -la %s'",
                        remote_path.c_str(), err_msg, err_code, params.port,
                        params.username.c_str(), params.hostname.c_str(),
                        remote_path.c_str());
    }
  }

  // Remove file via SFTP
  int rc = libssh2_sftp_unlink(sftp, remote_path.c_str());
  libssh2_sftp_shutdown(sftp);

  if (rc != 0) {
    throw IOException("Failed to remove remote file: %s", remote_path);
  }
}

void SSHClient::RenameFile(const std::string &source_path,
                           const std::string &target_path) {
  if (!connected) {
    throw IOException(
        "Not connected to SSH server\n"
        "  → Connection may have been closed or timed out\n"
        "  → Try reconnecting or check keepalive_interval setting");
  }

  // Initialize SFTP session
  LIBSSH2_SFTP *sftp = libssh2_sftp_init(session);
  if (!sftp) {
    // If SFTP fails, try using mv command
    try {
      ExecuteCommand("mv " + source_path + " " + target_path);
      return;
    } catch (...) {
      char *err_msg;
      int err_code = libssh2_session_last_error(session, &err_msg, nullptr, 0);
      throw IOException("Failed to rename remote file from %s to %s\n"
                        "  → libssh2 error: %s (code: %d)\n"
                        "  → Source file may not exist or lack permissions\n"
                        "  → Try: ssh -p %d %s@%s 'mv %s %s'",
                        source_path.c_str(), target_path.c_str(), err_msg,
                        err_code, params.port, params.username.c_str(),
                        params.hostname.c_str(), source_path.c_str(),
                        target_path.c_str());
    }
  }

  // Rename file via SFTP
  // LIBSSH2_SFTP_RENAME_OVERWRITE flag ensures atomic rename behavior
  int rc = libssh2_sftp_rename_ex(
      sftp, source_path.c_str(), source_path.length(), target_path.c_str(),
      target_path.length(),
      LIBSSH2_SFTP_RENAME_OVERWRITE | LIBSSH2_SFTP_RENAME_ATOMIC);
  libssh2_sftp_shutdown(sftp);

  if (rc != 0) {
    unsigned long sftp_error = libssh2_sftp_last_error(sftp);
    throw IOException("Failed to rename remote file from %s to %s\n"
                      "  → SFTP error code: %lu\n"
                      "  → Source may not exist or target may already exist\n"
                      "  → Check file permissions and paths",
                      source_path.c_str(), target_path.c_str(), sftp_error);
  }
}

LIBSSH2_SFTP_ATTRIBUTES
SSHClient::GetFileStats(const std::string &remote_path) {
  if (!connected) {
    throw IOException(
        "Not connected to SSH server\n"
        "  → Connection may have been closed or timed out\n"
        "  → Try reconnecting or check keepalive_interval setting");
  }

  auto stats_start = std::chrono::steady_clock::now();

  // Use pooled SFTP session instead of creating new one each time
  LIBSSH2_SFTP *sftp = BorrowSFTPSession();

  // Get file stats
  auto stat_start = std::chrono::steady_clock::now();
  LIBSSH2_SFTP_ATTRIBUTES attrs;
  int rc = libssh2_sftp_stat(sftp, remote_path.c_str(), &attrs);
  auto stat_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - stat_start)
                     .count();

  if (rc != 0) {
    unsigned long sftp_error = libssh2_sftp_last_error(sftp);
    ReturnSFTPSession(sftp);
    throw IOException("Failed to get file stats for: %s\n"
                      "  → SFTP error code: %lu\n"
                      "  → File may not exist or you may lack permissions\n"
                      "  → Try: ssh -p %d %s@%s 'ls -la %s'",
                      remote_path.c_str(), sftp_error, params.port,
                      params.username.c_str(), params.hostname.c_str(),
                      remote_path.c_str());
  }

  // Return session to pool
  ReturnSFTPSession(sftp);

  auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - stats_start)
                      .count();

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [STAT] GetFileStats for " << remote_path
              << " (stat=" << stat_ms << "ms, total=" << total_ms << "ms)"
              << std::endl;
  }

  return attrs;
}

size_t SSHClient::ReadBytes(const std::string &remote_path, char *buffer,
                            size_t offset, size_t length) {
  if (!connected) {
    throw IOException("Not connected to SSH server");
  }

  // Fall back to SFTP reads for SFTP-only servers or if dd has been disabled
  if (!supports_commands || dd_disabled) {
    return ReadBytesSFTP(remote_path, buffer, offset, length);
  }

  // Serialize dd command execution globally to avoid overwhelming Hetzner's SSH
  // channel limits
  std::lock_guard<std::mutex> dd_lock(g_dd_command_mutex);

  auto read_start = std::chrono::steady_clock::now();

  // Use SSH dd command for reads - faster than SFTP (no session overhead)
  // This is similar to HTTP range requests - only transfers exact bytes needed
  //
  // dd parameters:
  // - bs=4096: read in 4KB blocks (efficient)
  // - iflag=skip_bytes,count_bytes: treat skip/count as bytes not blocks
  // - skip=OFFSET: skip OFFSET bytes from start
  // - count=LENGTH: read LENGTH bytes
  // - status=none: suppress dd's stderr output

  std::string command =
      "dd if=" + remote_path + " bs=4096" + " iflag=skip_bytes,count_bytes" +
      " skip=" + std::to_string(offset) + " count=" + std::to_string(length) +
      " status=none 2>/dev/null";

  // Open channel for command
  auto channel_open_start = std::chrono::steady_clock::now();
  LIBSSH2_CHANNEL *channel = libssh2_channel_open_session(session);
  if (!channel) {
    // Fall back to SFTP if SSH channel creation fails
    // This happens on servers with strict channel limits (e.g., Hetzner Storage
    // Boxes) Disable dd permanently to avoid repeatedly hitting the same limit
    SSHFS_LOG(
        "  [READ-DD] Failed to open SSH channel, disabling dd and using SFTP");
    dd_disabled = true;
    return ReadBytesSFTP(remote_path, buffer, offset, length);
  }
  auto channel_open_end = std::chrono::steady_clock::now();
  auto channel_open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             channel_open_end - channel_open_start)
                             .count();

  // Execute dd command
  auto exec_start = std::chrono::steady_clock::now();
  int rc = libssh2_channel_exec(channel, command.c_str());
  if (rc != 0) {
    libssh2_channel_free(channel);
    // Fall back to SFTP if dd command execution fails
    // This happens on servers where dd execution is blocked despite command
    // support Disable dd permanently since it's likely to fail again
    SSHFS_LOG("  [READ-DD] Failed to execute dd command, disabling dd and "
              "using SFTP");
    dd_disabled = true;
    return ReadBytesSFTP(remote_path, buffer, offset, length);
  }
  auto exec_end = std::chrono::steady_clock::now();
  auto exec_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     exec_end - exec_start)
                     .count();

  // Read output directly into buffer
  auto actual_read_start = std::chrono::steady_clock::now();
  size_t total_read = 0;
  while (total_read < length) {
    ssize_t nread =
        libssh2_channel_read(channel, buffer + total_read, length - total_read);

    if (nread == LIBSSH2_ERROR_EAGAIN) {
      continue; // Retry
    } else if (nread < 0) {
      libssh2_channel_close(channel);
      libssh2_channel_free(channel);
      throw IOException("Failed to read from SSH channel");
    } else if (nread == 0) {
      // End of output
      break;
    }

    total_read += nread;
  }
  auto actual_read_end = std::chrono::steady_clock::now();
  auto actual_read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            actual_read_end - actual_read_start)
                            .count();

  // Wait for command to complete
  auto close_start = std::chrono::steady_clock::now();
  int exit_status = libssh2_channel_get_exit_status(channel);

  // Close channel
  libssh2_channel_close(channel);
  libssh2_channel_wait_closed(channel);
  libssh2_channel_free(channel);
  auto close_end = std::chrono::steady_clock::now();
  auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      close_end - close_start)
                      .count();

  if (exit_status != 0 && total_read == 0) {
    throw IOException("dd command failed with exit status %d", exit_status);
  }

  auto read_end = std::chrono::steady_clock::now();
  auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      read_end - read_start)
                      .count();

  if (IsDebugLoggingEnabled()) {
    double mb_size = total_read / (1024.0 * 1024.0);
    double mb_per_sec = total_ms > 0 ? (mb_size / (total_ms / 1000.0)) : 0;
    std::cerr << "  [READ-DD] offset=" << offset << " length=" << length
              << " read=" << total_read << " bytes in " << total_ms << "ms ("
              << mb_per_sec << " MB/s)" << std::endl;
    std::cerr << "    [BREAKDOWN] channel_open=" << channel_open_ms
              << "ms, exec=" << exec_ms << "ms, actual_read=" << actual_read_ms
              << "ms, close=" << close_ms << "ms" << std::endl;
  }

  return total_read;
}

void SSHClient::InitializeSFTPPool() {
  if (pool_initialized) {
    return;
  }

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [POOL] Initializing SFTP session pool with " << pool_size
              << " sessions..." << std::endl;
  }

  auto pool_start = std::chrono::steady_clock::now();

  for (size_t i = 0; i < pool_size; i++) {
    LIBSSH2_SFTP *sftp = libssh2_sftp_init(session);
    if (!sftp) {
      // Clean up any sessions we already created
      CleanupSFTPPool();
      throw IOException("Failed to initialize SFTP session for pool");
    }
    sftp_pool.push(sftp);
  }

  pool_initialized = true;

  auto pool_end = std::chrono::steady_clock::now();
  auto pool_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     pool_end - pool_start)
                     .count();

  if (IsDebugLoggingEnabled()) {
    std::cerr << "  [POOL] Initialized " << pool_size << " SFTP sessions in "
              << pool_ms << "ms" << std::endl;
  }
}

void SSHClient::CleanupSFTPPool() {
  std::lock_guard<std::mutex> lock(pool_mutex);

  while (!sftp_pool.empty()) {
    LIBSSH2_SFTP *sftp = sftp_pool.front();
    sftp_pool.pop();
    libssh2_sftp_shutdown(sftp);
  }

  pool_initialized = false;
}

LIBSSH2_SFTP *SSHClient::BorrowSFTPSession() {
  std::unique_lock<std::mutex> lock(pool_mutex);

  SSHFS_LOG("  [POOL] BorrowSFTPSession called, pool has " << sftp_pool.size()
                                                           << " sessions");

  // Lazy pool initialization on first use
  if (!pool_initialized) {
    lock.unlock();
    InitializeSFTPPool();
    lock.lock();
  }

  // Wait for an available session from the pool
  // This ensures serialized access to the single SFTP session
  SSHFS_LOG("  [POOL] Waiting for available session from pool");
  pool_cv.wait(lock, [this]() { return !sftp_pool.empty(); });

  SSHFS_LOG("  [POOL] Borrowing existing session from pool");
  LIBSSH2_SFTP *sftp = sftp_pool.front();
  sftp_pool.pop();
  return sftp;
}

void SSHClient::ReturnSFTPSession(LIBSSH2_SFTP *sftp) {
  std::lock_guard<std::mutex> lock(pool_mutex);
  sftp_pool.push(sftp);
  pool_cv.notify_one();
}

// Capability detection

void SSHClient::DetectCapabilities() {
  // Test if we can execute SSH commands (channel exec)
  // Some SFTP-only servers don't support command execution

  try {
    // Try to execute a simple command that should work on any Unix system
    LIBSSH2_CHANNEL *channel;
    while ((channel = libssh2_channel_open_session(session)) == nullptr &&
           libssh2_session_last_error(session, nullptr, nullptr, 0) ==
               LIBSSH2_ERROR_EAGAIN) {
      // Wait for non-blocking mode
    }

    if (!channel) {
      // Can't open channel = SFTP-only server
      SSHFS_LOG("  [DETECT] Server does not support SSH command execution "
                "(SFTP-only mode)");
      supports_commands = false;
      return;
    }

    // Try to execute a simple test command
    // Use 'pwd' instead of ':' because restricted shells (like Hetzner Storage
    // Box) may not support ':' but do support basic commands like pwd
    int rc = libssh2_channel_exec(channel, "pwd");
    if (rc != 0) {
      libssh2_channel_free(channel);
      SSHFS_LOG("  [DETECT] Server does not support command execution "
                "(SFTP-only mode)");
      supports_commands = false;
      return;
    }

    // Read and discard any output
    char buffer[256];
    while (libssh2_channel_read(channel, buffer, sizeof(buffer)) > 0) {
      // Drain output
    }

    libssh2_channel_send_eof(channel);
    libssh2_channel_wait_eof(channel);
    libssh2_channel_wait_closed(channel);

    // Check exit status - SFTP-only servers may accept the exec but return
    // non-zero exit status
    int exit_status = libssh2_channel_get_exit_status(channel);
    libssh2_channel_free(channel);

    if (exit_status != 0) {
      // Command failed - likely SFTP-only server
      SSHFS_LOG("  [DETECT] Command execution returned non-zero exit status ("
                << exit_status << "), assuming SFTP-only mode");
      supports_commands = false;
      return;
    }

    // Success - server supports command execution
    SSHFS_LOG("  [DETECT] Server supports SSH command execution");
    supports_commands = true;

  } catch (...) {
    // If anything goes wrong, assume SFTP-only
    SSHFS_LOG("  [DETECT] Error testing commands, assuming SFTP-only mode");
    supports_commands = false;
  }
}

// SFTP-only operations (no SSH command execution)

void SSHClient::CreateDirectorySFTP(const std::string &remote_path) {
  // Recursively create directories using SFTP mkdir
  // Split path by '/' and create each directory level
  // Note: Called from UploadChunk which already holds upload_mutex

  LIBSSH2_SFTP *sftp = BorrowSFTPSession();

  try {
    std::string path;
    size_t pos = 0;
    size_t start = 0;

    // Skip leading slash if present
    if (!remote_path.empty() && remote_path[0] == '/') {
      start = 1;
      path = "/";
    }

    while (true) {
      pos = remote_path.find('/', start);
      std::string component;

      if (pos == std::string::npos) {
        // Last component
        component = remote_path.substr(start);
      } else {
        component = remote_path.substr(start, pos - start);
      }

      if (!component.empty()) {
        path += component;

        // Try to create directory (ignore if it already exists)
        int rc = libssh2_sftp_mkdir(
            sftp, path.c_str(),
            LIBSSH2_SFTP_S_IRWXU | LIBSSH2_SFTP_S_IRGRP | LIBSSH2_SFTP_S_IXGRP |
                LIBSSH2_SFTP_S_IROTH | LIBSSH2_SFTP_S_IXOTH);

        if (rc != 0) {
          unsigned long err_code = libssh2_sftp_last_error(sftp);
          // Ignore "file exists" errors
          if (err_code != LIBSSH2_FX_FILE_ALREADY_EXISTS) {
            ReturnSFTPSession(sftp);
            throw IOException(
                "Failed to create directory: %s (SFTP error: %lu)", path,
                err_code);
          }
        }

        if (pos == std::string::npos) {
          break;
        }

        path += "/";
      }

      start = pos + 1;
      if (start >= remote_path.length()) {
        break;
      }
    }

    ReturnSFTPSession(sftp);
  } catch (...) {
    ReturnSFTPSession(sftp);
    throw;
  }
}

void SSHClient::RemoveDirectorySFTP(const std::string &remote_path) {
  LIBSSH2_SFTP *sftp = BorrowSFTPSession();

  try {
    int rc = libssh2_sftp_rmdir(sftp, remote_path.c_str());
    ReturnSFTPSession(sftp);

    if (rc != 0) {
      unsigned long err_code = libssh2_sftp_last_error(sftp);
      throw IOException("Failed to remove directory: %s (SFTP error: %lu)",
                        remote_path, err_code);
    }
  } catch (...) {
    ReturnSFTPSession(sftp);
    throw;
  }
}

void SSHClient::TruncateFileSFTP(const std::string &remote_path,
                                 int64_t new_size) {
  LIBSSH2_SFTP *sftp = BorrowSFTPSession();

  try {
    // Open file for writing
    LIBSSH2_SFTP_HANDLE *handle =
        libssh2_sftp_open(sftp, remote_path.c_str(), LIBSSH2_FXF_WRITE, 0);

    if (!handle) {
      ReturnSFTPSession(sftp);
      throw IOException("Failed to open file for truncate: %s", remote_path);
    }

    // Set file size using fsetstat
    LIBSSH2_SFTP_ATTRIBUTES attrs;
    memset(&attrs, 0, sizeof(attrs));
    attrs.filesize = new_size;
    attrs.flags = LIBSSH2_SFTP_ATTR_SIZE;

    int rc = libssh2_sftp_fsetstat(handle, &attrs);
    libssh2_sftp_close(handle);
    ReturnSFTPSession(sftp);

    if (rc != 0) {
      throw IOException("Failed to truncate file: %s to size %lld", remote_path,
                        new_size);
    }
  } catch (...) {
    ReturnSFTPSession(sftp);
    throw;
  }
}

size_t SSHClient::ReadBytesSFTP(const std::string &remote_path, char *buffer,
                                size_t offset, size_t length) {
  if (!buffer) {
    throw IOException("ReadBytesSFTP: buffer is null");
  }
  if (length == 0) {
    return 0;
  }

  auto read_start = std::chrono::steady_clock::now();

  // Borrow SFTP session from pool (shared across file handles)
  // This allows multiple file handles to share a small pool of sessions
  SSHFS_LOG("  [READ-SFTP] Borrowing SFTP session from pool for " << remote_path
                                                                  << "...");
  LIBSSH2_SFTP *sftp = BorrowSFTPSession();
  SSHFS_LOG("  [READ-SFTP] Session borrowed, opening file...");

  // Serialize SFTP operations - libssh2 SFTP sessions are NOT thread-safe
  // Multiple threads cannot use the same session concurrently
  std::lock_guard<std::mutex> lock(upload_mutex);

  try {
    // Open file for reading
    auto open_start = std::chrono::steady_clock::now();
    SSHFS_LOG("  [READ-SFTP] Opening " << remote_path << " for read...");
    LIBSSH2_SFTP_HANDLE *handle =
        libssh2_sftp_open(sftp, remote_path.c_str(), LIBSSH2_FXF_READ, 0);

    if (!handle) {
      int sftp_error = libssh2_sftp_last_error(sftp);
      char *err_msg = nullptr;
      int session_error =
          libssh2_session_last_error(session, &err_msg, nullptr, 0);
      throw IOException("Failed to open file for read: %s\n"
                        "  SFTP error: %d\n"
                        "  Session error: %d (%s)",
                        remote_path.c_str(), sftp_error, session_error,
                        err_msg ? err_msg : "Unknown error");
    }
    SSHFS_LOG("  [READ-SFTP] File opened successfully");
    auto open_end = std::chrono::steady_clock::now();
    auto open_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       open_end - open_start)
                       .count();

    // Seek to the offset
    auto seek_start = std::chrono::steady_clock::now();
    SSHFS_LOG("  [READ-SFTP] Seeking to offset " << offset << "...");
    libssh2_sftp_seek64(handle, offset);
    SSHFS_LOG("  [READ-SFTP] Seek complete, starting read of " << length
                                                               << " bytes...");
    auto seek_end = std::chrono::steady_clock::now();
    auto seek_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       seek_end - seek_start)
                       .count();

    // Read data in chunks to avoid timeouts on large reads
    // libssh2_sftp_read() can timeout for very large requests
    // Reading in 32KB chunks matches libssh2's natural packet size
    auto actual_read_start = std::chrono::steady_clock::now();
    size_t total_read = 0;
    const size_t READ_CHUNK_SIZE = 32 * 1024; // 32KB chunks

    while (total_read < length) {
      // Calculate how much to read in this iteration
      size_t bytes_remaining = length - total_read;
      size_t bytes_to_read = std::min(bytes_remaining, READ_CHUNK_SIZE);

      SSHFS_LOG("  [READ-SFTP] Reading chunk: " << total_read << "/" << length
                                                << " (" << bytes_to_read
                                                << " bytes)...");
      ssize_t nread =
          libssh2_sftp_read(handle, buffer + total_read, bytes_to_read);
      SSHFS_LOG("  [READ-SFTP] Chunk read returned: " << nread << " bytes");

      if (nread == LIBSSH2_ERROR_EAGAIN) {
        SSHFS_LOG("  [READ-SFTP] Got EAGAIN, retrying...");
        continue; // Retry
      } else if (nread < 0) {
        libssh2_sftp_close(handle);
        throw IOException("Failed to read from SFTP file: %s (error: %zd)",
                          remote_path, nread);
      } else if (nread == 0) {
        // End of file
        break;
      }

      total_read += nread;
    }
    auto actual_read_end = std::chrono::steady_clock::now();
    auto actual_read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              actual_read_end - actual_read_start)
                              .count();

    // Close handle and return session to pool
    auto close_start = std::chrono::steady_clock::now();
    libssh2_sftp_close(handle);
    ReturnSFTPSession(sftp); // Return to pool for reuse
    auto close_end = std::chrono::steady_clock::now();
    auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        close_end - close_start)
                        .count();

    auto read_end = std::chrono::steady_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        read_end - read_start)
                        .count();

    SSHFS_LOG("  [READ-SFTP] offset="
              << offset << " length=" << length << " read=" << total_read
              << " bytes in " << total_ms << "ms ("
              << (total_read / 1024.0 / 1024.0) / (total_ms / 1000.0)
              << " MB/s)");
    SSHFS_LOG("    [BREAKDOWN] open=" << open_ms << "ms, seek=" << seek_ms
                                      << "ms, actual_read=" << actual_read_ms
                                      << "ms, close=" << close_ms << "ms");

    return total_read;
  } catch (...) {
    // Return session to pool on error - critical to avoid leaking sessions!
    ReturnSFTPSession(sftp);
    throw;
  }
}

} // namespace duckdb
