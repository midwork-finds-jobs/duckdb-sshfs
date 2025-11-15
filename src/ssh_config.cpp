#include "ssh_config.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <pwd.h>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace duckdb {

std::string SSHConfigParser::Trim(const std::string &str) {
  size_t first = str.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }
  size_t last = str.find_last_not_of(" \t\r\n");
  return str.substr(first, (last - first + 1));
}

std::string SSHConfigParser::ExpandPath(const std::string &path) {
  if (path.empty() || path[0] != '~') {
    return path;
  }

  const char *home = getenv("HOME");
  if (!home) {
    struct passwd *pw = getpwuid(getuid());
    if (pw) {
      home = pw->pw_dir;
    }
  }

  if (!home) {
    return path; // Cannot expand, return as-is
  }

  if (path.length() == 1 || path[1] == '/') {
    return std::string(home) + path.substr(1);
  }

  return path; // ~user/path format not supported
}

bool SSHConfigParser::HostMatches(const std::string &pattern,
                                  const std::string &host_alias) {
  // Simple pattern matching - support * wildcard
  if (pattern == "*") {
    return true;
  }

  // Exact match
  if (pattern == host_alias) {
    return true;
  }

  // Wildcard matching (simple implementation)
  if (pattern.find('*') != std::string::npos) {
    // Convert pattern to regex-like matching
    // For now, only support suffix matching like "*.example.com"
    if (pattern[0] == '*') {
      std::string suffix = pattern.substr(1);
      if (host_alias.length() >= suffix.length()) {
        return host_alias.compare(host_alias.length() - suffix.length(),
                                  suffix.length(), suffix) == 0;
      }
    }
    // Prefix matching like "internal-*"
    if (pattern[pattern.length() - 1] == '*') {
      std::string prefix = pattern.substr(0, pattern.length() - 1);
      return host_alias.compare(0, prefix.length(), prefix) == 0;
    }
  }

  return false;
}

std::map<std::string, SSHHostConfig> SSHConfigParser::ParseConfigFile(
    const std::string &config_path,
    const std::map<std::string, SSHHostConfig> &existing_configs) {

  std::map<std::string, SSHHostConfig> configs = existing_configs;

  std::ifstream file(ExpandPath(config_path));
  if (!file.is_open()) {
    return configs; // File doesn't exist or can't be read, return what we have
  }

  std::string line;
  std::string current_host;
  SSHHostConfig current_config;

  while (std::getline(file, line)) {
    // Remove comments
    size_t comment_pos = line.find('#');
    if (comment_pos != std::string::npos) {
      line = line.substr(0, comment_pos);
    }

    line = Trim(line);
    if (line.empty()) {
      continue;
    }

    // Parse key-value pairs
    std::istringstream iss(line);
    std::string key, value;
    iss >> key >> std::ws;
    std::getline(iss, value);
    value = Trim(value);

    // Convert key to lowercase for case-insensitive matching
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);

    if (key == "host") {
      // Save previous host config if any
      if (!current_host.empty()) {
        // Only save if not already defined (first match wins)
        if (configs.find(current_host) == configs.end()) {
          configs[current_host] = current_config;
          configs[current_host].found = true;
        }
      }

      // Start new host config
      current_host = value;
      current_config = SSHHostConfig();

    } else if (key == "hostname") {
      current_config.hostname = value;

    } else if (key == "user") {
      current_config.user = value;

    } else if (key == "port") {
      try {
        current_config.port = std::stoi(value);
      } catch (...) {
        // Invalid port, ignore
      }

    } else if (key == "identityfile") {
      // Take the first identity file (SSH tries multiple)
      if (current_config.identity_file.empty()) {
        current_config.identity_file = ExpandPath(value);
      }

    } else if (key == "include") {
      // Handle Include directives
      std::string include_path = ExpandPath(value);
      configs = ParseConfigFile(include_path, configs);
    }
  }

  // Save last host config
  if (!current_host.empty()) {
    if (configs.find(current_host) == configs.end()) {
      configs[current_host] = current_config;
      configs[current_host].found = true;
    }
  }

  return configs;
}

SSHHostConfig SSHConfigParser::LookupHost(const std::string &host_alias) {
  SSHHostConfig result;

  // Parse SSH config files in order of precedence
  // 1. User config (~/.ssh/config)
  // 2. System config (/etc/ssh/ssh_config)

  std::map<std::string, SSHHostConfig> all_configs;

  // Parse user config first (higher precedence)
  std::string user_config = ExpandPath("~/.ssh/config");
  all_configs = ParseConfigFile(user_config, all_configs);

  // Parse system config
  all_configs = ParseConfigFile("/etc/ssh/ssh_config", all_configs);

  // Look for exact match first
  auto it = all_configs.find(host_alias);
  if (it != all_configs.end()) {
    return it->second;
  }

  // Look for pattern matches (Host * entries)
  for (const auto &entry : all_configs) {
    if (HostMatches(entry.first, host_alias)) {
      // Merge configs (earlier matches take precedence)
      if (!result.found) {
        result = entry.second;
      } else {
        // Only fill in missing values
        if (result.hostname.empty()) {
          result.hostname = entry.second.hostname;
        }
        if (result.user.empty()) {
          result.user = entry.second.user;
        }
        if (result.port == 22 && entry.second.port != 22) {
          result.port = entry.second.port;
        }
        if (result.identity_file.empty()) {
          result.identity_file = entry.second.identity_file;
        }
      }
    }
  }

  return result;
}

} // namespace duckdb
