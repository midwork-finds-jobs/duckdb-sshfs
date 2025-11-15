#pragma once

#include "duckdb.hpp"
#include <map>
#include <string>
#include <vector>

namespace duckdb {

struct SSHHostConfig {
  std::string hostname;
  std::string user;
  int port = 22;
  std::string identity_file;
  bool found = false;
};

class SSHConfigParser {
public:
  // Parse SSH config files and lookup host configuration
  static SSHHostConfig LookupHost(const std::string &host_alias);

private:
  // Parse a single SSH config file
  static std::map<std::string, SSHHostConfig> ParseConfigFile(
      const std::string &config_path,
      const std::map<std::string, SSHHostConfig> &existing_configs = {});

  // Expand tilde (~) in paths
  static std::string ExpandPath(const std::string &path);

  // Trim whitespace from string
  static std::string Trim(const std::string &str);

  // Check if a host pattern matches a host alias
  static bool HostMatches(const std::string &pattern,
                          const std::string &host_alias);
};

} // namespace duckdb
