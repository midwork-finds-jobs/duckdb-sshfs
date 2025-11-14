#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

struct CreateSSHSecretFunctions {
  static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
