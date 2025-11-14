#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {

class ExtensionLoader;

class SshfsExtension : public Extension {
public:
  void Load(ExtensionLoader &loader) override;
  std::string Name() override;
  std::string Version() const override;
};

} // namespace duckdb
