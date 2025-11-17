#include "ssh_secrets.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static unique_ptr<BaseSecret>
CreateSSHSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
  // Create a KeyValueSecret to store SSH credentials
  auto secret = make_uniq<KeyValueSecret>(input.scope, input.type,
                                          input.provider, input.name);

  // Add all the options from the input to the secret
  for (const auto &option : input.options) {
    secret->secret_map[option.first] = option.second;
  }

  return std::move(secret);
}

void CreateSSHSecretFunctions::Register(ExtensionLoader &loader) {
  // Register SSH secret type
  SecretType secret_type;
  secret_type.name = "ssh";
  secret_type.deserializer = nullptr;
  secret_type.default_provider = "config";

  loader.RegisterSecretType(secret_type);

  // Create CreateSecretFunction for SSH
  CreateSecretFunction ssh_config_fun;
  ssh_config_fun.secret_type = "ssh";
  ssh_config_fun.provider = "config";
  ssh_config_fun.function = CreateSSHSecretFromConfig;

  // Define parameters (authentication and connection only)
  // Performance tuning should be done via SET statements (e.g., SET
  // ssh_keepalive=60)
  ssh_config_fun.named_parameters["username"] = LogicalType::VARCHAR;
  ssh_config_fun.named_parameters["password"] = LogicalType::VARCHAR;
  ssh_config_fun.named_parameters["key_path"] = LogicalType::VARCHAR;
  ssh_config_fun.named_parameters["use_agent"] = LogicalType::BOOLEAN;
  ssh_config_fun.named_parameters["port"] = LogicalType::INTEGER;
  ssh_config_fun.named_parameters["host"] = LogicalType::VARCHAR;
  ssh_config_fun.named_parameters["hostname"] = LogicalType::VARCHAR;

  // Register the function with the secret manager
  auto &db = loader.GetDatabaseInstance();
  auto &secret_manager = db.GetSecretManager();
  secret_manager.RegisterSecretFunction(ssh_config_fun,
                                        OnCreateConflict::ERROR_ON_CONFLICT);
}

} // namespace duckdb
