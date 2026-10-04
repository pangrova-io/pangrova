#pragma once

#include <string>
#include <memory>

namespace facebook::velox::exec {
class FunctionRegistrySnapshot;
}

namespace pangrova::capability {

// Build-tool differential oracle. Requests use the existing registered type
// signature grammar, not SQL or a second cross-runtime type protocol.
std::string resolveRegisteredScalarOracle(
    const std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot>& snapshot,
    const std::string& request_json);

}  // namespace pangrova::capability
