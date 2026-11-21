#pragma once

#include <string>
#include <memory>

namespace facebook::velox::exec {
class FunctionRegistrySnapshot;
}

namespace pangrova::capability {

// Inspect one owned registration snapshot. This never invokes a function body
// or re-reads a live registry while emitting the build intermediate.
std::string exportRegisteredFunctions(
    const std::shared_ptr<const facebook::velox::exec::FunctionRegistrySnapshot>& snapshot);

}  // namespace pangrova::capability
