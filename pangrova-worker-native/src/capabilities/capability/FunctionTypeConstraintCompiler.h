#pragma once

#include <string_view>
#include <unordered_set>

#include "pangrova/protocol/v2/capability.pb.h"

namespace pangrova::capability {

// Compiles actual registered calculator expressions; never parses SQL,
// substitutes a formula by function name, or executes a factory.
protocol::v2::FunctionTypeFormula compileRegisteredTypeConstraint(
    std::string_view source, const std::unordered_set<std::string>& integerVariables);

}  // namespace pangrova::capability
