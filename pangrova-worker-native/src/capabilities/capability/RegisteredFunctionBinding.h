#pragma once

#include <string>
#include <vector>

#include "velox/core/Expressions.h"

namespace facebook::velox::exec { class FunctionRegistrySnapshot; }

namespace pangrova::capability {

using FunctionArguments = std::vector<facebook::velox::core::TypedExprPtr>;

// Exact binding against the running engine. No implicit conversions or
// alternate overload is selected to justify a caller's declared result type.
facebook::velox::core::CallTypedExprPtr bindRegisteredScalar(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments);

struct RegisteredAggregateBinding {
  facebook::velox::core::CallTypedExprPtr call;
  facebook::velox::TypePtr intermediate;
};

// The arguments here are ORIGINAL raw inputs, not intermediate-state columns.
// State phases require separate, authoritative raw-input provenance.
RegisteredAggregateBinding bindRegisteredRawAggregate(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments, bool intermediateOutput);

RegisteredAggregateBinding bindRegisteredStateAggregate(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments states, const std::vector<facebook::velox::TypePtr>& rawTypes);

facebook::velox::core::CallTypedExprPtr bindRegisteredWindow(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    const std::string& name, const facebook::velox::TypePtr& output,
    FunctionArguments arguments);

}  // namespace pangrova::capability
