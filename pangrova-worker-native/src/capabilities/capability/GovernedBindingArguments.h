#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "pangrova/protocol/v2/execution_binding.pb.h"
#include "StructuralContractIdentities.h"

namespace pangrova::capability {

std::string governedBindingArgument(
    const generated::GovernedArgument& argument,
    const protocol::v2::OperatorBinding& binding,
    std::string_view invocation_id,
    uint32_t output_vector_dimension);

// Exact layout and protobuf paths originate in the sole generated declaration.
std::vector<std::string> governedBindingArguments(
    std::string_view layout,
    const protocol::v2::OperatorBinding& binding,
    std::string_view invocation_id,
    uint32_t output_vector_dimension);

}  // namespace pangrova::capability
