#pragma once

#include <string>

#include "substrait/algebra.pb.h"

namespace pangrova::capability {

// FunctionOption names and accepted values originate in the sole declaration.
bool registeredWindowIgnoreNulls(const std::string& implementation,
    const google::protobuf::RepeatedPtrField<substrait::FunctionOption>& options);

}  // namespace pangrova::capability
