#pragma once

#include "capabilities/capability/RegisteredTypeTemplate.h"

namespace pangrova::capability {

// Registration is the source of signatures, variables and formulas. This
// projection contains no SQL function names, overload lists or return rules.
protocol::v2::FunctionSignatureContract projectRegisteredSignature(
    const facebook::velox::exec::FunctionSignature& signature,
    const RegisteredTypeTemplate& types);

}  // namespace pangrova::capability
