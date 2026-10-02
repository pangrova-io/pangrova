#pragma once

#include <string>

namespace pangrova::capability {

// Inspect actual finalized registrations. This never invokes a function body.
std::string exportRegisteredFunctions();

}  // namespace pangrova::capability
