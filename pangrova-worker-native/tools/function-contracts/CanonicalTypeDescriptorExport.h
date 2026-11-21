#pragma once

#include <string>

namespace pangrova::capability {

// Canonical descriptor graph, not a second list of supported physical types.
std::string exportCanonicalTypeDescriptor();
// Same generated bridge used by the actual Native factory; not a function inventory.
std::string exportCanonicalPhysicalTypeProjections();

}  // namespace pangrova::capability
