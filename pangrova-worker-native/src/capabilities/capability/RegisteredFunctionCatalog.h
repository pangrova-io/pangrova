#pragma once

#include "pangrova/protocol/v2/capability.pb.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::capability {

// Projects actual owned entries, preserving runtime traversal and opaque types.
// SQL exposure and startup publication are separate validated responsibilities.
void appendRegisteredFunctionCatalog(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry,
    protocol::v2::FunctionCatalogManifest& manifest);

}  // namespace pangrova::capability
