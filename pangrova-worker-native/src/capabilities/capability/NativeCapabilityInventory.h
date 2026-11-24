#pragma once

#include "pangrova/protocol/v2/capability.pb.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::capability {

/** Exact protocol identity shared by the startup inventory and Native admission. */
const protocol::v2::ProtocolVersion& canonicalProtocolVersion();

// Runtime readiness and protocol identity, projected once from the same actual
// function owner that the assembler freezes. No factory is instantiated.
protocol::v2::CapabilityInventory runtimeCapabilityInventory(
    const facebook::velox::exec::FunctionRegistrySnapshot& registry);

}  // namespace pangrova::capability
