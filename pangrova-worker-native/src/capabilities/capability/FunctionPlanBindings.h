#pragma once

#include <string>
#include <unordered_map>

#include "pangrova/protocol/v2/capability.pb.h"
#include "substrait/plan.pb.h"

namespace pangrova::capability {

// Sole SQL declarations supply names and URNs; registered signature support is
// independently checked against the task's actual immutable factory owner.
const protocol::v2::SubstraitFunctionDefinition& declaredPlanFunction(
    protocol::v2::FunctionKind kind, const std::string& name);

const protocol::v2::FunctionCatalogManifest& declaredPlanFunctions();

const protocol::v2::SubstraitFunctionDefinition& declaredRewriteTarget(
    const protocol::v2::FunctionRewriteNode& node);

std::unordered_map<uint32_t, protocol::v2::SubstraitFunctionDefinition>
bindPlanFunctionDeclarations(const substrait::Plan& plan);

}  // namespace pangrova::capability
