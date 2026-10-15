// Registered table functions consume the one complete canonical output schema.
#include "velox/plan/VeloxPlanConverter.h"

#include "StructuralContractIdentities.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;
}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionMultiRel(
    const substrait::ExtensionMultiRel& relation,
    const std::vector<split::SplitConfig>& splits) {
  (void)relation;
  (void)splits;
  throw std::runtime_error(
      "GENERATE_SERIES requires a zero-input ExtensionLeafRel");
}

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertTableFunctionDetail(
    const pangrova::protocol::v2::TableFunctionDetail& detail) {
  const auto& reference = detail.invocation().capability();
  const auto& identity = pangrova::capability::generated::pangrova_table_generate_series;
  const std::string signature(reinterpret_cast<const char*>(identity.signature.data()),
                              identity.signature.size());
  const auto& functionName = reference.capability_id();
  if (detail.plan_node_id().empty() || functionName.empty()) {
    throw std::runtime_error("Table function requires plan_node_id and capability_id");
  }
  if (functionName != identity.id || reference.semantic_version() != identity.version ||
      reference.signature_sha256() != signature ||
      !reference.implementation_build_id().empty()) {
    throw std::runtime_error("Table function capability identity mismatch: " + functionName);
  }
  if (detail.invocation().arguments_size() < 2 ||
      detail.invocation().arguments_size() > 3 ||
      detail.invocation().options_size() != 0) {
    throw std::runtime_error("GENERATE_SERIES requires 2 or 3 arguments and no options");
  }
  constexpr const char* names[] = {"start", "stop", "step"};
  for (int i = 0; i < detail.invocation().arguments_size(); ++i) {
    if (detail.invocation().arguments(i).name() != names[i]) {
      throw std::runtime_error("GENERATE_SERIES arguments must use canonical names and order");
    }
  }
  auto* registry = pangrova::jni::NativeEngineLifecycle::instance().tableFunctionRegistry();
  if (!registry || !registry->isRegistered(functionName)) {
    throw std::runtime_error("Unsupported table function: " + functionName);
  }
  if (!detail.has_output_schema()) {
    throw std::runtime_error("Table function missing complete output_schema: " + functionName);
  }
  const auto declared = convertNamedStruct(detail.output_schema());
  if (declared->size() != 1 || declared->nameOf(0) != "val" ||
      !(*declared->childAt(0) == *BIGINT())) {
    throw std::runtime_error("GENERATE_SERIES output must be one BIGINT val column");
  }
  std::vector<core::TypedExprPtr> arguments;
  for (const auto& argument : detail.invocation().arguments()) {
    if (!argument.has_expression()) {
      throw std::runtime_error("Table-function argument is missing expression");
    }
    arguments.push_back(convertExpression(argument.expression(), ROW({}, {})));
    if (arguments.back()->type()->kind() != TypeKind::BIGINT &&
        arguments.back()->type()->kind() != TypeKind::INTEGER) {
      throw std::runtime_error("GENERATE_SERIES arguments must be integer-typed");
    }
    if (!std::dynamic_pointer_cast<const core::ConstantTypedExpr>(arguments.back())) {
      throw std::runtime_error("GENERATE_SERIES arguments must be constants");
    }
  }
  // The source has no input; its one declared output is the complete schema.
  auto physical = physicalRowType(declared);
  return std::make_shared<TableFunctionNode>(detail.plan_node_id(), functionName,
      std::move(arguments), std::move(physical));
}
}  // namespace pangrova::velox

