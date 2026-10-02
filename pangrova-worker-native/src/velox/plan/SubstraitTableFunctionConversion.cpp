// Registered table functions consume the one complete canonical output schema.
#include "velox/plan/VeloxPlanConverter.h"

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/TableFunctionNode.h"
#include "velox/runtime/NativeEngineLifecycle.h"
#include "velox/runtime/TableFunctionRegistry.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;

void requireInputPrefix(const RowTypePtr& declared, const RowTypePtr& input) {
  if (declared->size() < input->size()) {
    throw std::runtime_error("Table function complete output schema is missing input prefix");
  }
  for (size_t ordinal = 0; ordinal < input->size(); ++ordinal) {
    if (declared->nameOf(ordinal) != input->nameOf(ordinal) ||
        !(*declared->childAt(ordinal) == *input->childAt(ordinal))) {
      throw std::runtime_error("Table function output prefix does not match declared input schema");
    }
  }
}
}  // namespace

std::shared_ptr<const facebook::velox::core::PlanNode>
VeloxPlanConverter::convertExtensionMultiRel(
    const substrait::ExtensionMultiRel& relation,
    const std::vector<split::SplitConfig>& splits) {
  if (relation.inputs_size() > 1) {
    throw std::runtime_error("Table function supports zero or one declared input");
  }
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  if (!relation.has_detail() || !relation.detail().UnpackTo(&envelope) ||
      envelope.schema_version() != 1 || !envelope.has_table_function()) {
    throw std::runtime_error("ExtensionMultiRel requires canonical v2 table-function detail");
  }
  const auto& detail = envelope.table_function();
  const auto& functionName = detail.invocation().capability().capability_id();
  if (detail.plan_node_id().empty() || functionName.empty()) {
    throw std::runtime_error("Table function requires plan_node_id and capability_id");
  }
  auto* registry = pangrova::jni::NativeEngineLifecycle::instance().tableFunctionRegistry();
  if (!registry || !registry->isRegistered(functionName)) {
    throw std::runtime_error("Unsupported table function: " + functionName);
  }
  if (!detail.has_output_schema()) {
    throw std::runtime_error("Table function missing complete output_schema: " + functionName);
  }
  const auto declared = convertNamedStruct(detail.output_schema());
  auto input = relation.inputs_size() == 1 ? convertRel(relation.inputs(0), splits) : nullptr;
  const auto context = input ? input->outputType() : ROW({}, {});
  if (input) requireInputPrefix(declared, logicalNodeType(input));
  std::vector<core::TypedExprPtr> arguments;
  for (const auto& argument : detail.invocation().arguments()) {
    if (!argument.has_expression()) {
      throw std::runtime_error("Table-function argument is missing expression");
    }
    arguments.push_back(convertExpression(argument.expression(), context));
  }
  // The schema is already complete; no second input-prefix append or decoder.
  auto physical = physicalRowType(declared);
  if (input) {
    return std::make_shared<TableFunctionNode>(detail.plan_node_id(), functionName,
        std::move(arguments), std::move(physical), std::move(input));
  }
  return std::make_shared<TableFunctionNode>(detail.plan_node_id(), functionName,
      std::move(arguments), std::move(physical));
}
}  // namespace pangrova::velox

