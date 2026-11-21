// Exact function binding through the existing Velox registry.
#include "engine/velox/plan/VeloxPlanConverter.h"

#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "capabilities/capability/LogicalTypeContract.h"
#include "capabilities/capability/GovernedBindingArguments.h"
#include "capabilities/capability/RegisteredFunctionBinding.h"
#include "StructuralContractIdentities.h"
#include "velox/core/Expressions.h"
#include "velox/functions/FunctionRegistry.h"

namespace pangrova::velox {
namespace {

bool isGovernedAiFunction(const std::string& name) {
  return pangrova::capability::generated::governedLayoutForImplementation(name) == "AI_MODEL";
}

std::string constantVarchar(
    const std::shared_ptr<const facebook::velox::core::ITypedExpr>& expression,
    const char* field) {
  auto constant = std::dynamic_pointer_cast<
      const facebook::velox::core::ConstantTypedExpr>(expression);
  if (!constant || constant->hasValueVector() || constant->value().isNull() ||
      constant->value().kind() != facebook::velox::TypeKind::VARCHAR) {
    throw std::runtime_error(
        std::string("VeloxPlanConverter: AI ") + field +
        " must be a non-null VARCHAR literal");
  }
  auto value = constant->value().value<facebook::velox::StringView>();
  if (value.size() == 0) {
    throw std::runtime_error(
        std::string("VeloxPlanConverter: AI ") + field + " is blank");
  }
  return std::string(value.data(), value.size());
}

std::shared_ptr<const facebook::velox::core::ConstantTypedExpr>
varcharConstant(const std::string& value) {
  return std::make_shared<facebook::velox::core::ConstantTypedExpr>(
      facebook::velox::VARCHAR(), facebook::velox::variant(value));
}

void appendAiBindingArguments(
    const pangrova::protocol::v2::OperatorBinding& binding,
    uint32_t expected_vector_dimension,
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args) {
  const auto values = pangrova::capability::governedBindingArguments("AI_MODEL", binding,
      constantVarchar(args.back(), "invocation_id"), expected_vector_dimension);
  // Planner already appended the sole invocation-id field. Append the remaining declared fields.
  for (size_t ordinal = 1; ordinal < values.size(); ++ordinal) args.push_back(varcharConstant(values[ordinal]));
}

}  // namespace

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::checkedScalarCall(
    const std::string& name,
    const facebook::velox::TypePtr& output_type,
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args) {
  return pangrova::capability::bindRegisteredScalar(*function_registry_, name, output_type, std::move(args));
}

void VeloxPlanConverter::appendGovernedAiArguments(
    const std::string& name,
    const substrait::Expression_ScalarFunction& function,
    const facebook::velox::TypePtr& output_type,
    std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>>& args) {
  if (args.size() < 2) {
    throw std::runtime_error(
        "VeloxPlanConverter: governed AI invocation is missing "
        "model or invocation identity");
  }
  const std::string invocation_id =
      constantVarchar(args.back(), "invocation_id");
  const std::string sql_model = constantVarchar(args.front(), "model");
  pangrova::protocol::v2::CapabilityDescriptor expected;
  if (name == "ai_embed") {
    expected = pangrova::capability::aiEmbeddingDescriptor();
  } else if (name == "ai_describe_image" ||
             name == "ai_parse_document" ||
             name == "ai_transcribe" || name == "ai_ocr" ||
             (name == "ai_complete" && args.size() >= 4 &&
              args[2]->type()->kind() ==
                  facebook::velox::TypeKind::ROW)) {
    expected = pangrova::capability::aiMultimodalGenerationDescriptor();
  } else {
    expected = pangrova::capability::aiTextGenerationDescriptor();
  }
  expected.mutable_ref()->clear_implementation_build_id();
  const auto& binding = requireOperatorBinding(
      invocation_id, expected.ref());
  const auto& policy = binding.execution_policy();
  if (!binding.has_model() ||
      binding.model().model().id() != sql_model ||
      binding.model().model().version().empty() ||
      binding.model().model().signature_sha256().size() != 32 ||
      binding.artifacts_size() != 0 || binding.policies_size() != 0 ||
      binding.has_output_artifact() ||
      policy.coverage_policy() !=
          pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE ||
      policy.fallback_policy() !=
          pangrova::protocol::v2::FALLBACK_POLICY_FORBID ||
      policy.retry_class() !=
          pangrova::protocol::v2::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY ||
      policy.idempotency_key().empty() || !binding.has_resources() ||
      binding.resources().provider_concurrency() == 0 ||
      binding.resources().provider_items() == 0 || binding.resources().provider_attempts() == 0 ||
      binding.resources().provider_request_bytes() == 0 || binding.resources().provider_response_bytes() == 0 ||
      !binding.model().has_request_limits() || binding.model().request_limits().max_input_tokens() == 0 ||
      binding.model().request_limits().max_request_bytes() == 0 ||
      binding.model().request_limits().max_response_bytes() == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: governed AI OperatorBinding policy/model "
        "mismatch");
  }
  uint32_t expected_vector_dimension = 0;
  if (!binding.model().has_output_type() ||
      !binding.model().output_type().has_substrait_type() ||
      !convertType(binding.model().output_type().substrait_type())
           ->equivalent(*output_type)) {
    throw std::runtime_error(
        "VeloxPlanConverter: governed AI expression output differs "
        "from its exact model contract: expression=" +
        function.output_type().ShortDebugString() + " model=" +
        binding.model().output_type().substrait_type().ShortDebugString());
  }
  if (name == "ai_embed") {
    expected_vector_dimension =
        pangrova::capability::requireFloatVectorDimension(
            binding.model().output_type());
    if (output_type->kind() != facebook::velox::TypeKind::ARRAY ||
        output_type->childAt(0)->kind() !=
            facebook::velox::TypeKind::REAL) {
      throw std::runtime_error(
          "VeloxPlanConverter: AI_EMBED must use the canonical "
          "FLOAT32 VECTOR physical type");
    }
  }
  appendAiBindingArguments(binding, expected_vector_dimension, args);
}

std::shared_ptr<const facebook::velox::core::ITypedExpr>
VeloxPlanConverter::convertScalarFunction(
    const substrait::Expression_ScalarFunction& function,
    const facebook::velox::RowTypePtr& input_type) {
  requireFunctionKind(function.function_reference(), protocol::v2::FUNCTION_KIND_SCALAR);
  const auto anchor = function_map_.find(function.function_reference());
  if (anchor == function_map_.end()) {
    throw std::runtime_error(
        "VeloxPlanConverter: unbound scalar function anchor " +
        std::to_string(function.function_reference()));
  }
  const auto& name = anchor->second;
  auto output_type = convertType(function.output_type());
  std::vector<std::shared_ptr<const facebook::velox::core::ITypedExpr>> args;
  args.reserve(function.arguments_size());
  for (const auto& argument : function.arguments()) {
    if (!argument.has_value()) {
      throw std::runtime_error(
          "VeloxPlanConverter: scalar arguments require value expressions");
    }
    args.push_back(convertExpression(argument.value(), input_type));
  }
  if (isGovernedAiFunction(name)) {
    appendGovernedAiArguments(name, function, output_type, args);
  }
  return checkedScalarCall(name, output_type, std::move(args));
}

}  // namespace pangrova::velox
