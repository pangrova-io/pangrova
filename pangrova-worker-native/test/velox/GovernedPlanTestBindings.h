#pragma once

#include "capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/execution_binding.pb.h"
#include "substrait/type.pb.h"

namespace pangrova::velox::test {

inline pangrova::protocol::v2::OperatorBinding governedCompleteBinding(const substrait::Type& outputType) {
  pangrova::protocol::v2::OperatorBinding binding;
  binding.set_plan_node_id("ai-invoke-1");
  *binding.mutable_capability() =
      pangrova::capability::aiTextGenerationDescriptor().ref();
  binding.mutable_capability()->clear_implementation_build_id();
  auto* model = binding.mutable_model()->mutable_model();
  model->set_id("catalog.llm");
  model->set_version("2026-09-12");
  model->set_signature_sha256(std::string(32, 'm'));
  auto* request_limits = binding.mutable_model()->mutable_request_limits();
  request_limits->set_max_input_tokens(8192);
  request_limits->set_max_output_tokens(2048);
  request_limits->set_max_request_bytes(8192);
  request_limits->set_max_response_bytes(2048);
  auto* model_output = binding.mutable_model()->mutable_output_type();
  model_output->set_canonical_sql("VARCHAR");
  *model_output->mutable_substrait_type() = outputType;
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(
      pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(
      pangrova::protocol::v2::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(
      pangrova::protocol::v2::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
  policy->set_idempotency_key("query:stage:partition:ai-invoke-1");
  auto* resources = binding.mutable_resources();
  resources->set_provider_concurrency(2);
  resources->set_provider_items(1024);
  resources->set_provider_attempts(1024);
  resources->set_provider_request_bytes(8192 * 1024);
  resources->set_provider_response_bytes(2048 * 1024);

  return binding;
}

inline pangrova::protocol::v2::OperatorBinding governedEmbeddingBinding(const substrait::Type& outputType) {
  pangrova::protocol::v2::OperatorBinding binding;
  binding.set_plan_node_id("ai-embed-1");
  *binding.mutable_capability() =
      pangrova::capability::aiEmbeddingDescriptor().ref();
  binding.mutable_capability()->clear_implementation_build_id();
  auto* model = binding.mutable_model()->mutable_model();
  model->set_id("catalog.embed");
  model->set_version("2026-09-12");
  model->set_signature_sha256(std::string(32, 'e'));
  auto* request_limits = binding.mutable_model()->mutable_request_limits();
  request_limits->set_max_input_tokens(8192);
  request_limits->set_max_request_bytes(8192);
  request_limits->set_max_response_bytes(8192);
  auto* model_output = binding.mutable_model()->mutable_output_type();
  model_output->set_canonical_sql("VECTOR(3)");
  *model_output->mutable_substrait_type() = outputType;
  auto* dimension = model_output->add_constraints();
  dimension->set_name("dimension");
  dimension->set_integer_value(3);
  auto* element = model_output->add_constraints();
  element->set_name("element_type");
  element->set_string_value("FLOAT32");
  auto* policy = binding.mutable_execution_policy();
  policy->set_coverage_policy(
      pangrova::protocol::v2::COVERAGE_POLICY_REQUIRE_COMPLETE);
  policy->set_fallback_policy(
      pangrova::protocol::v2::FALLBACK_POLICY_FORBID);
  policy->set_retry_class(
      pangrova::protocol::v2::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
  policy->set_idempotency_key("query:stage:partition:ai-embed-1");
  auto* resources = binding.mutable_resources();
  resources->set_provider_concurrency(2);
  resources->set_provider_items(1024);
  resources->set_provider_attempts(1024);
  resources->set_provider_request_bytes(8192 * 1024);
  resources->set_provider_response_bytes(8192 * 1024);

  return binding;
}
}  // namespace pangrova::velox::test
