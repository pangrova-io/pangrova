#include <gtest/gtest.h>

#include "capability/GovernedBindingArguments.h"
#include "StructuralContractIdentities.h"

namespace pangrova::capability {
namespace {

protocol::v2::OperatorBinding binding() {
  protocol::v2::OperatorBinding value;
  value.mutable_model()->mutable_model()->set_id("model");
  value.mutable_model()->mutable_model()->set_version("v1");
  value.mutable_model()->mutable_model()->set_signature_sha256(std::string(32, 'x'));
  value.mutable_capability()->set_capability_id("capability");
  value.mutable_capability()->set_semantic_version("1.0.0");
  value.mutable_capability()->set_signature_sha256(std::string(32, 'y'));
  value.mutable_execution_policy()->set_idempotency_key("operator-key");
  value.mutable_resources()->set_provider_concurrency(3);
  value.mutable_resources()->set_provider_items(8);
  value.mutable_resources()->set_provider_attempts(11);
  value.mutable_resources()->set_provider_request_bytes(1024);
  value.mutable_resources()->set_provider_response_bytes(2048);
  value.mutable_model()->mutable_request_limits()->set_max_input_tokens(900);
  value.mutable_model()->mutable_request_limits()->set_max_output_tokens(100);
  value.mutable_model()->mutable_request_limits()->set_max_request_bytes(128);
  value.mutable_model()->mutable_request_limits()->set_max_response_bytes(256);
  return value;
}

size_t ordinal(std::string_view name) {
  return generated::governedOrdinal(generated::AI_MODEL, name);
}

TEST(GovernedBindingArgumentsTest, OneDeclarationPreservesItemsAttemptsAndPerRequestLimits) {
  const auto values = governedBindingArguments("AI_MODEL", binding(), "invocation", 384);
  ASSERT_EQ(values.size(), generated::AI_MODEL.size());
  EXPECT_EQ(values[ordinal("invocation_id")], "invocation");
  EXPECT_EQ(values[ordinal("provider_items")], "8");
  EXPECT_EQ(values[ordinal("provider_attempts")], "11");
  EXPECT_EQ(values[ordinal("provider_request_bytes")], "1024");
  EXPECT_EQ(values[ordinal("provider_response_bytes")], "2048");
  EXPECT_EQ(values[ordinal("max_input_tokens")], "900");
  EXPECT_EQ(values[ordinal("max_request_bytes")], "128");
  EXPECT_EQ(values[ordinal("output_vector_dimension")], "384");
  EXPECT_EQ(values[ordinal("reuse_enabled")], "false");
}

TEST(GovernedBindingArgumentsTest, ReuseThresholdRoundTripsWithoutSixDecimalTruncation) {
  auto value = binding();
  const double threshold = 0.9876543210123456;
  value.mutable_reuse_policy()->set_similarity_threshold(threshold);
  const auto encoded = governedBindingArguments("AI_MODEL", value, "invocation", 0);
  EXPECT_DOUBLE_EQ(std::stod(encoded[ordinal("reuse_similarity_threshold")]), threshold);
}

TEST(GovernedBindingArgumentsTest, MissingAuthorityAndUnknownLayoutsFailClosed) {
  auto value = binding();
  value.mutable_model()->clear_request_limits();
  EXPECT_THROW(governedBindingArguments("AI_MODEL", value, "invocation", 0), std::invalid_argument);
  EXPECT_THROW(governedBindingArguments("unknown", binding(), "invocation", 0), std::invalid_argument);
  EXPECT_THROW(governedBindingArguments("AI_MODEL", binding(), "", 0), std::invalid_argument);
  value = binding();
  value.mutable_model()->mutable_model()->set_signature_sha256("short");
  EXPECT_THROW(governedBindingArguments("AI_MODEL", value, "invocation", 0), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::capability
