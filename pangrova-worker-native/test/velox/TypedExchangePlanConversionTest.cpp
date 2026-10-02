#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/plan/VeloxPlanConverter.h"

namespace pangrova::velox::test {
namespace {

std::vector<uint8_t> serializePlan(const substrait::Rel& rel) {
  substrait::Plan plan;
  *plan.add_relations()->mutable_root()->mutable_input() = rel;
  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

substrait::Rel exchangeInputRelation() {
  substrait::Rel rel;
  auto* extension = rel.mutable_extension_leaf();
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_exchange_input();
  detail->set_plan_node_id("exchange-input-exchange-1-to-0");
  detail->set_exchange_id("exchange-1-to-0");
  detail->set_upstream_stage_id(1);
  detail->set_downstream_stage_id(0);
  detail->set_distribution(
      pangrova::protocol::v2::EXCHANGE_DISTRIBUTION_HASH);
  detail->set_partition_count(16);
  detail->set_batch_serde(
      pangrova::protocol::v2::BATCH_SERDE_NATIVE_ROW);
  detail->set_require_complete(true);
  detail->mutable_output_schema()->add_names("id");
  detail->mutable_output_schema()
      ->mutable_struct_()
      ->add_types()
      ->mutable_i64();
  extension->mutable_detail()->PackFrom(envelope);
  return rel;
}

pangrova::protocol::v2::RelationDetailEnvelope unpack(
    const substrait::Rel& rel) {
  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  EXPECT_TRUE(rel.extension_leaf().detail().UnpackTo(&envelope));
  return envelope;
}

void repack(
    substrait::Rel* rel,
    const pangrova::protocol::v2::RelationDetailEnvelope& envelope) {
  rel->mutable_extension_leaf()->mutable_detail()->PackFrom(envelope);
}

}  // namespace

TEST(TypedExchangePlanConversionTest, BuildsNativeExchangeNode) {
  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(exchangeInputRelation()), {});

  auto exchange = std::dynamic_pointer_cast<
      const facebook::velox::core::ExchangeNode>(result);
  ASSERT_NE(exchange, nullptr);
  EXPECT_EQ(exchange->id(), "exchange-input-exchange-1-to-0");
  EXPECT_EQ(exchange->serdeKind(), "Presto");
  ASSERT_EQ(exchange->outputType()->size(), 1);
  EXPECT_EQ(exchange->outputType()->nameOf(0), "id");
  EXPECT_EQ(exchange->outputType()->childAt(0)->kind(),
            facebook::velox::TypeKind::BIGINT);
}

TEST(TypedExchangePlanConversionTest, RejectsUnboundPartitionCount) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_partition_count(0);
  repack(&rel, envelope);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsImplicitPartialInput) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_require_complete(false);
  repack(&rel, envelope);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsUnsupportedBatchSerde) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_batch_serde(
      pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH);
  repack(&rel, envelope);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsInvalidStageIdentity) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_downstream_stage_id(1);
  repack(&rel, envelope);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsEmptyOutputSchema) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->clear_output_schema();
  repack(&rel, envelope);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

}  // namespace pangrova::velox::test
