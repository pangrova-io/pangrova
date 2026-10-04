#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "CanonicalOutputAssertions.h"

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
  auto* root = plan.add_relations()->mutable_root();
  *root->mutable_input() = rel;
  root->add_names("id");
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
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
      pangrova::protocol::v2::BATCH_SERDE_ARROW_BATCH);
  detail->set_require_complete(true);
  detail->mutable_output_schema()->add_names("id");
  detail->mutable_output_schema()
      ->mutable_struct_()
      ->add_types()
      ->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(serializePlan(exchangeInputRelation()), {});

  const auto exchange = std::dynamic_pointer_cast<
      const facebook::velox::core::ExchangeNode>(result);
  ASSERT_NE(exchange, nullptr);
  EXPECT_EQ(exchange->id(), "exchange-input-exchange-1-to-0");
  EXPECT_EQ(exchange->serdeKind(), "ArrowBatch");
  ASSERT_EQ(exchange->outputType()->size(), 1);
  EXPECT_EQ(exchange->outputType()->nameOf(0), physicalFieldAlias(0));
  expectCanonicalOutputField(converter, exchange->outputType(), 0, "id");
  EXPECT_EQ(exchange->outputType()->childAt(0)->kind(),
            facebook::velox::TypeKind::BIGINT);
  const auto binding = converter.serdeSchemaBindings().find(exchange->id());
  ASSERT_NE(binding, converter.serdeSchemaBindings().end());
  EXPECT_EQ(binding->second.SerializeAsString(),
      unpack(exchangeInputRelation()).exchange_input().output_schema().SerializeAsString());
}

TEST(TypedExchangePlanConversionTest, WireBindingPrecedesEmitAndResultWriterUsesCanonicalNotPresentationNames) {
  auto relation = exchangeInputRelation();
  auto envelope = unpack(relation);
  auto* declared = envelope.mutable_exchange_input()->mutable_output_schema();
  declared->add_names("text");
  declared->mutable_struct_()->add_types()->mutable_string()->set_nullability(
      substrait::Type::NULLABILITY_NULLABLE);
  const auto sourceContract = declared->SerializeAsString();
  repack(&relation, envelope);
  relation.mutable_extension_leaf()->mutable_common()->mutable_emit()->add_output_mapping(1);
  const auto registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(registry);
  const auto result = converter.convert(serializePlan(relation), {}, true);
  const auto output = std::dynamic_pointer_cast<const facebook::velox::core::PartitionedOutputNode>(result);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->sources().size(), 1);
  const auto project = std::dynamic_pointer_cast<const facebook::velox::core::ProjectNode>(output->sources()[0]);
  ASSERT_NE(project, nullptr);
  ASSERT_EQ(project->sources().size(), 1);
  const auto exchange = std::dynamic_pointer_cast<const facebook::velox::core::ExchangeNode>(project->sources()[0]);
  ASSERT_NE(exchange, nullptr);
  ASSERT_EQ(converter.serdeSchemaBindings().size(), 2);
  EXPECT_EQ(converter.serdeSchemaBindings().at(exchange->id()).SerializeAsString(), sourceContract);
  const auto& resultContract = converter.serdeSchemaBindings().at(output->id());
  EXPECT_EQ(resultContract.SerializeAsString(), converter.canonicalOutputSchema().SerializeAsString());
  ASSERT_EQ(resultContract.names_size(), 1);
  EXPECT_EQ(resultContract.names(0), "text");
  EXPECT_EQ(resultContract.struct_().types(0).string().nullability(), substrait::Type::NULLABILITY_NULLABLE);
  EXPECT_EQ(converter.resultColumnLabels(), (std::vector<std::string>{"id"}));
  EXPECT_EQ(output->outputType()->nameOf(0), physicalFieldAlias(0));
}

TEST(TypedExchangePlanConversionTest, RejectsUnboundPartitionCount) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_partition_count(0);
  repack(&rel, envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsImplicitPartialInput) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_require_complete(false);
  repack(&rel, envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsUnsupportedBatchSerde) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_batch_serde(
      static_cast<pangrova::protocol::v2::BatchSerde>(1));
  repack(&rel, envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsInvalidStageIdentity) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->set_downstream_stage_id(1);
  repack(&rel, envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, RejectsEmptyOutputSchema) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  envelope.mutable_exchange_input()->clear_output_schema();
  repack(&rel, envelope);

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST(TypedExchangePlanConversionTest, ExplicitZeroColumnSchemaIsAValidExchange) {
  auto rel = exchangeInputRelation();
  auto envelope = unpack(rel);
  auto* schema = envelope.mutable_exchange_input()->mutable_output_schema();
  schema->clear_names();
  schema->mutable_struct_()->clear_types();
  repack(&rel, envelope);
  substrait::Plan plan;
  *plan.add_relations()->mutable_root()->mutable_input() = rel;
  std::string wire;
  ASSERT_TRUE(pangrova::test::canonicalTestPlan(plan).SerializeToString(&wire));
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert({wire.begin(), wire.end()}, {});
  auto exchange = std::dynamic_pointer_cast<const facebook::velox::core::ExchangeNode>(result);
  ASSERT_NE(exchange, nullptr);
  EXPECT_EQ(exchange->outputType()->size(), 0);
  EXPECT_EQ(exchange->serdeKind(), "ArrowBatch");
}

}  // namespace pangrova::velox::test
