// VeloxPlanConverterTest: Verifies Substrait → Velox PlanNode conversion
// for each core operator type.
//
// Tests build Substrait Plan protobuf programmatically, call convert(),
// and verify the returned PlanNode tree has the correct type and structure.
//
// **Validates: Requirements 1.61, 2.61**
#include <gtest/gtest.h>
#include "CanonicalOutputAssertions.h"

#include "velox/plan/VeloxPlanConverter.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"

#include "capability/NativeCapabilityCatalog.h"
#include "../TestMemoryHelper.h"
#include "udf/JvmScalarFunction.h"
#include "StructuralContractIdentities.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"

#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>

namespace pangrova::velox::test {
namespace {

class VeloxPlanConverterTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::detail::ensureRuntimeInitialized();
  }
};

// ---------------------------------------------------------------------------
// Helper: build a minimal NamedStruct with given column names and types.
// ---------------------------------------------------------------------------
void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_i64();
}

void addStringColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_string();
}

void expectLogicalOutputField(const VeloxPlanConverter& converter,
    const facebook::velox::RowTypePtr& physicalType, size_t ordinal, const std::string& label) {
  expectCanonicalOutputField(converter, physicalType, ordinal, label);
}

// Use the production JVM UDF implementation and registry, not a test function.
// Conversion validates signatures; these component tests do not invoke Java.
void registerGovernedAiSignature(
    const std::string& name, const std::string& output_type) {
  auto pool = pangrova::test::testLeafPool("governed-ai-signature");
  pangrova::udf::NativeUdfRegistry registry;
  for (const auto& signature : pangrova::capability::generated::runtimeCallbackSignatures(name)) {
    EXPECT_EQ(std::string(signature.returnType), output_type);
    registry.registerScalar(pangrova::udf::makeJvmScalarFunctionSpec(name,
        std::vector<std::string>(signature.arguments.begin(), signature.arguments.end()), signature.returnType));
  }
  registry.registerAllWithVelox();
}

// Build a simple ReadRel (TableScan) with a named table and schema.
substrait::ReadRel buildReadRel(
    const std::string& table_name,
    const std::vector<std::string>& col_names) {
  substrait::ReadRel read;
  auto* named = read.mutable_named_table();
  named->add_names(table_name);
  auto* schema = read.mutable_base_schema();
  for (const auto& col : col_names) {
    addI64Column(schema, col);
  }
  return read;
}

// Wrap a Rel into a Plan and serialize to bytes.
std::vector<uint8_t> serializePlan(const substrait::Rel& rel,
    const std::vector<std::string>& names = {"output"}) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  for (const auto& name : names) plan_rel->mutable_root()->add_names(name);

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Helper: set a field reference expression pointing to field index.
void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Helper: set a field reference on a FieldReference message (for join keys).
void setJoinKeyRef(
    substrait::Expression::FieldReference* ref, int field_index) {
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

template <typename JoinRel>
void addStandardJoinKey(JoinRel* join, int left_index, int right_index) {
  auto* key = join->add_keys();
  setJoinKeyRef(key->mutable_left(), left_index);
  setJoinKeyRef(key->mutable_right(), right_index);
  key->mutable_comparison()->set_simple(
      substrait::ComparisonJoinKey::SIMPLE_COMPARISON_TYPE_EQ);
}

// ---------------------------------------------------------------------------
// Test: Empty / invalid input
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, EmptyBytesReturnsNull) {
  VeloxPlanConverter converter;
  auto result = converter.convert({}, {});
  EXPECT_EQ(result, nullptr);
}

TEST_F(VeloxPlanConverterTest, InvalidProtobufReturnsNull) {
  VeloxPlanConverter converter;
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(converter.convert(garbage, {}), std::runtime_error);
}

TEST_F(VeloxPlanConverterTest, EmptyPlanReturnsNull) {
  substrait::Plan plan;
  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  EXPECT_EQ(result, nullptr);
}

TEST_F(VeloxPlanConverterTest, LiveValuesPlansHaveDistinctMemoryPools) {
  substrait::Rel rel;
  addI64Column(rel.mutable_read()->mutable_base_schema(), "value");
  auto* row = rel.mutable_read()->mutable_virtual_table()->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(42);
  auto plan_bytes = serializePlan(rel);

  VeloxPlanConverter first;
  VeloxPlanConverter second;
  auto first_plan = first.convert(plan_bytes, {});
  auto second_plan = second.convert(plan_bytes, {});

  ASSERT_NE(first_plan, nullptr);
  ASSERT_NE(second_plan, nullptr);
  auto first_values = std::dynamic_pointer_cast<const facebook::velox::core::ValuesNode>(first_plan);
  auto second_values = std::dynamic_pointer_cast<const facebook::velox::core::ValuesNode>(second_plan);
  ASSERT_NE(first_values, nullptr);
  ASSERT_NE(second_values, nullptr);
  ASSERT_EQ(first_values->values().size(), 1);
  ASSERT_EQ(second_values->values().size(), 1);
  EXPECT_NE(first_values->values().front()->pool()->name(),
            second_values->values().front()->pool()->name());
  EXPECT_TRUE(first_values->values().front()->ownsMemoryPool());
  EXPECT_TRUE(second_values->values().front()->ownsMemoryPool());
}

// ---------------------------------------------------------------------------
// Test: TableScan (ReadRel)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, TableScanProducesTableScanNode) {
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("orders");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "order_id");
  addI64Column(schema, "customer_id");
  addStringColumn(schema, "status");

  auto plan_bytes = serializePlan(rel, {"order_id", "customer_id", "status"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "TableScan");

  // Verify output schema has 3 columns.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 3);
  expectLogicalOutputField(converter, output_type, 0, "order_id");
  expectLogicalOutputField(converter, output_type, 1, "customer_id");
  expectLogicalOutputField(converter, output_type, 2, "status");
}

// ---------------------------------------------------------------------------
// Test: Filter (FilterRel → TableScan)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, FilterProducesFilterNode) {
  substrait::Rel rel;
  auto* filter = rel.mutable_filter();

  // Input: TableScan on "orders" with 2 columns.
  auto* input_read = filter->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("orders");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "order_id");
  addI64Column(schema, "amount");

  // A canonical predicate is BOOLEAN, not an implicitly truthy BIGINT.
  auto* predicate = filter->mutable_condition()->mutable_cast();
  setFieldRef(predicate->mutable_input(), 0);
  predicate->mutable_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  predicate->set_failure_behavior(substrait::Expression_Cast::FAILURE_BEHAVIOR_THROW_EXCEPTION);

  auto plan_bytes = serializePlan(rel, {"order_id", "amount"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Filter");

  // Child should be TableScan.
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: Project (ProjectRel → TableScan)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, ProjectProducesProjectNode) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();

  // Input: TableScan.
  auto* input_read = project->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("customers");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "id");
  addStringColumn(schema, "name");
  addI64Column(schema, "age");

  // Project expressions: field 0 and field 2.
  setFieldRef(project->add_expressions(), 0);
  setFieldRef(project->add_expressions(), 2);
  project->mutable_common()->mutable_emit()->add_output_mapping(3);
  project->mutable_common()->mutable_emit()->add_output_mapping(4);

  auto plan_bytes = serializePlan(rel, {"id", "age"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");

  // Child should be TableScan.
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
}

TEST_F(VeloxPlanConverterTest, TypedEmptyArrayIsAnArrayConstant) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  addI64Column(read->mutable_base_schema(), "id");
  auto* row = read->mutable_virtual_table()->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(1);
  auto* empty = project->add_expressions()->mutable_literal()->mutable_empty_list();
  empty->mutable_type()->mutable_string()->set_nullability(
      substrait::Type::NULLABILITY_NULLABLE);
  empty->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  project->mutable_common()->mutable_emit()->add_output_mapping(1);

  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(rel), {});
  std::shared_ptr<const facebook::velox::core::ConstantTypedExpr> constant;
  for (auto node = result; node && !constant;) {
    if (auto project_node = std::dynamic_pointer_cast<
            const facebook::velox::core::ProjectNode>(node)) {
      for (const auto& expression : project_node->projections()) {
        constant = std::dynamic_pointer_cast<
            const facebook::velox::core::ConstantTypedExpr>(expression);
        if (constant) break;
      }
    }
    node = node->sources().size() == 1 ? node->sources().front() : nullptr;
  }
  ASSERT_NE(constant, nullptr);
  EXPECT_TRUE(constant->type()->equivalent(
      *facebook::velox::ARRAY(facebook::velox::VARCHAR())));
}

TEST_F(VeloxPlanConverterTest, GovernedAiExpressionConsumesExactBinding) {
  registerGovernedAiSignature("ai_complete", "VARCHAR");
  substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(0);
  extension->set_name("ai_complete:string_string");
  auto* project = plan.add_relations()
                      ->mutable_root()
                      ->mutable_input()
                      ->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("prompts");
  addStringColumn(read->mutable_base_schema(), "prompt");
  auto* function = project->add_expressions()->mutable_scalar_function();
  function->set_function_reference(0);
  function->mutable_output_type()->mutable_string();
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("catalog.llm");
  setFieldRef(function->add_arguments()->mutable_value(), 0);
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("ai-invoke-1");

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
  *model_output->mutable_substrait_type() = function->output_type();
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

  std::string bytes;
  ASSERT_TRUE(plan.SerializeToString(&bytes));
  VeloxPlanConverter converter;
  auto result = converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "Presto",
      {{binding.plan_node_id(), binding}});

  auto project_node = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(result);
  ASSERT_NE(project_node, nullptr);
  ASSERT_EQ(project_node->projections().size(), 1);
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(
      project_node->projections().front());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "ai_complete");
  EXPECT_EQ(call->inputs().size(),
      pangrova::capability::generated::runtimeCallbackSignatures("ai_complete").front().arguments.size());
  auto invocation = std::dynamic_pointer_cast<
      const facebook::velox::core::ConstantTypedExpr>(call->inputs()[2]);
  ASSERT_NE(invocation, nullptr);
  auto invocation_value = invocation->value().value<
      facebook::velox::StringView>();
  EXPECT_EQ(std::string(invocation_value.data(), invocation_value.size()),
            "ai-invoke-1");

  VeloxPlanConverter missing_binding_converter;
  EXPECT_THROW(
      missing_binding_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "Presto", {}),
      std::runtime_error);

  auto drifted = binding;
  drifted.mutable_model()->mutable_model()->set_id("catalog.other-model");
  VeloxPlanConverter drifted_binding_converter;
  EXPECT_THROW(
      drifted_binding_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "Presto",
          {{drifted.plan_node_id(), drifted}}),
      std::runtime_error);

  auto output_drifted = binding;
  output_drifted.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->clear_string();
  output_drifted.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->mutable_fp64();
  VeloxPlanConverter output_drifted_converter;
  EXPECT_THROW(
      output_drifted_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "Presto",
          {{output_drifted.plan_node_id(), output_drifted}}),
      std::runtime_error);

  // Calcite can infer a required expression for non-null literals while the
  // model advertises a nullable SQL result. This is not a physical type drift.
  auto nullable_model_output = binding;
  nullable_model_output.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->mutable_string()->set_nullability(
          substrait::Type_Nullability_NULLABILITY_NULLABLE);
  VeloxPlanConverter nullable_output_converter;
  EXPECT_NO_THROW(nullable_output_converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "Presto",
      {{nullable_model_output.plan_node_id(), nullable_model_output}}));
}

TEST_F(VeloxPlanConverterTest, GovernedAiEmbedCarriesExactVectorDimension) {
  registerGovernedAiSignature("ai_embed", "VECTOR");
  substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(0);
  extension->set_name("ai_embed:string_string_string");
  auto* project = plan.add_relations()
                      ->mutable_root()
                      ->mutable_input()
                      ->mutable_project();
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("prompts");
  addStringColumn(read->mutable_base_schema(), "prompt");
  auto* function = project->add_expressions()->mutable_scalar_function();
  function->set_function_reference(0);
  auto* output = function->mutable_output_type()->mutable_list();
  output->set_nullability(
      substrait::Type_Nullability_NULLABILITY_NULLABLE);
  output->mutable_type()->mutable_fp32()->set_nullability(
      substrait::Type_Nullability_NULLABILITY_REQUIRED);
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("catalog.embed");
  setFieldRef(function->add_arguments()->mutable_value(), 0);
  // RAW is the sole default normalized public purpose, never inferred from batch size.
  function->add_arguments()->mutable_value()->mutable_literal()->set_string("RAW");
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("ai-embed-1");

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
  *model_output->mutable_substrait_type() = function->output_type();
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

  std::string bytes;
  ASSERT_TRUE(plan.SerializeToString(&bytes));
  VeloxPlanConverter converter;
  auto result = converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "Presto",
      {{binding.plan_node_id(), binding}});

  auto project_node = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(result);
  ASSERT_NE(project_node, nullptr);
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(
      project_node->projections().front());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "ai_embed");
  ASSERT_EQ(call->inputs().size(),
      pangrova::capability::generated::runtimeCallbackSignatures("ai_embed").front().arguments.size());
  EXPECT_TRUE(call->type()->isArray());
  EXPECT_TRUE(call->type()->childAt(0)->isReal());
  auto exact_dimension = std::dynamic_pointer_cast<
      const facebook::velox::core::ConstantTypedExpr>(call->inputs().back());
  ASSERT_NE(exact_dimension, nullptr);
  auto value = exact_dimension->value().value<facebook::velox::StringView>();
  EXPECT_EQ(std::string(value.data(), value.size()), "3");

  auto drifted = binding;
  drifted.mutable_model()->mutable_output_type()->set_canonical_sql(
      "VECTOR(4)");
  VeloxPlanConverter drifted_converter;
  EXPECT_ANY_THROW(
      drifted_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "Presto",
          {{drifted.plan_node_id(), drifted}}));
}

// ---------------------------------------------------------------------------
// Test: Aggregate (AggregateRel → TableScan)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, AggregateProducesAggregationNode) {
  substrait::Rel rel;
  auto* agg = rel.mutable_aggregate();

  // Input: TableScan.
  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("sales");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "region_id");
  addI64Column(schema, "amount");

  // Grouping: field 0 (region_id).
  auto* grouping = agg->add_groupings();
  setFieldRef(agg->add_grouping_expressions(), 0);
  grouping->add_expression_references(0);

  // Measure: aggregate on field 1 (amount).
  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  agg_func->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  auto* arg = agg_func->add_arguments();
  setFieldRef(arg->mutable_value(), 1);
  agg_func->mutable_output_type()->mutable_i64();

  // Register the function in the plan extensions.
  substrait::Plan plan;
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  plan_rel->mutable_root()->add_names("region_id");
  plan_rel->mutable_root()->add_names("total_amount");

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // The converter may wrap with a Project rename node at the root.
  // Verify an Aggregation node exists in the plan tree.
  auto node = result;
  while (node && node->name() != "Aggregation") {
    ASSERT_GE(node->sources().size(), 1u);
    node = node->sources()[0];
  }
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->name(), "Aggregation");
}

// ---------------------------------------------------------------------------
// Test: Aggregate measure FILTER (SQL:2003) → Velox per-aggregate mask
// ---------------------------------------------------------------------------

namespace {
void addBoolColumn(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  auto* field = schema->mutable_struct_()->add_types();
  field->mutable_bool_();
}

// Builds an AggregateRel plan: group by field 0, sum(field 1) with an
// optional measure filter referencing filter_field (or -1 for none).
std::vector<uint8_t> buildAggregateWithFilterPlan(int filter_field) {
  substrait::Rel rel;
  auto* agg = rel.mutable_aggregate();

  auto* input_read = agg->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("sales");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "region_id");
  addI64Column(schema, "amount");
  addBoolColumn(schema, "is_valid");

  auto* grouping = agg->add_groupings();
  setFieldRef(agg->add_grouping_expressions(), 0);
  grouping->add_expression_references(0);

  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
  agg_func->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  auto* arg = agg_func->add_arguments();
  setFieldRef(arg->mutable_value(), 1);
  agg_func->mutable_output_type()->mutable_i64();
  if (filter_field >= 0) {
    setFieldRef(measure->mutable_filter(), filter_field);
  }

  substrait::Plan plan;
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(1);
  ext_func->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  plan_rel->mutable_root()->add_names("region_id");
  plan_rel->mutable_root()->add_names("total_amount");

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

const facebook::velox::core::AggregationNode* findAggregationNode(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& root) {
  auto node = root;
  while (node && node->name() != "Aggregation") {
    if (node->sources().empty()) {
      return nullptr;
    }
    node = node->sources()[0];
  }
  return dynamic_cast<const facebook::velox::core::AggregationNode*>(
      node.get());
}

// Collects every AggregationNode along the first-source spine (the converter
// may split kSingle into kPartial → kFinal for parallel aggregation).
std::vector<const facebook::velox::core::AggregationNode*>
collectAggregationNodes(
    const std::shared_ptr<const facebook::velox::core::PlanNode>& root) {
  std::vector<const facebook::velox::core::AggregationNode*> found;
  auto node = root;
  while (node) {
    if (const auto* agg =
            dynamic_cast<const facebook::velox::core::AggregationNode*>(
                node.get())) {
      found.push_back(agg);
    }
    if (node->sources().empty()) {
      break;
    }
    node = node->sources()[0];
  }
  return found;
}
}  // namespace

TEST_F(VeloxPlanConverterTest, AggregateMeasureFilterSetsMask) {
  auto plan_bytes = buildAggregateWithFilterPlan(/*filter_field=*/2);

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // The converter splits kSingle into kPartial → kFinal. The mask must be
  // applied exactly once, on the step that sees raw input rows (partial or
  // single); the final step aggregates partial accumulators and must NOT
  // re-apply the mask.
  auto agg_nodes = collectAggregationNodes(result);
  ASSERT_FALSE(agg_nodes.empty());

  // Deepest aggregation node sees the raw input — mask required there.
  const auto* raw_input_agg = agg_nodes.back();
  ASSERT_EQ(raw_input_agg->aggregates().size(), 1u);
  const auto& aggregate = raw_input_agg->aggregates()[0];
  ASSERT_NE(aggregate.mask, nullptr);
  EXPECT_EQ(aggregate.mask->name(), physicalFieldAlias(2));
  EXPECT_EQ(
      aggregate.mask->type()->kind(), facebook::velox::TypeKind::BOOLEAN);

  // Any upstream (final) aggregation steps must not carry the mask.
  for (size_t i = 0; i + 1 < agg_nodes.size(); ++i) {
    ASSERT_EQ(agg_nodes[i]->aggregates().size(), 1u);
    EXPECT_EQ(agg_nodes[i]->aggregates()[0].mask, nullptr);
  }
}

TEST_F(VeloxPlanConverterTest, AggregateWithoutMeasureFilterHasNoMask) {
  auto plan_bytes = buildAggregateWithFilterPlan(/*filter_field=*/-1);

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  const auto* agg_node = findAggregationNode(result);
  ASSERT_NE(agg_node, nullptr);
  ASSERT_EQ(agg_node->aggregates().size(), 1u);
  EXPECT_EQ(agg_node->aggregates()[0].mask, nullptr);
}

TEST_F(VeloxPlanConverterTest, AggregateMeasureFilterOnNonBooleanColumnThrows) {
  // Filter references field 1 (amount, i64) — planner contract violation,
  // conversion must fail loud instead of silently dropping the filter.
  auto plan_bytes = buildAggregateWithFilterPlan(/*filter_field=*/1);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: HashJoin (HashJoinRel with two TableScan inputs)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, HashJoinProducesHashJoinNode) {
  substrait::Rel rel;
  auto* join = rel.mutable_hash_join();

  // Left input: orders table.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("orders");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "order_id");
  addI64Column(left_schema, "customer_id");

  // Right input: customers table.
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("customers");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "id");
  addStringColumn(right_schema, "name");

  // Join keys: orders.customer_id (field 1) = customers.id (field 0).
  addStandardJoinKey(join, 1, 0);

  join->set_type(substrait::HashJoinRel::JOIN_TYPE_INNER);

  auto plan_bytes = serializePlan(rel, {"order_id", "customer_id", "id", "name"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");

  // Should have two children (left and right).
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
  EXPECT_EQ(result->sources()[1]->name(), "Project");
  ASSERT_EQ(result->sources()[1]->sources().size(), 1);
  EXPECT_EQ(result->sources()[1]->sources()[0]->name(), "TableScan");

  // Output should have 4 columns (2 from left + 2 from right).
  EXPECT_EQ(result->outputType()->size(), 4);
}

// ---------------------------------------------------------------------------
// Test: producer ExchangeRel binds the one callback-backed shuffle sink
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, ProducerExchangeUsesCallbackSink) {
  substrait::Rel rel;
  auto* exchange = rel.mutable_exchange();

  // Input: TableScan.
  auto* input_read = exchange->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("lineitem");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "item_id");
  addI64Column(schema, "quantity");

  exchange->set_partition_count(4);
  exchange->mutable_round_robin();

  auto plan_bytes = serializePlan(rel, {"item_id", "quantity"});
  VeloxPlanConverter converter;
  auto result = converter.convert(
      plan_bytes, {}, false, "Presto", {}, nullptr, {}, 4);

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "TableScan");
  ASSERT_TRUE(converter.shuffleOutputSpec().has_value());
  EXPECT_EQ(converter.shuffleOutputSpec()->partition_count, 4);
  EXPECT_EQ(converter.shuffleOutputSpec()->distribution,
            ShuffleOutputSpec::Distribution::ROUND_ROBIN);

  // Exchange output type should match input.
  EXPECT_EQ(result->outputType()->size(), 2);
}

TEST_F(VeloxPlanConverterTest, ProducerExchangeRejectsUnboundPartitionCount) {
  substrait::Rel rel;
  auto* exchange = rel.mutable_exchange();
  auto* input = exchange->mutable_input()->mutable_read();
  input->mutable_named_table()->add_names("lineitem");
  addI64Column(input->mutable_base_schema(), "item_id");
  exchange->set_partition_count(4);
  exchange->mutable_round_robin();
  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
  EXPECT_THROW(converter.convert(
      plan_bytes, {}, false, "Presto", {}, nullptr, {}, 2),
      std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: Nested plan (Project → Filter → TableScan)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, NestedPlanProjectFilterTableScan) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();

  // Filter → TableScan as input to Project.
  auto* filter = project->mutable_input()->mutable_filter();
  auto* input_read = filter->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("events");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "event_id");
  addI64Column(schema, "user_id");
  addI64Column(schema, "timestamp");

  auto* predicate = filter->mutable_condition()->mutable_cast();
  setFieldRef(predicate->mutable_input(), 1);
  predicate->mutable_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  predicate->set_failure_behavior(substrait::Expression_Cast::FAILURE_BEHAVIOR_THROW_EXCEPTION);

  // Project: select field 0 and field 2.
  setFieldRef(project->add_expressions(), 0);
  setFieldRef(project->add_expressions(), 2);
  project->mutable_common()->mutable_emit()->add_output_mapping(3);
  project->mutable_common()->mutable_emit()->add_output_mapping(4);

  auto plan_bytes = serializePlan(rel, {"event_id", "timestamp"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");

  // Project → Filter.
  ASSERT_EQ(result->sources().size(), 1);
  auto filter_node = result->sources()[0];
  EXPECT_EQ(filter_node->name(), "Filter");

  // Filter → TableScan.
  ASSERT_EQ(filter_node->sources().size(), 1);
  EXPECT_EQ(filter_node->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: JoinRel (logical join) also produces HashJoinNode
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, LogicalJoinProducesHashJoinNode) {
  // Build a Plan directly (not via serializePlan) so we can register
  // the "equal" function extension needed by extractEquiKeys.
  substrait::Plan plan;

  // Register the equality function used to extract hash join keys.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("equal:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("order_id");
  root->add_names("customer_id");
  root->add_names("id");
  auto* join = root->mutable_input()->mutable_join();

  // Left: orders (field 0: order_id, field 1: customer_id).
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("orders");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "order_id");
  addI64Column(left_schema, "customer_id");

  // Right: customers (field 0: id → combined field 2).
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("customers");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "id");

  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);

  // Join expression: equal(orders.customer_id[1], customers.id[2]).
  // In combined schema, left fields are [0,1], right fields start at 2.
  auto* expr = join->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);  // anchor for "equal"
  func->mutable_output_type()->mutable_bool_();
  // Arg 0: left field 1 (customer_id).
  setFieldRef(func->add_arguments()->mutable_value(), 1);
  // Arg 1: right field 0 → combined index 2 (id).
  setFieldRef(func->add_arguments()->mutable_value(), 2);

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // Logical JoinRel with equi-join expression → HashJoinNode.
  EXPECT_EQ(result->name(), "HashJoin");
  ASSERT_EQ(result->sources().size(), 2);
}

TEST_F(VeloxPlanConverterTest, NullSafeEqualityRemainsAJoinFilter) {
  substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(0);
  extension->set_name("is_not_distinct_from:i64_i64");

  auto* join = plan.add_relations()->mutable_root()->mutable_input()
                   ->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
  auto* left = join->mutable_left()->mutable_read();
  left->mutable_named_table()->add_names("left_table");
  addI64Column(left->mutable_base_schema(), "left_key");
  auto* right = join->mutable_right()->mutable_read();
  right->mutable_named_table()->add_names("right_table");
  addI64Column(right->mutable_base_schema(), "right_key");
  auto* equality = join->mutable_expression()->mutable_scalar_function();
  equality->set_function_reference(0);
  equality->mutable_output_type()->mutable_bool_();
  setFieldRef(equality->add_arguments()->mutable_value(), 0);
  setFieldRef(equality->add_arguments()->mutable_value(), 1);

  std::string bytes;
  ASSERT_TRUE(plan.SerializeToString(&bytes));
  VeloxPlanConverter converter;
  auto result = converter.convert({bytes.begin(), bytes.end()}, {});

  auto nested = std::dynamic_pointer_cast<
      const facebook::velox::core::NestedLoopJoinNode>(result);
  ASSERT_NE(nested, nullptr);
  auto negation = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(nested->joinCondition());
  ASSERT_NE(negation, nullptr);
  EXPECT_EQ(negation->name(), "not");
  ASSERT_EQ(negation->inputs().size(), 1);
  auto distinct = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(negation->inputs()[0]);
  ASSERT_NE(distinct, nullptr);
  EXPECT_EQ(distinct->name(), "distinct_from");
}

TEST_F(VeloxPlanConverterTest, AntiJoinRenamesOnlyOverlappingRightColumns) {
  substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(0);
  extension->set_name("equal:i64_i64");
  auto* join = plan.add_relations()->mutable_root()->mutable_input()
                   ->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT_ANTI);
  auto* left = join->mutable_left()->mutable_read();
  left->mutable_named_table()->add_names("source");
  addI64Column(left->mutable_base_schema(), "id");
  auto* right = join->mutable_right()->mutable_read();
  right->mutable_named_table()->add_names("target");
  addI64Column(right->mutable_base_schema(), "id");
  addStringColumn(right->mutable_base_schema(), "target_only");
  auto* equality = join->mutable_expression()->mutable_scalar_function();
  equality->set_function_reference(0);
  equality->mutable_output_type()->mutable_bool_();
  setFieldRef(equality->add_arguments()->mutable_value(), 0);
  setFieldRef(equality->add_arguments()->mutable_value(), 1);

  std::string bytes;
  ASSERT_TRUE(plan.SerializeToString(&bytes));
  VeloxPlanConverter converter;
  auto result = converter.convert({bytes.begin(), bytes.end()}, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[1]->name(), "Project");
  EXPECT_EQ(result->outputType()->size(), 1);
}

// ---------------------------------------------------------------------------
// Test: Unsupported rel type throws with type name
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, UnsupportedRelTypeThrowsWithTypeName) {
  // Build a plan with a Rel that has no recognized rel_type set.
  // We use a raw Rel with only the default (unset) case.
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  // Create a Rel with no rel_type set — this triggers the default case.
  plan_rel->mutable_root()->mutable_input();
  plan_rel->mutable_root()->add_names("output");

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  try {
    converter.convert(plan_bytes, {});
    FAIL() << "Expected std::runtime_error for unsupported rel type";
  } catch (const std::runtime_error& e) {
    std::string msg = e.what();
    EXPECT_TRUE(msg.find("unsupported") != std::string::npos)
        << "Error message should contain 'unsupported': " << msg;
    EXPECT_TRUE(msg.find("rel type") != std::string::npos ||
                msg.find("Rel") != std::string::npos)
        << "Error message should reference rel type: " << msg;
  }
}

// ---------------------------------------------------------------------------
// Test: Unsupported expression type throws with type name
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, UnsupportedExpressionTypeThrows) {
  substrait::Rel rel;
  auto* filter = rel.mutable_filter();

  auto* input_read = filter->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("test_table");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "col1");

  // Set a condition with an expression type that is not handled.
  // Use subquery (kSubquery) which is not supported.
  auto* condition = filter->mutable_condition();
  condition->mutable_subquery();

  auto plan_bytes = serializePlan(rel, {"event_id", "timestamp"});
  VeloxPlanConverter converter;
  try {
    converter.convert(plan_bytes, {});
    FAIL() << "Expected std::runtime_error for unsupported expression type";
  } catch (const std::runtime_error& e) {
    std::string msg = e.what();
    EXPECT_TRUE(msg.find("unsupported") != std::string::npos)
        << "Error message should contain 'unsupported': " << msg;
    EXPECT_TRUE(msg.find("expression") != std::string::npos)
        << "Error message should reference expression type: " << msg;
  }
}

// ---------------------------------------------------------------------------
// Test: Sort (SortRel → OrderByNode)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, SortProducesOrderByNode) {
  substrait::Rel rel;
  auto* sort = rel.mutable_sort();

  auto* input_read = sort->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("events");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "event_id");
  addI64Column(schema, "timestamp");

  // Sort by field 1 (timestamp) ascending nulls first.
  auto* sort_field = sort->add_sorts();
  setFieldRef(sort_field->mutable_expr(), 1);
  sort_field->set_direction(
      substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);

  auto plan_bytes = serializePlan(rel, {"item_id", "price"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "OrderBy");
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: Limit (FetchRel → LimitNode)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, FetchProducesLimitNode) {
  substrait::Rel rel;
  auto* fetch = rel.mutable_fetch();

  auto* input_read = fetch->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("items");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "item_id");
  addI64Column(schema, "price");

  fetch->mutable_offset_expr()->mutable_literal()->set_i64(5);
  fetch->mutable_count_expr()->mutable_literal()->set_i64(10);

  auto plan_bytes = serializePlan(rel, {"student_id", "score"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // The converter may wrap with a Project rename node at the root.
  // Verify a Limit node exists in the plan tree.
  auto node = result;
  while (node && node->name() != "Limit") {
    ASSERT_GE(node->sources().size(), 1u);
    node = node->sources()[0];
  }
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->name(), "Limit");
}

// ---------------------------------------------------------------------------
// Test: TopN (FetchRel on top of SortRel → TopNNode)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, FetchOnSortProducesTopNNode) {
  substrait::Rel rel;
  auto* fetch = rel.mutable_fetch();

  // Input: SortRel → TableScan.
  auto* sort = fetch->mutable_input()->mutable_sort();
  auto* input_read = sort->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("scores");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "student_id");
  addI64Column(schema, "score");

  auto* sort_field = sort->add_sorts();
  setFieldRef(sort_field->mutable_expr(), 1);
  sort_field->set_direction(
      substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST);

  fetch->mutable_offset_expr()->mutable_literal()->set_i64(0);
  fetch->mutable_count_expr()->mutable_literal()->set_i64(5);

  auto plan_bytes = serializePlan(rel, {"id", "value"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "TopN");
  // TopN may be wrapped by a Project rename node. Verify TopN exists in tree.
  auto node = result;
  while (node && node->name() != "TopN") {
    ASSERT_GE(node->sources().size(), 1u);
    node = node->sources()[0];
  }
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->name(), "TopN");
}

// ---------------------------------------------------------------------------
// Test: Window (ConsistentPartitionWindowRel → WindowNode)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, WindowProducesWindowNode) {
  substrait::Rel rel;
  auto* window = rel.mutable_window();

  auto* input_read = window->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("sales");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "region_id");
  addI64Column(schema, "amount");
  addI64Column(schema, "sale_date");

  // Partition by field 0 (region_id).
  setFieldRef(window->add_partition_expressions(), 0);

  // Sort by field 2 (sale_date).
  auto* sort_field = window->add_sorts();
  setFieldRef(sort_field->mutable_expr(), 2);
  sort_field->set_direction(
      substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST);

  // Window function: sum(amount) over partition.
  auto* wf = window->add_window_functions();
  wf->set_function_reference(100);
  wf->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
  wf->set_bounds_type(substrait::Expression_WindowFunction::BOUNDS_TYPE_ROWS);
  auto* arg = wf->add_arguments();
  setFieldRef(arg->mutable_value(), 1);
  wf->mutable_output_type()->mutable_i64();
  wf->mutable_lower_bound()->mutable_unbounded();
  wf->mutable_upper_bound()->mutable_current_row();

  // Register the function in the plan extensions.
  substrait::Plan plan;
  auto* ext = plan.add_extensions();
  auto* ext_func = ext->mutable_extension_function();
  ext_func->set_function_anchor(100);
  ext_func->set_name("sum:i64");

  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  plan_rel->mutable_root()->add_names("region_id");
  plan_rel->mutable_root()->add_names("amount");
  plan_rel->mutable_root()->add_names("sale_date");
  plan_rel->mutable_root()->add_names("sum_amount");

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // The converter may add exchange/project nodes around the Window.
  // Verify a Window node exists in the plan tree.
  auto node = result;
  while (node && node->name() != "Window") {
    ASSERT_GE(node->sources().size(), 1u);
    node = node->sources()[0];
  }
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->name(), "Window");
}

// ---------------------------------------------------------------------------
// Test: UNION ALL (SetRel → LocalPartitionNode gather)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, UnionAllProducesLocalPartitionNode) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNION_ALL);

  // First input.
  auto* input1 = set->add_inputs()->mutable_read();
  input1->mutable_named_table()->add_names("table_a");
  auto* schema1 = input1->mutable_base_schema();
  addI64Column(schema1, "id");
  addI64Column(schema1, "value");

  // Second input.
  auto* input2 = set->add_inputs()->mutable_read();
  input2->mutable_named_table()->add_names("table_b");
  auto* schema2 = input2->mutable_base_schema();
  addI64Column(schema2, "id");
  addI64Column(schema2, "value");

  auto plan_bytes = serializePlan(rel, {"id", "value"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "LocalPartition");
  ASSERT_EQ(result->sources().size(), 2);
}

TEST_F(VeloxPlanConverterTest, UnionAllAlignsInputNamesByPosition) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNION_ALL);
  *set->add_inputs()->mutable_read() = buildReadRel("left", {"id", "value"});
  *set->add_inputs()->mutable_read() = buildReadRel("right", {"key", "amount"});

  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(rel, {"id", "value"}), {});

  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[1]->name(), "TableScan");
  const auto rightLogical = converter.getScanNodeOutputType(result->sources()[1]->id());
  ASSERT_TRUE(rightLogical.has_value());
  EXPECT_EQ(rightLogical.value()->names(), (std::vector<std::string>{"key", "amount"}));
  EXPECT_EQ(*result->sources()[0]->outputType(),
            *result->sources()[1]->outputType());
  auto output = std::dynamic_pointer_cast<const facebook::velox::RowType>(
      result->outputType());
  ASSERT_NE(output, nullptr);
  expectLogicalOutputField(converter, output, 0, "id");
  expectLogicalOutputField(converter, output, 1, "value");
}

TEST_F(VeloxPlanConverterTest, UnionAllRejectsIncompatibleInputTypes) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNION_ALL);
  *set->add_inputs()->mutable_read() = buildReadRel("left", {"id"});
  auto* right = set->add_inputs()->mutable_read();
  right->mutable_named_table()->add_names("right");
  addStringColumn(right->mutable_base_schema(), "id");

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST_F(VeloxPlanConverterTest, UnsupportedSetOperationFailsClosedEvenWithOneInput) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNSPECIFIED);
  *set->add_inputs()->mutable_read() = buildReadRel("left", {"id"});

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: INTERSECT (SetRel → LocalPartitionNode + Aggregation dedup)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, IntersectProducesAggregationDedup) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_INTERSECTION_PRIMARY);

  auto* input1 = set->add_inputs()->mutable_read();
  input1->mutable_named_table()->add_names("table_a");
  auto* schema1 = input1->mutable_base_schema();
  addI64Column(schema1, "id");

  auto* input2 = set->add_inputs()->mutable_read();
  input2->mutable_named_table()->add_names("table_b");
  auto* schema2 = input2->mutable_base_schema();
  addI64Column(schema2, "id");

  auto plan_bytes = serializePlan(rel, {"id"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // INTERSECT is implemented via chained semi-joins (HashJoinNode).
  // The converter may wrap with a Project rename node at the root.
  auto node = result;
  while (node && node->name() != "HashJoin") {
    ASSERT_GE(node->sources().size(), 1u);
    node = node->sources()[0];
  }
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->name(), "HashJoin");
}

// ---------------------------------------------------------------------------
// Test: EXCEPT (SetRel → HashJoinNode anti-join)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, ExceptProducesAntiJoin) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_MINUS_PRIMARY);

  auto* input1 = set->add_inputs()->mutable_read();
  input1->mutable_named_table()->add_names("table_a");
  auto* schema1 = input1->mutable_base_schema();
  addI64Column(schema1, "a_id");
  addI64Column(schema1, "a_value");

  auto* input2 = set->add_inputs()->mutable_read();
  input2->mutable_named_table()->add_names("table_b");
  auto* schema2 = input2->mutable_base_schema();
  addI64Column(schema2, "b_id");
  addI64Column(schema2, "b_value");

  auto plan_bytes = serializePlan(rel, {"a_id", "a_value"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // Canonical MINUS_PRIMARY performs distinct input handling around its anti-join.
  auto anti = result;
  while (anti && anti->name() != "HashJoin") {
    ASSERT_FALSE(anti->sources().empty());
    anti = anti->sources().front();
  }
  ASSERT_NE(anti, nullptr);
  EXPECT_EQ(anti->name(), "HashJoin");
  ASSERT_EQ(anti->sources().size(), 2);
}

// ---------------------------------------------------------------------------
// Test: Cast expression conversion
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, CastExpressionProducesCastNode) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();

  auto* input_read = project->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "int_col");

  // Project: CAST(int_col AS DOUBLE).
  auto* cast_expr = project->add_expressions()->mutable_cast();
  setFieldRef(cast_expr->mutable_input(), 0);
  cast_expr->mutable_type()->mutable_fp64();
  project->mutable_common()->mutable_emit()->add_output_mapping(1);

  auto plan_bytes = serializePlan(rel, {"cast_result"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
}

// ---------------------------------------------------------------------------
// Test: IfThen (CASE/WHEN) expression conversion
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, IfThenExpressionProducesCallExpr) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();

  auto* input_read = project->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  schema->add_names("condition");
  schema->mutable_struct_()->add_types()->mutable_bool_();

  // Project: CASE WHEN condition THEN 1 ELSE 0 END.
  auto* if_then = project->add_expressions()->mutable_if_then();
  auto* clause = if_then->add_ifs();
  // A canonical CASE condition is BOOLEAN, never an implicitly truthy BIGINT.
  setFieldRef(clause->mutable_if_(), 0);
  // Then: literal 1.
  clause->mutable_then()->mutable_literal()->set_i64(1);
  // Else: literal 0.
  if_then->mutable_else_()->mutable_literal()->set_i64(0);
  project->mutable_common()->mutable_emit()->add_output_mapping(1);

  auto plan_bytes = serializePlan(rel, {"case_result"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
}

// ---------------------------------------------------------------------------
// Test: SetRel with no inputs throws
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, SetRelNoInputsThrows) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  // No inputs added.

  auto plan_bytes = serializePlan(rel, {"order_id", "customer_id", "product_id", "price", "name"});
  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: Type conversion covers basic Substrait types
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, TableScanWithMixedTypes) {
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("mixed_types");
  auto* schema = read->mutable_base_schema();

  // Add various column types.
  schema->add_names("bool_col");
  schema->mutable_struct_()->add_types()->mutable_bool_();

  schema->add_names("int_col");
  schema->mutable_struct_()->add_types()->mutable_i32();

  schema->add_names("bigint_col");
  schema->mutable_struct_()->add_types()->mutable_i64();

  schema->add_names("double_col");
  schema->mutable_struct_()->add_types()->mutable_fp64();

  schema->add_names("string_col");
  schema->mutable_struct_()->add_types()->mutable_string();

  auto plan_bytes = serializePlan(rel, {"bool_col", "int_col", "bigint_col", "double_col", "string_col"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  auto output = result->outputType();
  ASSERT_EQ(output->size(), 5);
  expectLogicalOutputField(converter, output, 0, "bool_col");
  expectLogicalOutputField(converter, output, 4, "string_col");
}

// ---------------------------------------------------------------------------
// Test: getScanNodeOutputType records TableScan planNodeId → outputType
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, GetScanNodeOutputTypeReturnsMappingAfterConvert) {
  // Build a plan with a single TableScan.
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("lineitem");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "l_orderkey");
  addStringColumn(schema, "l_comment");

  auto plan_bytes = serializePlan(rel, {"l_orderkey", "l_comment"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // The first node id generated is "node_0" for the TableScan.
  auto scan_type = converter.getScanNodeOutputType("node_0");
  ASSERT_TRUE(scan_type.has_value());
  ASSERT_EQ(scan_type.value()->size(), 2);
  EXPECT_EQ(scan_type.value()->nameOf(0), "l_orderkey");
  EXPECT_EQ(scan_type.value()->nameOf(1), "l_comment");
}

TEST_F(VeloxPlanConverterTest, GetScanNodeOutputTypeReturnsNulloptForUnknownId) {
  // Build a plan with a single TableScan.
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("orders");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "order_id");

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  converter.convert(plan_bytes, {});

  // Query a non-existent node id.
  auto result = converter.getScanNodeOutputType("node_999");
  EXPECT_FALSE(result.has_value());
}

TEST_F(VeloxPlanConverterTest, GetScanNodeOutputTypeClearedOnReconvert) {
  VeloxPlanConverter converter;

  // First conversion: single scan → node_0.
  substrait::Rel rel1;
  auto* read1 = rel1.mutable_read();
  read1->mutable_named_table()->add_names("t1");
  addI64Column(read1->mutable_base_schema(), "a");
  converter.convert(serializePlan(rel1, {"a"}), {});
  ASSERT_TRUE(converter.getScanNodeOutputType("node_0").has_value());

  // Second conversion resets state: node_0 now has different schema.
  substrait::Rel rel2;
  auto* read2 = rel2.mutable_read();
  read2->mutable_named_table()->add_names("t2");
  addStringColumn(read2->mutable_base_schema(), "x");
  addStringColumn(read2->mutable_base_schema(), "y");
  converter.convert(serializePlan(rel2, {"x", "y"}), {});

  auto scan_type = converter.getScanNodeOutputType("node_0");
  ASSERT_TRUE(scan_type.has_value());
  ASSERT_EQ(scan_type.value()->size(), 2);
  EXPECT_EQ(scan_type.value()->nameOf(0), "x");
  EXPECT_EQ(scan_type.value()->nameOf(1), "y");
}

TEST_F(VeloxPlanConverterTest, GetScanNodeOutputTypeWithHashJoinTwoScans) {
  // Build a HashJoin plan with two TableScan inputs.
  // This verifies both scan nodes are recorded.
  substrait::Rel rel;
  auto* join = rel.mutable_hash_join();
  join->set_type(substrait::HashJoinRel::JOIN_TYPE_INNER);

  // Left scan: orders(order_id, amount)
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("orders");
  addI64Column(left_read->mutable_base_schema(), "order_id");
  addI64Column(left_read->mutable_base_schema(), "amount");

  // Right scan: customers(cust_id, name)
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("customers");
  addI64Column(right_read->mutable_base_schema(), "cust_id");
  addStringColumn(right_read->mutable_base_schema(), "name");

  // Join keys: left[0] = right[0]
  addStandardJoinKey(join, 0, 0);

  auto plan_bytes = serializePlan(rel, {"order_id", "amount", "cust_id", "name"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // node_0 = left TableScan (orders)
  auto left_type = converter.getScanNodeOutputType("node_0");
  ASSERT_TRUE(left_type.has_value());
  ASSERT_EQ(left_type.value()->size(), 2);
  EXPECT_EQ(left_type.value()->nameOf(0), "order_id");

  // node_1 = right TableScan (customers)
  auto right_type = converter.getScanNodeOutputType("node_1");
  ASSERT_TRUE(right_type.has_value());
  ASSERT_EQ(right_type.value()->size(), 2);
  EXPECT_EQ(right_type.value()->nameOf(0), "cust_id");
  EXPECT_EQ(right_type.value()->nameOf(1), "name");
}

// ---------------------------------------------------------------------------
// Test: ExtensionSingleRel → UnnestNode (EXPLODE)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, ExtensionSingleRelProducesUnnestNode) {
  // Build an ExtensionSingleRel wrapping a ReadRel with an ARRAY column.
  // Canonical typed detail, with explicit output emit preserving the array.
  substrait::Rel rel;
  auto* ext = rel.mutable_extension_single();

  // Input: ReadRel with schema (id: I64, tags: LIST<STRING>)
  auto* input_read = ext->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("test_table");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "id");
  // Add LIST<STRING> column
  schema->add_names("tags");
  auto* list_type = schema->mutable_struct_()->add_types();
  list_type->mutable_list()->mutable_type()->mutable_string();

  pangrova::protocol::v2::RelationDetailEnvelope envelope;
  envelope.set_schema_version(1);
  auto* detail = envelope.mutable_table_function();
  detail->set_plan_node_id("unnest-with-preserved-array");
  const auto& identity = pangrova::capability::generated::pangrova_table_unnest;
  auto* invocation = detail->mutable_invocation();
  invocation->mutable_capability()->set_capability_id(identity.id);
  invocation->mutable_capability()->set_semantic_version(identity.version);
  invocation->mutable_capability()->set_signature_sha256(identity.signature.data(), identity.signature.size());
  auto* argument = invocation->add_arguments();
  argument->set_name("input_0");
  setFieldRef(argument->mutable_expression(), 1);
  for (const char* name : {"with_ordinality", "outer"}) {
    auto* option = invocation->add_options();
    option->set_name(name);
    option->mutable_value()->set_bool_value(false);
    auto* type = option->mutable_value()->mutable_type();
    type->set_canonical_sql("BOOLEAN");
    type->mutable_substrait_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  }
  detail->mutable_output_schema()->CopyFrom(*schema);
  addStringColumn(detail->mutable_output_schema(), "val");
  ext->mutable_detail()->PackFrom(envelope);
  for (int ordinal = 0; ordinal < 3; ++ordinal) ext->mutable_common()->mutable_emit()->add_output_mapping(ordinal);

  auto plan_bytes = serializePlan(rel, {"id", "__unnest_placeholder_1", "val"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // The declared emit has one exact physical projection for each output slot.
  EXPECT_EQ(result->name(), "Project");

  // ProjectNode output: all original input columns + unnest output columns.
  // The source column keeps its ARRAY type and value under a unique internal
  // name so a downstream stage can serialize it without an encoding mismatch.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 3);
  expectLogicalOutputField(converter, output_type, 0, "id");
  expectLogicalOutputField(converter, output_type, 1, "__unnest_placeholder_1");
  EXPECT_EQ(output_type->childAt(1)->toString(), "ARRAY<VARCHAR>");
  expectLogicalOutputField(converter, output_type, 2, "val");
  EXPECT_EQ(output_type->childAt(2)->toString(), "VARCHAR");

  // Verify the Project wraps an Unnest which wraps a TableScan.
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "Unnest");
  auto unnest = std::dynamic_pointer_cast<
      const facebook::velox::core::UnnestNode>(result->sources()[0]);
  ASSERT_NE(unnest, nullptr);
  ASSERT_EQ(unnest->replicateVariables().size(), 2);
  EXPECT_EQ(unnest->replicateVariables()[1]->name(), physicalFieldAlias(1));
  EXPECT_EQ(unnest->replicateVariables()[1]->type()->toString(),
            "ARRAY<VARCHAR>");
  auto project = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(result);
  ASSERT_NE(project, nullptr);
  auto source = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(
          project->projections()[1]);
  ASSERT_NE(source, nullptr);
  EXPECT_EQ(source->name(), physicalFieldAlias(1));
  EXPECT_EQ(source->type()->toString(), "ARRAY<VARCHAR>");
  ASSERT_EQ(result->sources()[0]->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: CrossRel → NestedLoopJoinNode (cross product, no condition)
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, CrossRelProducesNestedLoopJoinNode) {
  substrait::Rel rel;
  auto* cross = rel.mutable_cross();

  // Left input: orders (order_id, customer_id).
  auto* left_read = cross->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("orders");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "order_id");
  addI64Column(left_schema, "customer_id");

  // Right input: products (product_id, price, name).
  auto* right_read = cross->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("products");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "product_id");
  addI64Column(right_schema, "price");
  addStringColumn(right_schema, "name");

  auto plan_bytes = serializePlan(rel, {"order_id", "customer_id", "product_id", "price", "name"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema: combined [left_col_0..N-1, right_col_0..M-1].
  // Left has 2 cols, right has 3 cols → total 5.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 5);
  expectLogicalOutputField(converter, output_type, 0, "order_id");
  expectLogicalOutputField(converter, output_type, 1, "customer_id");
  expectLogicalOutputField(converter, output_type, 2, "product_id");
  expectLogicalOutputField(converter, output_type, 3, "price");
  expectLogicalOutputField(converter, output_type, 4, "name");

  // Two children (left and right).
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
  EXPECT_EQ(result->sources()[1]->name(), "Project");
  ASSERT_EQ(result->sources()[1]->sources().size(), 1);
  EXPECT_EQ(result->sources()[1]->sources()[0]->name(), "TableScan");
}

TEST_F(VeloxPlanConverterTest, LeftSingleJoinRetainsCardinalityObligation) {
  substrait::Rel rel;
  auto* join = rel.mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT_SINGLE);
  auto* target = join->mutable_left()->mutable_read();
  target->mutable_named_table()->add_names("target_rows");
  addI64Column(target->mutable_base_schema(), "target_id");
  auto* source = join->mutable_right()->mutable_read();
  source->mutable_named_table()->add_names("source_rows");
  addI64Column(source->mutable_base_schema(), "source_id");

  VeloxPlanConverter converter;
  auto single_match = converter.convert(serializePlan(rel, {"target_id", "source_id"}), {});
  ASSERT_NE(single_match, nullptr);
  EXPECT_EQ(single_match->name(), "NestedLoopJoin");
  const std::vector<SingleMatchJoinRequirement> expectedRequirements{{single_match->id(), false}};
  EXPECT_EQ(converter.singleMatchJoinRequirements(), expectedRequirements);

  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
  ASSERT_NE(converter.convert(serializePlan(rel, {"target_id", "source_id"}), {}), nullptr);
  EXPECT_TRUE(converter.singleMatchJoinRequirements().empty());
}

// ---------------------------------------------------------------------------
// Test: JoinRel without equi keys → NestedLoopJoinNode with filter
// Verifies column reference resolution for combined schema.
// Left cols at indices 0..N-1, right cols at indices N..N+M-1.
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, JoinRelWithConditionResolvesColumnReferences) {
  // Build a Plan directly to register the "less_than" function extension.
  substrait::Plan plan;

  // Register "less_than" function with anchor 0.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("a");
  root->add_names("b");
  root->add_names("x");
  root->add_names("y");
  auto* join = root->mutable_input()->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_INNER);

  // Left input: left_table (a: I64, b: I64) — fields 0, 1 in combined schema.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "a");
  addI64Column(left_schema, "b");

  // Right input: right_table (x: I64, y: I64) — fields 2, 3 in combined schema.
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "x");
  addI64Column(right_schema, "y");

  // Condition: left.b < right.x → lt(field[1], field[2])
  // field[1] = left column "b" (index 1 in combined schema)
  // field[2] = right column "x" (index 2 in combined schema = right's first col)
  auto* expr = join->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);  // anchor for "lt"
  func->mutable_output_type()->mutable_bool_();
  // Arg 0: left field 1 (b).
  setFieldRef(func->add_arguments()->mutable_value(), 1);
  // Arg 1: right field 0 → combined index 2 (x).
  setFieldRef(func->add_arguments()->mutable_value(), 2);

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema: [a, b, x, y] — 4 columns total.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 4);
  expectLogicalOutputField(converter, output_type, 0, "a");
  expectLogicalOutputField(converter, output_type, 1, "b");
  expectLogicalOutputField(converter, output_type, 2, "x");
  expectLogicalOutputField(converter, output_type, 3, "y");

  // Verify the NLJ has a filter (join condition).
  auto nlj = std::dynamic_pointer_cast<
      const facebook::velox::core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr);
  ASSERT_NE(nlj->joinCondition(), nullptr);

  // The filter should be a CallTypedExpr for "lt" with two FieldAccessTypedExpr
  // children referencing "b" (left col 1) and "x" (right col 0 = combined 2).
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(nlj->joinCondition());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "lt");
  ASSERT_EQ(call->inputs().size(), 2);

  auto left_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[0]);
  ASSERT_NE(left_ref, nullptr);
  EXPECT_EQ(left_ref->name(), physicalFieldAlias(1));  // Left column at combined index 1.

  auto right_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[1]);
  ASSERT_NE(right_ref, nullptr);
  EXPECT_EQ(right_ref->name(), physicalFieldAlias(2));  // Right column at combined index 2.
}

// ---------------------------------------------------------------------------
// Test: JoinRel condition referencing last right column.
// Verifies that wider schemas (N left + M right) resolve correctly.
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, JoinRelConditionResolvesWideSchema) {
  substrait::Plan plan;

  // Register a non-equality function so the standard JoinRel selects NLJ.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("c0");
  root->add_names("c1");
  root->add_names("c2");
  root->add_names("d0");
  root->add_names("d1");
  root->add_names("d2");
  root->add_names("d3");
  auto* join = root->mutable_input()->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_INNER);

  // Left input: 3 columns (c0, c1, c2) — combined indices 0, 1, 2.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("wide_left");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "c0");
  addI64Column(left_schema, "c1");
  addI64Column(left_schema, "c2");

  // Right input: 4 columns (d0, d1, d2, d3) — combined indices 3, 4, 5, 6.
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("wide_right");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "d0");
  addI64Column(right_schema, "d1");
  addI64Column(right_schema, "d2");
  addI64Column(right_schema, "d3");

  // Condition: left.c0 < right.d3 → lt(field[0], field[6])
  // field[0] = left column "c0" (first left col)
  // field[6] = right column "d3" (last right col, combined index 3+3=6)
  auto* expr = join->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);  // anchor for "lt"
  func->mutable_output_type()->mutable_bool_();
  // Arg 0: left field 0 (c0).
  setFieldRef(func->add_arguments()->mutable_value(), 0);
  // Arg 1: right field 3 → combined index 6 (d3).
  setFieldRef(func->add_arguments()->mutable_value(), 6);

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema: 3 + 4 = 7 columns.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 7);
  expectLogicalOutputField(converter, output_type, 0, "c0");
  expectLogicalOutputField(converter, output_type, 6, "d3");

  // Verify filter references correct columns.
  auto nlj = std::dynamic_pointer_cast<
      const facebook::velox::core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr);
  ASSERT_NE(nlj->joinCondition(), nullptr);

  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(nlj->joinCondition());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "lt");
  ASSERT_EQ(call->inputs().size(), 2);

  auto left_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[0]);
  ASSERT_NE(left_ref, nullptr);
  EXPECT_EQ(left_ref->name(), physicalFieldAlias(0));  // Left col 0.

  auto right_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[1]);
  ASSERT_NE(right_ref, nullptr);
  EXPECT_EQ(right_ref->name(), physicalFieldAlias(6));  // Right col 3 = combined index 6.
}

// ---------------------------------------------------------------------------
// Test: JoinRel with out-of-bounds field reference throws.
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, JoinRelConditionOutOfBoundsThrows) {
  substrait::Plan plan;

  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("a");
  root->add_names("b");
  auto* join = root->mutable_input()->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_INNER);

  // Left: 1 column (a). Right: 1 column (b). Combined schema has 2 cols.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("t1");
  addI64Column(left_read->mutable_base_schema(), "a");

  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("t2");
  addI64Column(right_read->mutable_base_schema(), "b");

  // Condition references field[5] — out of bounds for 2-column combined schema.
  auto* expr = join->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);
  func->mutable_output_type()->mutable_bool_();
  setFieldRef(func->add_arguments()->mutable_value(), 0);
  setFieldRef(func->add_arguments()->mutable_value(), 5);  // Out of bounds!

  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: CrossRel with duplicate column names deduplicates correctly.
// ---------------------------------------------------------------------------

TEST_F(VeloxPlanConverterTest, CrossRelDeduplicatesDuplicateColumnNames) {
  substrait::Rel rel;
  auto* cross = rel.mutable_cross();

  // Both sides have column named "id".
  auto* left_read = cross->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("t1");
  addI64Column(left_read->mutable_base_schema(), "id");
  addI64Column(left_read->mutable_base_schema(), "val");

  auto* right_read = cross->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("t2");
  addI64Column(right_read->mutable_base_schema(), "id");
  addI64Column(right_read->mutable_base_schema(), "val");

  auto plan_bytes = serializePlan(rel, {"id", "val", "id_1", "val_1"});
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema should have 4 columns with deduplicated names.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 4);
  expectLogicalOutputField(converter, output_type, 0, "id");
  expectLogicalOutputField(converter, output_type, 1, "val");
  // Right side "id" and "val" should be deduplicated.
  expectLogicalOutputField(converter, output_type, 2, "id_1");
  expectLogicalOutputField(converter, output_type, 3, "val_1");
}

TEST_F(VeloxPlanConverterTest, StandardNestedLoopJoinLeftSemiOutputsLeftOnly) {
  substrait::Rel rel;
  auto* join = rel.mutable_nested_loop_join();
  join->set_type(substrait::NestedLoopJoinRel::JOIN_TYPE_LEFT_SEMI);

  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  addI64Column(left_read->mutable_base_schema(), "id");
  addI64Column(left_read->mutable_base_schema(), "left_payload");

  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  addI64Column(right_read->mutable_base_schema(), "id");
  addI64Column(right_read->mutable_base_schema(), "right_payload");

  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(rel, {"id", "left_payload"}), {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
  ASSERT_EQ(result->outputType()->size(), 2);
  expectLogicalOutputField(converter, result->outputType(), 0, "id");
  expectLogicalOutputField(converter, result->outputType(), 1, "left_payload");
  ASSERT_EQ(result->sources().size(), 1);
  auto filter = result->sources()[0];
  ASSERT_EQ(filter->name(), "Filter");
  ASSERT_EQ(filter->sources().size(), 1);
  auto nested_loop = filter->sources()[0];
  ASSERT_EQ(nested_loop->name(), "NestedLoopJoin");
  ASSERT_EQ(nested_loop->sources().size(), 2);
  EXPECT_EQ(nested_loop->sources()[1]->name(), "Project");
}

TEST_F(VeloxPlanConverterTest, StandardHashJoinLeftSemiOutputsLeftOnly) {
  substrait::Rel rel;
  auto* join = rel.mutable_hash_join();
  join->set_type(substrait::HashJoinRel::JOIN_TYPE_LEFT_SEMI);

  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  addI64Column(left_read->mutable_base_schema(), "left_id");
  addI64Column(left_read->mutable_base_schema(), "left_payload");
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  addI64Column(right_read->mutable_base_schema(), "right_id");
  addI64Column(right_read->mutable_base_schema(), "right_payload");
  addStandardJoinKey(join, 0, 0);

  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(rel, {"left_id", "left_payload"}), {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");
  ASSERT_EQ(result->outputType()->size(), 2);
  expectLogicalOutputField(converter, result->outputType(), 0, "left_id");
  expectLogicalOutputField(converter, result->outputType(), 1, "left_payload");
}

TEST_F(VeloxPlanConverterTest, StandardMergeJoinRightSemiOutputsRightOnly) {
  substrait::Rel rel;
  auto* join = rel.mutable_merge_join();
  join->set_type(substrait::MergeJoinRel::JOIN_TYPE_RIGHT_SEMI);

  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  addI64Column(left_read->mutable_base_schema(), "left_id");
  addI64Column(left_read->mutable_base_schema(), "left_payload");
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  addI64Column(right_read->mutable_base_schema(), "right_id");
  addI64Column(right_read->mutable_base_schema(), "right_payload");
  addStandardJoinKey(join, 0, 0);

  VeloxPlanConverter converter;
  auto result = converter.convert(serializePlan(rel, {"right_id", "right_payload"}), {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "MergeJoin");
  ASSERT_EQ(result->outputType()->size(), 2);
  expectLogicalOutputField(converter, result->outputType(), 0, "right_id");
  expectLogicalOutputField(converter, result->outputType(), 1, "right_payload");
}

}  // namespace
}  // namespace pangrova::velox::test
