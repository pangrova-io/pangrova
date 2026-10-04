#include "PlanConversionTestFixture.h"
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "GovernedPlanTestBindings.h"

namespace pangrova::velox::test {

TEST_F(VeloxPlanConverterTest, EmptyBytesReturnsNull) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert({}, {});
  EXPECT_EQ(result, nullptr);
}

TEST_F(VeloxPlanConverterTest, InvalidProtobufReturnsNull) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(converter.convert(garbage, {}), std::runtime_error);
}

TEST_F(VeloxPlanConverterTest, EmptyPlanReturnsNull) {
  substrait::Plan plan;
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  EXPECT_EQ(result, nullptr);
}

TEST_F(VeloxPlanConverterTest, LiveValuesPlansHaveDistinctMemoryPools) {
  substrait::Rel rel;
  addI64Column(rel.mutable_read()->mutable_base_schema(), "value");
  auto* row = rel.mutable_read()->mutable_virtual_table()->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(42);
  auto plan_bytes = serializePlan(rel);

  const auto first_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter first(first_registry);
  const auto second_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter second(second_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");

  // Substrait appends expressions before emit selects the two appended fields.
  expectFieldProjection(result, {3, 4});
  ASSERT_EQ(result->sources().size(), 1);
  auto appended = result->sources().front();
  expectFieldProjection(appended, {0, 1, 2, 0, 2});
  ASSERT_EQ(appended->sources().size(), 1);
  EXPECT_EQ(appended->sources().front()->name(), "TableScan");
  expectLogicalOutputField(converter, result->outputType(), 0, "id");
  expectLogicalOutputField(converter, result->outputType(), 1, "age");
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("completion");
  auto* project = root->mutable_input()->mutable_project();
  project->mutable_common()->mutable_emit()->add_output_mapping(1);
  auto* read = project->mutable_input()->mutable_read();
  read->mutable_named_table()->add_names("prompts");
  addStringColumn(read->mutable_base_schema(), "prompt");
  auto* function = project->add_expressions()->mutable_scalar_function();
  function->set_function_reference(0);
  function->mutable_output_type()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("catalog.llm");
  setFieldRef(function->add_arguments()->mutable_value(), 0);
  function->add_arguments()
      ->mutable_value()
      ->mutable_literal()
      ->set_string("ai-invoke-1");

  auto binding = governedCompleteBinding(function->output_type());

  std::string bytes;
  ASSERT_TRUE(pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes));
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}});

  auto project_node = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(result);
  ASSERT_NE(project_node, nullptr);
  ASSERT_EQ(project_node->projections().size(), 1);
  expectFieldProjection(project_node, {1});
  expectLogicalOutputField(converter, result->outputType(), 0, "completion");
  auto appended = std::dynamic_pointer_cast<const facebook::velox::core::ProjectNode>(
      project_node->sources().front());
  ASSERT_NE(appended, nullptr);
  ASSERT_EQ(appended->projections().size(), 2);
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(
      appended->projections()[1]);
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

  const auto missing_binding_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter missing_binding_converter(missing_binding_converter_registry);
  EXPECT_THROW(
      missing_binding_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch", {}),
      std::runtime_error);

  auto drifted = binding;
  drifted.mutable_model()->mutable_model()->set_id("catalog.other-model");
  const auto drifted_binding_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter drifted_binding_converter(drifted_binding_converter_registry);
  EXPECT_THROW(
      drifted_binding_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
          {{drifted.plan_node_id(), drifted}}),
      std::runtime_error);

  auto output_drifted = binding;
  output_drifted.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->clear_string();
  output_drifted.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->mutable_fp64()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  const auto output_drifted_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter output_drifted_converter(output_drifted_converter_registry);
  EXPECT_THROW(
      output_drifted_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
          {{output_drifted.plan_node_id(), output_drifted}}),
      std::runtime_error);

  // Calcite can infer a required expression for non-null literals while the
  // model advertises a nullable SQL result. This is not a physical type drift.
  auto nullable_model_output = binding;
  nullable_model_output.mutable_model()->mutable_output_type()
      ->mutable_substrait_type()->mutable_string()->set_nullability(
          substrait::Type_Nullability_NULLABILITY_NULLABLE);
  const auto nullable_output_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter nullable_output_converter(nullable_output_converter_registry);
  EXPECT_NO_THROW(nullable_output_converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
      {{nullable_model_output.plan_node_id(), nullable_model_output}}));
}

TEST_F(VeloxPlanConverterTest, GovernedAiEmbedCarriesExactVectorDimension) {
  registerGovernedAiSignature("ai_embed", "VECTOR");
  substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(0);
  extension->set_name("ai_embed:string_string_string");
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("embedding");
  auto* project = root->mutable_input()->mutable_project();
  project->mutable_common()->mutable_emit()->add_output_mapping(1);
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

  auto binding = governedEmbeddingBinding(function->output_type());

  std::string bytes;
  ASSERT_TRUE(pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes));
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
      {{binding.plan_node_id(), binding}});

  auto project_node = std::dynamic_pointer_cast<
      const facebook::velox::core::ProjectNode>(result);
  ASSERT_NE(project_node, nullptr);
  expectFieldProjection(project_node, {1});
  expectLogicalOutputField(converter, result->outputType(), 0, "embedding");
  auto appended = std::dynamic_pointer_cast<const facebook::velox::core::ProjectNode>(
      project_node->sources().front());
  ASSERT_NE(appended, nullptr);
  ASSERT_EQ(appended->projections().size(), 2);
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(
      appended->projections()[1]);
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
  const auto drifted_converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter drifted_converter(drifted_converter_registry);
  EXPECT_ANY_THROW(
      drifted_converter.convert(
          {bytes.begin(), bytes.end()}, {}, false, "ArrowBatch",
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
  agg_func->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

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
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  field->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
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
  agg_func->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
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
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

}  // namespace pangrova::velox::test
