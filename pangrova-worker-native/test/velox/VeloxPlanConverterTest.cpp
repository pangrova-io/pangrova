// VeloxPlanConverterTest: Verifies Substrait → Velox PlanNode conversion
// for each core operator type.
//
// Tests build Substrait Plan protobuf programmatically, call convert(),
// and verify the returned PlanNode tree has the correct type and structure.
//
// **Validates: Requirements 1.61, 2.61**
#include <gtest/gtest.h>

#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"

#include <google/protobuf/wrappers.pb.h>

namespace pangrova::velox::test {
namespace {

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
std::vector<uint8_t> serializePlan(const substrait::Rel& rel) {
  substrait::Plan plan;
  auto* plan_rel = plan.add_relations();
  *plan_rel->mutable_root()->mutable_input() = rel;
  plan_rel->mutable_root()->add_names("output");

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Helper: set a field reference expression pointing to field index.
void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Helper: set a field reference on a FieldReference message (for join keys).
void setJoinKeyRef(
    substrait::Expression::FieldReference* ref, int field_index) {
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// ---------------------------------------------------------------------------
// Test: Empty / invalid input
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, EmptyBytesReturnsNull) {
  VeloxPlanConverter converter;
  auto result = converter.convert({}, {});
  EXPECT_EQ(result, nullptr);
}

TEST(VeloxPlanConverterTest, InvalidProtobufReturnsNull) {
  VeloxPlanConverter converter;
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(converter.convert(garbage, {}), std::runtime_error);
}

TEST(VeloxPlanConverterTest, EmptyPlanReturnsNull) {
  substrait::Plan plan;
  std::string bytes;
  plan.SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  EXPECT_EQ(result, nullptr);
}

// ---------------------------------------------------------------------------
// Test: TableScan (ReadRel)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, TableScanProducesTableScanNode) {
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("orders");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "order_id");
  addI64Column(schema, "customer_id");
  addStringColumn(schema, "status");

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "TableScan");

  // Verify output schema has 3 columns.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 3);
  EXPECT_EQ(output_type->nameOf(0), "order_id");
  EXPECT_EQ(output_type->nameOf(1), "customer_id");
  EXPECT_EQ(output_type->nameOf(2), "status");
}

// ---------------------------------------------------------------------------
// Test: Filter (FilterRel → TableScan)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, FilterProducesFilterNode) {
  substrait::Rel rel;
  auto* filter = rel.mutable_filter();

  // Input: TableScan on "orders" with 2 columns.
  auto* input_read = filter->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("orders");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "order_id");
  addI64Column(schema, "amount");

  // Condition: reference to field 0 (order_id).
  setFieldRef(filter->mutable_condition(), 0);

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, ProjectProducesProjectNode) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");

  // Child should be TableScan.
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: Aggregate (AggregateRel → TableScan)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, AggregateProducesAggregationNode) {
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
  setFieldRef(grouping->add_grouping_expressions(), 0);

  // Measure: aggregate on field 1 (amount).
  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
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
  setFieldRef(grouping->add_grouping_expressions(), 0);

  auto* measure = agg->add_measures();
  auto* agg_func = measure->mutable_measure();
  agg_func->set_function_reference(1);
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

TEST(VeloxPlanConverterTest, AggregateMeasureFilterSetsMask) {
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
  EXPECT_EQ(aggregate.mask->name(), "is_valid");
  EXPECT_EQ(
      aggregate.mask->type()->kind(), facebook::velox::TypeKind::BOOLEAN);

  // Any upstream (final) aggregation steps must not carry the mask.
  for (size_t i = 0; i + 1 < agg_nodes.size(); ++i) {
    ASSERT_EQ(agg_nodes[i]->aggregates().size(), 1u);
    EXPECT_EQ(agg_nodes[i]->aggregates()[0].mask, nullptr);
  }
}

TEST(VeloxPlanConverterTest, AggregateWithoutMeasureFilterHasNoMask) {
  auto plan_bytes = buildAggregateWithFilterPlan(/*filter_field=*/-1);

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  const auto* agg_node = findAggregationNode(result);
  ASSERT_NE(agg_node, nullptr);
  ASSERT_EQ(agg_node->aggregates().size(), 1u);
  EXPECT_EQ(agg_node->aggregates()[0].mask, nullptr);
}

TEST(VeloxPlanConverterTest, AggregateMeasureFilterOnNonBooleanColumnThrows) {
  // Filter references field 1 (amount, i64) — planner contract violation,
  // conversion must fail loud instead of silently dropping the filter.
  auto plan_bytes = buildAggregateWithFilterPlan(/*filter_field=*/1);

  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: HashJoin (HashJoinRel with two TableScan inputs)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, HashJoinProducesHashJoinNode) {
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
  setJoinKeyRef(join->add_left_keys(), 1);
  setJoinKeyRef(join->add_right_keys(), 0);

  join->set_type(substrait::HashJoinRel::JOIN_TYPE_INNER);

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");

  // Should have two children (left and right).
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
  EXPECT_EQ(result->sources()[1]->name(), "TableScan");

  // Output should have 4 columns (2 from left + 2 from right).
  EXPECT_EQ(result->outputType()->size(), 4);
}

// ---------------------------------------------------------------------------
// Test: Exchange (ExchangeRel → TableScan)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, ExchangeProducesExchangeNode) {
  substrait::Rel rel;
  auto* exchange = rel.mutable_exchange();

  // Input: TableScan.
  auto* input_read = exchange->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("lineitem");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "item_id");
  addI64Column(schema, "quantity");

  exchange->set_partition_count(4);

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Exchange");

  // Exchange output type should match input.
  EXPECT_EQ(result->outputType()->size(), 2);
}

// ---------------------------------------------------------------------------
// Test: Nested plan (Project → Filter → TableScan)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, NestedPlanProjectFilterTableScan) {
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

  // Filter condition: field reference to field 1 (user_id).
  setFieldRef(filter->mutable_condition(), 1);

  // Project: select field 0 and field 2.
  setFieldRef(project->add_expressions(), 0);
  setFieldRef(project->add_expressions(), 2);

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, LogicalJoinProducesHashJoinNode) {
  // Build a Plan directly (not via serializePlan) so we can register
  // the "equal" function extension needed by extractEquiKeys.
  substrait::Plan plan;

  // Register "equal" function with anchor 0.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("equal:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");
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

// ---------------------------------------------------------------------------
// Test: Unsupported rel type throws with type name
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, UnsupportedRelTypeThrowsWithTypeName) {
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

TEST(VeloxPlanConverterTest, UnsupportedExpressionTypeThrows) {
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

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, SortProducesOrderByNode) {
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

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, FetchProducesLimitNode) {
  substrait::Rel rel;
  auto* fetch = rel.mutable_fetch();

  auto* input_read = fetch->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("items");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "item_id");
  addI64Column(schema, "price");

  fetch->set_offset(5);
  fetch->set_count(10);

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, FetchOnSortProducesTopNNode) {
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

  fetch->set_offset(0);
  fetch->set_count(5);

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, WindowProducesWindowNode) {
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
  plan_rel->mutable_root()->add_names("output");

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

TEST(VeloxPlanConverterTest, UnionAllProducesLocalPartitionNode) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "LocalPartition");
  ASSERT_EQ(result->sources().size(), 2);
}

// ---------------------------------------------------------------------------
// Test: INTERSECT (SetRel → LocalPartitionNode + Aggregation dedup)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, IntersectProducesAggregationDedup) {
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

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, ExceptProducesAntiJoin) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // EXCEPT produces HashJoin (anti-join).
  EXPECT_EQ(result->name(), "HashJoin");
  ASSERT_EQ(result->sources().size(), 2);
}

// ---------------------------------------------------------------------------
// Test: Cast expression conversion
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, CastExpressionProducesCastNode) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
}

// ---------------------------------------------------------------------------
// Test: IfThen (CASE/WHEN) expression conversion
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, IfThenExpressionProducesCallExpr) {
  substrait::Rel rel;
  auto* project = rel.mutable_project();

  auto* input_read = project->mutable_input()->mutable_read();
  input_read->mutable_named_table()->add_names("data");
  auto* schema = input_read->mutable_base_schema();
  addI64Column(schema, "value");

  // Project: CASE WHEN value > 0 THEN 1 ELSE 0 END
  auto* if_then = project->add_expressions()->mutable_if_then();
  auto* clause = if_then->add_ifs();
  // Condition: field reference (simplified — just use field ref as boolean).
  setFieldRef(clause->mutable_if_(), 0);
  // Then: literal 1.
  clause->mutable_then()->mutable_literal()->set_i64(1);
  // Else: literal 0.
  if_then->mutable_else_()->mutable_literal()->set_i64(0);

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
}

// ---------------------------------------------------------------------------
// Test: SetRel with no inputs throws
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, SetRelNoInputsThrows) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  // No inputs added.

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Test: Type conversion covers basic Substrait types
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, TableScanWithMixedTypes) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  auto output = result->outputType();
  ASSERT_EQ(output->size(), 5);
  EXPECT_EQ(output->nameOf(0), "bool_col");
  EXPECT_EQ(output->nameOf(4), "string_col");
}

// ---------------------------------------------------------------------------
// Test: getScanNodeOutputType records TableScan planNodeId → outputType
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, GetScanNodeOutputTypeReturnsMappingAfterConvert) {
  // Build a plan with a single TableScan.
  substrait::Rel rel;
  auto* read = rel.mutable_read();
  read->mutable_named_table()->add_names("lineitem");
  auto* schema = read->mutable_base_schema();
  addI64Column(schema, "l_orderkey");
  addStringColumn(schema, "l_comment");

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, GetScanNodeOutputTypeReturnsNulloptForUnknownId) {
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

TEST(VeloxPlanConverterTest, GetScanNodeOutputTypeClearedOnReconvert) {
  VeloxPlanConverter converter;

  // First conversion: single scan → node_0.
  substrait::Rel rel1;
  auto* read1 = rel1.mutable_read();
  read1->mutable_named_table()->add_names("t1");
  addI64Column(read1->mutable_base_schema(), "a");
  converter.convert(serializePlan(rel1), {});
  ASSERT_TRUE(converter.getScanNodeOutputType("node_0").has_value());

  // Second conversion resets state: node_0 now has different schema.
  substrait::Rel rel2;
  auto* read2 = rel2.mutable_read();
  read2->mutable_named_table()->add_names("t2");
  addStringColumn(read2->mutable_base_schema(), "x");
  addStringColumn(read2->mutable_base_schema(), "y");
  converter.convert(serializePlan(rel2), {});

  auto scan_type = converter.getScanNodeOutputType("node_0");
  ASSERT_TRUE(scan_type.has_value());
  ASSERT_EQ(scan_type.value()->size(), 2);
  EXPECT_EQ(scan_type.value()->nameOf(0), "x");
  EXPECT_EQ(scan_type.value()->nameOf(1), "y");
}

TEST(VeloxPlanConverterTest, GetScanNodeOutputTypeWithHashJoinTwoScans) {
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
  setJoinKeyRef(join->add_left_keys(), 0);
  setJoinKeyRef(join->add_right_keys(), 0);

  auto plan_bytes = serializePlan(rel);
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

TEST(VeloxPlanConverterTest, ExtensionSingleRelProducesUnnestNode) {
  // Build an ExtensionSingleRel wrapping a ReadRel with an ARRAY column.
  // The detail payload encodes UNNEST metadata as JSON.
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

  // Detail: JSON payload for unnest metadata
  std::string json = R"({"op":"unnest","unnestColumns":[1],"unnestNames":["val"],"ordinality":false})";
  google::protobuf::StringValue sv;
  sv.set_value(json);
  ext->mutable_detail()->PackFrom(sv);

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  // The convertExtensionSingleRel wraps UnnestNode in a ProjectNode
  // to remap column indices (correlate layout alignment).
  EXPECT_EQ(result->name(), "Project");

  // ProjectNode output: all input columns + unnest output columns
  // = (id, <placeholder>, val) — 3 columns matching correlate row type.
  // The unnest SOURCE column position carries a unique placeholder name
  // (index alignment only; downstream ProjectRel references by index) to
  // avoid collision with the unnest OUTPUT column of the same name.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 3);
  EXPECT_EQ(output_type->nameOf(0), "id");
  EXPECT_EQ(output_type->nameOf(1), "__unnest_placeholder_1");
  EXPECT_EQ(output_type->nameOf(2), "val");

  // Verify the Project wraps an Unnest which wraps a TableScan.
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "Unnest");
  ASSERT_EQ(result->sources()[0]->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->sources()[0]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: CrossRel → NestedLoopJoinNode (cross product, no condition)
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, CrossRelProducesNestedLoopJoinNode) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema: combined [left_col_0..N-1, right_col_0..M-1].
  // Left has 2 cols, right has 3 cols → total 5.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 5);
  EXPECT_EQ(output_type->nameOf(0), "order_id");
  EXPECT_EQ(output_type->nameOf(1), "customer_id");
  EXPECT_EQ(output_type->nameOf(2), "product_id");
  EXPECT_EQ(output_type->nameOf(3), "price");
  EXPECT_EQ(output_type->nameOf(4), "name");

  // Two children (left and right).
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
  EXPECT_EQ(result->sources()[1]->name(), "TableScan");
}

// ---------------------------------------------------------------------------
// Test: CrossRel with condition → NestedLoopJoinNode with filter
// Verifies column reference resolution for combined schema.
// Left cols at indices 0..N-1, right cols at indices N..N+M-1.
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, CrossRelWithConditionResolvesColumnReferences) {
  // Build a Plan directly to register the "less_than" function extension.
  substrait::Plan plan;

  // Register "less_than" function with anchor 0.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");
  auto* cross = root->mutable_input()->mutable_cross();

  // Left input: left_table (a: I64, b: I64) — fields 0, 1 in combined schema.
  auto* left_read = cross->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "a");
  addI64Column(left_schema, "b");

  // Right input: right_table (x: I64, y: I64) — fields 2, 3 in combined schema.
  auto* right_read = cross->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "x");
  addI64Column(right_schema, "y");

  // Condition: left.b < right.x → lt(field[1], field[2])
  // field[1] = left column "b" (index 1 in combined schema)
  // field[2] = right column "x" (index 2 in combined schema = right's first col)
  auto* expr = cross->mutable_expression();
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
  EXPECT_EQ(output_type->nameOf(0), "a");
  EXPECT_EQ(output_type->nameOf(1), "b");
  EXPECT_EQ(output_type->nameOf(2), "x");
  EXPECT_EQ(output_type->nameOf(3), "y");

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
  EXPECT_EQ(left_ref->name(), "b");  // Left column at combined index 1.

  auto right_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[1]);
  ASSERT_NE(right_ref, nullptr);
  EXPECT_EQ(right_ref->name(), "x");  // Right column at combined index 2.
}

// ---------------------------------------------------------------------------
// Test: CrossRel with condition referencing last right column.
// Verifies that wider schemas (N left + M right) resolve correctly.
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, CrossRelConditionResolvesWideSchema) {
  substrait::Plan plan;

  // Register "equal" function with anchor 0.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("equal:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");
  auto* cross = root->mutable_input()->mutable_cross();

  // Left input: 3 columns (c0, c1, c2) — combined indices 0, 1, 2.
  auto* left_read = cross->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("wide_left");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "c0");
  addI64Column(left_schema, "c1");
  addI64Column(left_schema, "c2");

  // Right input: 4 columns (d0, d1, d2, d3) — combined indices 3, 4, 5, 6.
  auto* right_read = cross->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("wide_right");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "d0");
  addI64Column(right_schema, "d1");
  addI64Column(right_schema, "d2");
  addI64Column(right_schema, "d3");

  // Condition: left.c0 = right.d3 → equal(field[0], field[6])
  // field[0] = left column "c0" (first left col)
  // field[6] = right column "d3" (last right col, combined index 3+3=6)
  auto* expr = cross->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);  // anchor for "equal"
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
  EXPECT_EQ(output_type->nameOf(0), "c0");
  EXPECT_EQ(output_type->nameOf(6), "d3");

  // Verify filter references correct columns.
  auto nlj = std::dynamic_pointer_cast<
      const facebook::velox::core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr);
  ASSERT_NE(nlj->joinCondition(), nullptr);

  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(nlj->joinCondition());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), "eq");  // Velox maps "equal" → "eq"
  ASSERT_EQ(call->inputs().size(), 2);

  auto left_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[0]);
  ASSERT_NE(left_ref, nullptr);
  EXPECT_EQ(left_ref->name(), "c0");  // Left col 0.

  auto right_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[1]);
  ASSERT_NE(right_ref, nullptr);
  EXPECT_EQ(right_ref->name(), "d3");  // Right col 3 = combined index 6.
}

// ---------------------------------------------------------------------------
// Test: CrossRel with out-of-bounds field reference throws.
// ---------------------------------------------------------------------------

TEST(VeloxPlanConverterTest, CrossRelConditionOutOfBoundsThrows) {
  substrait::Plan plan;

  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("output");
  auto* cross = root->mutable_input()->mutable_cross();

  // Left: 1 column (a). Right: 1 column (b). Combined schema has 2 cols.
  auto* left_read = cross->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("t1");
  addI64Column(left_read->mutable_base_schema(), "a");

  auto* right_read = cross->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("t2");
  addI64Column(right_read->mutable_base_schema(), "b");

  // Condition references field[5] — out of bounds for 2-column combined schema.
  auto* expr = cross->mutable_expression();
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

TEST(VeloxPlanConverterTest, CrossRelDeduplicatesDuplicateColumnNames) {
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

  auto plan_bytes = serializePlan(rel);
  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Output schema should have 4 columns with deduplicated names.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 4);
  EXPECT_EQ(output_type->nameOf(0), "id");
  EXPECT_EQ(output_type->nameOf(1), "val");
  // Right side "id" and "val" should be deduplicated.
  EXPECT_EQ(output_type->nameOf(2), "id_1");
  EXPECT_EQ(output_type->nameOf(3), "val_1");
}

}  // namespace
}  // namespace pangrova::velox::test
