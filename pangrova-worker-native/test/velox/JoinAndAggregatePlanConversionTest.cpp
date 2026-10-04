#include "PlanConversionTestFixture.h"
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::velox::test {

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");

  // Should have two children (left and right).
  ASSERT_EQ(result->sources().size(), 2);
  expectFieldProjection(result->sources()[0], {0, 1, 1});
  expectFieldProjection(result->sources()[1], {0, 1, 0});
  ASSERT_EQ(result->sources()[0]->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->sources()[0]->name(), "TableScan");
  ASSERT_EQ(result->sources()[1]->sources().size(), 1);
  auto rebased = result->sources()[1]->sources()[0];
  expectFieldProjection(rebased, {0, 1});
  ASSERT_EQ(rebased->sources().size(), 1);
  EXPECT_EQ(rebased->sources()[0]->name(), "TableScan");
  auto hash = std::dynamic_pointer_cast<const facebook::velox::core::HashJoinNode>(result);
  ASSERT_NE(hash, nullptr);
  ASSERT_EQ(hash->leftKeys().size(), 1);
  ASSERT_EQ(hash->rightKeys().size(), 1);
  EXPECT_EQ(hash->leftKeys()[0]->name(), hash->sources()[0]->outputType()->nameOf(2));
  EXPECT_EQ(hash->rightKeys()[0]->name(), hash->sources()[1]->outputType()->nameOf(2));

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(
      plan_bytes, {}, false, "ArrowBatch", {}, nullptr, {}, 4);

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(plan_bytes, {}), std::runtime_error);
  EXPECT_THROW(converter.convert(
      plan_bytes, {}, false, "ArrowBatch", {}, nullptr, {}, 2),
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");

  // Emit → appended expressions → Filter → TableScan.
  expectFieldProjection(result, {3, 4});
  ASSERT_EQ(result->sources().size(), 1);
  auto appended = result->sources()[0];
  expectFieldProjection(appended, {0, 1, 2, 0, 2});
  ASSERT_EQ(appended->sources().size(), 1);
  auto filter_node = appended->sources()[0];
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
  func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  // Arg 0: left field 1 (customer_id).
  setFieldRef(func->add_arguments()->mutable_value(), 1);
  // Arg 1: right field 0 → combined index 2 (id).
  setFieldRef(func->add_arguments()->mutable_value(), 2);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("left_key");
  root->add_names("right_key");
  auto* join = root->mutable_input()->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_LEFT);
  auto* left = join->mutable_left()->mutable_read();
  left->mutable_named_table()->add_names("left_table");
  addI64Column(left->mutable_base_schema(), "left_key");
  auto* right = join->mutable_right()->mutable_read();
  right->mutable_named_table()->add_names("right_table");
  addI64Column(right->mutable_base_schema(), "right_key");
  auto* equality = join->mutable_expression()->mutable_scalar_function();
  equality->set_function_reference(0);
  equality->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  setFieldRef(equality->add_arguments()->mutable_value(), 0);
  setFieldRef(equality->add_arguments()->mutable_value(), 1);

  std::string bytes;
  ASSERT_TRUE(pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes));
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("id");
  auto* join = root->mutable_input()->mutable_join();
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
  equality->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  setFieldRef(equality->add_arguments()->mutable_value(), 0);
  setFieldRef(equality->add_arguments()->mutable_value(), 1);

  std::string bytes;
  ASSERT_TRUE(pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes));
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert({bytes.begin(), bytes.end()}, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "HashJoin");
  ASSERT_EQ(result->sources().size(), 2);
  EXPECT_EQ(result->sources()[1]->name(), "Project");
  expectFieldProjection(result->sources()[1], {0, 1});
  EXPECT_EQ(result->outputType()->size(), 1);
  auto anti = std::dynamic_pointer_cast<const facebook::velox::core::HashJoinNode>(result);
  ASSERT_NE(anti, nullptr);
  EXPECT_EQ(anti->joinType(), facebook::velox::core::JoinType::kAnti);
  ASSERT_EQ(anti->leftKeys().size(), 1);
  ASSERT_EQ(anti->rightKeys().size(), 1);
  EXPECT_EQ(anti->leftKeys()[0]->name(), anti->sources()[0]->outputType()->nameOf(0));
  EXPECT_EQ(anti->rightKeys()[0]->name(), anti->sources()[1]->outputType()->nameOf(0));
  expectLogicalOutputField(converter, result->outputType(), 0, "id");
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
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  auto final = std::dynamic_pointer_cast<const facebook::velox::core::LimitNode>(result);
  ASSERT_NE(final, nullptr);
  EXPECT_FALSE(final->isPartial());
  EXPECT_EQ(final->offset(), 5);
  EXPECT_EQ(final->count(), 10);
  ASSERT_EQ(final->sources().size(), 1);
  auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(
      final->sources().front());
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
  ASSERT_EQ(gather->sources().size(), 1);
  auto partial = std::dynamic_pointer_cast<const facebook::velox::core::LimitNode>(
      gather->sources().front());
  ASSERT_NE(partial, nullptr);
  EXPECT_TRUE(partial->isPartial());
  EXPECT_EQ(partial->offset(), 0);
  EXPECT_EQ(partial->count(), 15);
  ASSERT_EQ(partial->sources().size(), 1);
  EXPECT_EQ(partial->sources().front()->name(), "TableScan");
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "TopN");
  auto final = std::dynamic_pointer_cast<const facebook::velox::core::TopNNode>(result);
  ASSERT_NE(final, nullptr);
  EXPECT_FALSE(final->isPartial());
  EXPECT_EQ(final->count(), 5);
  ASSERT_EQ(final->sortingKeys().size(), 1);
  EXPECT_EQ(final->sortingKeys().front()->name(), physicalFieldAlias(1));
  EXPECT_EQ(final->sortingOrders().front(), facebook::velox::core::SortOrder(false, false));
  ASSERT_EQ(final->sources().size(), 1);
  auto gather = std::dynamic_pointer_cast<const facebook::velox::core::LocalPartitionNode>(
      final->sources().front());
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), facebook::velox::core::LocalPartitionNode::Type::kGather);
  ASSERT_EQ(gather->sources().size(), 1);
  auto partial = std::dynamic_pointer_cast<const facebook::velox::core::TopNNode>(
      gather->sources().front());
  ASSERT_NE(partial, nullptr);
  EXPECT_TRUE(partial->isPartial());
  EXPECT_EQ(partial->count(), 5);
  EXPECT_EQ(partial->sortingKeys().front()->name(), final->sortingKeys().front()->name());
  EXPECT_EQ(partial->sortingOrders(), final->sortingOrders());
  ASSERT_EQ(partial->sources().size(), 1);
  EXPECT_EQ(partial->sources().front()->name(), "TableScan");
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
  wf->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
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
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "LocalPartition");
  ASSERT_EQ(result->sources().size(), 2);
}


}  // namespace pangrova::velox::test
