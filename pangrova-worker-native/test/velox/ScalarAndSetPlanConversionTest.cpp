#include "PlanConversionTestFixture.h"
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::velox::test {

TEST_F(VeloxPlanConverterTest, UnionAllAlignsInputNamesByPosition) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNION_ALL);
  *set->add_inputs()->mutable_read() = buildReadRel("left", {"id", "value"});
  *set->add_inputs()->mutable_read() = buildReadRel("right", {"key", "amount"});

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  EXPECT_THROW(converter.convert(serializePlan(rel), {}), std::runtime_error);
}

TEST_F(VeloxPlanConverterTest, UnsupportedSetOperationFailsClosedEvenWithOneInput) {
  substrait::Rel rel;
  auto* set = rel.mutable_set();
  set->set_op(substrait::SetRel::SET_OP_UNSPECIFIED);
  *set->add_inputs()->mutable_read() = buildReadRel("left", {"id"});

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  cast_expr->mutable_type()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  project->mutable_common()->mutable_emit()->add_output_mapping(1);

  auto plan_bytes = serializePlan(rel, {"cast_result"});
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  schema->add_names("int_col");
  schema->mutable_struct_()->add_types()->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  schema->add_names("bigint_col");
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  schema->add_names("double_col");
  schema->mutable_struct_()->add_types()->mutable_fp64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  schema->add_names("string_col");
  schema->mutable_struct_()->add_types()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

  auto plan_bytes = serializePlan(rel, {"bool_col", "int_col", "bigint_col", "double_col", "string_col"});
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  converter.convert(plan_bytes, {});

  // Query a non-existent node id.
  auto result = converter.getScanNodeOutputType("node_999");
  EXPECT_FALSE(result.has_value());
}

TEST_F(VeloxPlanConverterTest, GetScanNodeOutputTypeClearedOnReconvert) {
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  list_type->mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  list_type->mutable_list()->mutable_type()->mutable_string()
      ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  // Arg 0: left field 1 (b).
  setFieldRef(func->add_arguments()->mutable_value(), 1);
  // Arg 1: right field 0 → combined index 2 (x).
  setFieldRef(func->add_arguments()->mutable_value(), 2);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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


}  // namespace pangrova::velox::test
