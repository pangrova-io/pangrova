#include "PlanConversionTestFixture.h"
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::velox::test {

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
  func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  // Arg 0: left field 0 (c0).
  setFieldRef(func->add_arguments()->mutable_value(), 0);
  // Arg 1: right field 3 → combined index 6 (d3).
  setFieldRef(func->add_arguments()->mutable_value(), 6);

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  setFieldRef(func->add_arguments()->mutable_value(), 0);
  setFieldRef(func->add_arguments()->mutable_value(), 5);  // Out of bounds!

  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  std::vector<uint8_t> plan_bytes(bytes.begin(), bytes.end());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
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

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(serializePlan(rel, {"right_id", "right_payload"}), {});

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "Project");
  ASSERT_EQ(result->sources().size(), 1);
  EXPECT_EQ(result->sources()[0]->name(), "MergeJoin");
  ASSERT_EQ(result->outputType()->size(), 2);
  expectLogicalOutputField(converter, result->outputType(), 0, "right_id");
  expectLogicalOutputField(converter, result->outputType(), 1, "right_payload");
}


}  // namespace pangrova::velox::test
