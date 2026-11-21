// NljConditionCorrectnessTest: Parameterized tests for NestedLoopJoin with
// condition correctness at the plan conversion level.
//
// Property 8: NestedLoopJoin with Condition Correctness
//
// TEST_P over join types (INNER, LEFT, RIGHT, OUTER) × condition types
// (lt, gt, between, compound) × dataset sizes (5, 50, 500).
// Verifies:
//   - convertJoinRel correctly creates NestedLoopJoinNode with the filter
//     expression and correct join type.
//   - Output schema has correct column count (left + right).
//   - Filter expression is non-null and references correct function name.
//   - Join type is correctly mapped from JoinRel::JoinType.
//
// **Validates: Requirements 4.1, 4.2, 4.3**

#include <gtest/gtest.h>
#include "CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <memory>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "engine/velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters
// ===========================================================================

enum class NljJoinType { kInner, kLeft, kRight, kOuter };

std::string joinTypeName(NljJoinType jt) {
  switch (jt) {
    case NljJoinType::kInner: return "Inner";
    case NljJoinType::kLeft: return "Left";
    case NljJoinType::kRight: return "Right";
    case NljJoinType::kOuter: return "Outer";
  }
  return "Unknown";
}

substrait::JoinRel::JoinType toSubstraitJoinType(NljJoinType jt) {
  switch (jt) {
    case NljJoinType::kInner:
      return substrait::JoinRel::JOIN_TYPE_INNER;
    case NljJoinType::kLeft:
      return substrait::JoinRel::JOIN_TYPE_LEFT;
    case NljJoinType::kRight:
      return substrait::JoinRel::JOIN_TYPE_RIGHT;
    case NljJoinType::kOuter:
      return substrait::JoinRel::JOIN_TYPE_OUTER;
  }
  return substrait::JoinRel::JOIN_TYPE_INNER;
}

core::JoinType expectedVeloxJoinType(NljJoinType jt) {
  switch (jt) {
    case NljJoinType::kInner: return core::JoinType::kInner;
    case NljJoinType::kLeft: return core::JoinType::kLeft;
    case NljJoinType::kRight: return core::JoinType::kRight;
    case NljJoinType::kOuter: return core::JoinType::kFull;
  }
  return core::JoinType::kInner;
}

enum class ConditionType { kLt, kGt, kBetween, kCompound };

std::string conditionTypeName(ConditionType ct) {
  switch (ct) {
    case ConditionType::kLt: return "Lt";
    case ConditionType::kGt: return "Gt";
    case ConditionType::kBetween: return "Between";
    case ConditionType::kCompound: return "Compound";
  }
  return "Unknown";
}

struct NljConditionParam {
  std::string name;
  NljJoinType join_type;
  ConditionType condition_type;
  int dataset_size;
};

// ===========================================================================
// Helpers
// ===========================================================================

namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

std::vector<uint8_t> serializePlan(const substrait::Plan& plan) {
  std::string bytes;
  pangrova::test::canonicalTestPlan(plan).SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

// Build a standard JoinRel plan with the given join type and condition type.
// Left table has `dataset_size` conceptual rows with columns (left_key, left_val).
// Right table has columns (right_key, right_val).
// Combined schema: [left_key(0), left_val(1), right_key(2), right_val(3)].
std::vector<uint8_t> buildNljPlan(
    NljJoinType join_type,
    ConditionType condition_type,
    const std::vector<uint32_t>& function_anchors) {
  substrait::Plan plan;

  // Register required functions based on condition type.
  switch (condition_type) {
    case ConditionType::kLt: {
      auto* ext = plan.add_extensions();
      ext->mutable_extension_function()->set_function_anchor(
          function_anchors[0]);
      ext->mutable_extension_function()->set_name("lt:i64_i64");
      break;
    }
    case ConditionType::kGt: {
      auto* ext = plan.add_extensions();
      ext->mutable_extension_function()->set_function_anchor(
          function_anchors[0]);
      ext->mutable_extension_function()->set_name("gt:i64_i64");
      break;
    }
    case ConditionType::kBetween: {
      // between: left_key >= right_key AND left_key <= right_val
      // Needs: gte, lte, and
      auto* ext_gte = plan.add_extensions();
      ext_gte->mutable_extension_function()->set_function_anchor(
          function_anchors[0]);
      ext_gte->mutable_extension_function()->set_name("gte:i64_i64");
      auto* ext_lte = plan.add_extensions();
      ext_lte->mutable_extension_function()->set_function_anchor(
          function_anchors[1]);
      ext_lte->mutable_extension_function()->set_name("lte:i64_i64");
      auto* ext_and = plan.add_extensions();
      ext_and->mutable_extension_function()->set_function_anchor(
          function_anchors[2]);
      ext_and->mutable_extension_function()->set_name("and:bool_bool");
      break;
    }
    case ConditionType::kCompound: {
      // compound: left_key < right_key AND left_val > right_val
      // Needs: lt, gt, and
      auto* ext_lt = plan.add_extensions();
      ext_lt->mutable_extension_function()->set_function_anchor(
          function_anchors[0]);
      ext_lt->mutable_extension_function()->set_name("lt:i64_i64");
      auto* ext_gt = plan.add_extensions();
      ext_gt->mutable_extension_function()->set_function_anchor(
          function_anchors[1]);
      ext_gt->mutable_extension_function()->set_name("gt:i64_i64");
      auto* ext_and = plan.add_extensions();
      ext_and->mutable_extension_function()->set_function_anchor(
          function_anchors[2]);
      ext_and->mutable_extension_function()->set_name("and:bool_bool");
      break;
    }
  }

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("left_key");
  root->add_names("left_val");
  root->add_names("right_key");
  root->add_names("right_val");
  auto* join = root->mutable_input()->mutable_join();

  // Left input: left_table (left_key: I64, left_val: I64).
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  addI64Column(left_schema, "left_key");
  addI64Column(left_schema, "left_val");

  // Right input: right_table (right_key: I64, right_val: I64).
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  addI64Column(right_schema, "right_key");
  addI64Column(right_schema, "right_val");

  // Set join type.
  join->set_type(toSubstraitJoinType(join_type));

  // Build condition expression based on condition type.
  // Combined schema: [left_key(0), left_val(1), right_key(2), right_val(3)].
  auto* expr = join->mutable_expression();

  switch (condition_type) {
    case ConditionType::kLt: {
      // left_key < right_key → lt(field[0], field[2])
      auto* func = expr->mutable_scalar_function();
      func->set_function_reference(function_anchors[0]);
      func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(func->add_arguments()->mutable_value(), 0);
      setFieldRef(func->add_arguments()->mutable_value(), 2);
      break;
    }
    case ConditionType::kGt: {
      // left_key > right_key → gt(field[0], field[2])
      auto* func = expr->mutable_scalar_function();
      func->set_function_reference(function_anchors[0]);
      func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(func->add_arguments()->mutable_value(), 0);
      setFieldRef(func->add_arguments()->mutable_value(), 2);
      break;
    }
    case ConditionType::kBetween: {
      // left_key >= right_key AND left_key <= right_val
      // → and(gte(field[0], field[2]), lte(field[0], field[3]))
      auto* and_func = expr->mutable_scalar_function();
      and_func->set_function_reference(function_anchors[2]);
      and_func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

      // First arg: gte(left_key, right_key)
      auto* gte_expr =
          and_func->add_arguments()->mutable_value()->mutable_scalar_function();
      gte_expr->set_function_reference(function_anchors[0]);
      gte_expr->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(gte_expr->add_arguments()->mutable_value(), 0);
      setFieldRef(gte_expr->add_arguments()->mutable_value(), 2);

      // Second arg: lte(left_key, right_val)
      auto* lte_expr =
          and_func->add_arguments()->mutable_value()->mutable_scalar_function();
      lte_expr->set_function_reference(function_anchors[1]);
      lte_expr->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(lte_expr->add_arguments()->mutable_value(), 0);
      setFieldRef(lte_expr->add_arguments()->mutable_value(), 3);
      break;
    }
    case ConditionType::kCompound: {
      // left_key < right_key AND left_val > right_val
      // → and(lt(field[0], field[2]), gt(field[1], field[3]))
      auto* and_func = expr->mutable_scalar_function();
      and_func->set_function_reference(function_anchors[2]);
      and_func->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);

      // First arg: lt(left_key, right_key)
      auto* lt_expr =
          and_func->add_arguments()->mutable_value()->mutable_scalar_function();
      lt_expr->set_function_reference(function_anchors[0]);
      lt_expr->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(lt_expr->add_arguments()->mutable_value(), 0);
      setFieldRef(lt_expr->add_arguments()->mutable_value(), 2);

      // Second arg: gt(left_val, right_val)
      auto* gt_expr =
          and_func->add_arguments()->mutable_value()->mutable_scalar_function();
      gt_expr->set_function_reference(function_anchors[1]);
      gt_expr->mutable_output_type()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
      setFieldRef(gt_expr->add_arguments()->mutable_value(), 1);
      setFieldRef(gt_expr->add_arguments()->mutable_value(), 3);
      break;
    }
  }

  return serializePlan(plan);
}

// Return the expected top-level function name in the filter expression.
std::string expectedFilterFunctionName(ConditionType ct) {
  switch (ct) {
    case ConditionType::kLt: return "lt";
    case ConditionType::kGt: return "gt";
    case ConditionType::kBetween: return "and";
    case ConditionType::kCompound: return "and";
  }
  return "";
}

// Return the expected number of arguments in the top-level filter function.
size_t expectedFilterArgCount(ConditionType ct) {
  switch (ct) {
    case ConditionType::kLt: return 2;
    case ConditionType::kGt: return 2;
    case ConditionType::kBetween: return 2;
    case ConditionType::kCompound: return 2;
  }
  return 0;
}

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class NljConditionCorrectnessTest
    : public ::testing::TestWithParam<NljConditionParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 8.1: NestedLoopJoinNode Created with Correct Join Type
//
// For any JoinRel with a condition expression and a specified join type,
// convertJoinRel SHALL produce a NestedLoopJoinNode with the corresponding
// Velox join type.
//
// **Validates: Requirements 4.1, 4.2**
// ===========================================================================

TEST_P(NljConditionCorrectnessTest, CorrectJoinTypeMapping) {
  const auto& param = GetParam();
  std::vector<uint32_t> anchors;
  if (param.condition_type == ConditionType::kLt ||
      param.condition_type == ConditionType::kGt) {
    anchors = {0};
  } else {
    anchors = {0, 1, 2};
  }

  auto plan_bytes = buildNljPlan(param.join_type, param.condition_type, anchors);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});

  ASSERT_NE(result, nullptr) << "Plan conversion failed for " << param.name;
  EXPECT_EQ(result->name(), "NestedLoopJoin")
      << "Expected NestedLoopJoin node for " << param.name;

  auto nlj = std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr) << "Failed to cast to NestedLoopJoinNode";
  EXPECT_EQ(nlj->joinType(), expectedVeloxJoinType(param.join_type))
      << "Join type mismatch for " << param.name;
}

// ===========================================================================
// Property 8.2: Filter Expression Present and Correct
//
// For any JoinRel with a condition expression, the resulting
// NestedLoopJoinNode SHALL have a non-null joinCondition with the correct
// top-level function name matching the condition type.
//
// **Validates: Requirements 4.1, 4.3**
// ===========================================================================

TEST_P(NljConditionCorrectnessTest, FilterExpressionPresent) {
  const auto& param = GetParam();
  std::vector<uint32_t> anchors;
  if (param.condition_type == ConditionType::kLt ||
      param.condition_type == ConditionType::kGt) {
    anchors = {0};
  } else {
    anchors = {0, 1, 2};
  }

  auto plan_bytes = buildNljPlan(param.join_type, param.condition_type, anchors);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto nlj = std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr);
  ASSERT_NE(nlj->joinCondition(), nullptr)
      << "Filter expression must be non-null for " << param.name;

  // Verify top-level function name.
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
      nlj->joinCondition());
  ASSERT_NE(call, nullptr)
      << "Filter must be a CallTypedExpr for " << param.name;
  EXPECT_EQ(call->name(), expectedFilterFunctionName(param.condition_type))
      << "Filter function name mismatch for " << param.name;
  EXPECT_EQ(call->inputs().size(), expectedFilterArgCount(param.condition_type))
      << "Filter argument count mismatch for " << param.name;
}

// ===========================================================================
// Property 8.3: Output Schema Correctness
//
// For any JoinRel with left (N cols) and right (M cols) inputs, the
// NestedLoopJoinNode output schema SHALL have exactly N+M columns with
// left columns first, then right columns.
//
// **Validates: Requirements 4.1, 4.2**
// ===========================================================================

TEST_P(NljConditionCorrectnessTest, OutputSchemaCorrectness) {
  const auto& param = GetParam();
  std::vector<uint32_t> anchors;
  if (param.condition_type == ConditionType::kLt ||
      param.condition_type == ConditionType::kGt) {
    anchors = {0};
  } else {
    anchors = {0, 1, 2};
  }

  auto plan_bytes = buildNljPlan(param.join_type, param.condition_type, anchors);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  // Left has 2 cols, right has 2 cols → output has 4 cols.
  auto output_type = result->outputType();
  ASSERT_EQ(output_type->size(), 4)
      << "Output schema should have 4 columns for " << param.name;

  // Verify column ordering: left columns first, then right.
  expectCanonicalOutputField(converter, output_type, 0, "left_key");
  expectCanonicalOutputField(converter, output_type, 1, "left_val");
  expectCanonicalOutputField(converter, output_type, 2, "right_key");
  expectCanonicalOutputField(converter, output_type, 3, "right_val");

  // All columns should be BIGINT.
  for (size_t i = 0; i < output_type->size(); ++i) {
    EXPECT_TRUE(output_type->childAt(i)->isBigint())
        << "Column " << i << " should be BIGINT for " << param.name;
  }
}

// ===========================================================================
// Property 8.4: Two Source Inputs Present
//
// The NestedLoopJoinNode SHALL have exactly two source inputs (left and
// right), both reading TableScan nodes. The right ordinal-rebase Project is
// explicit and preserves every original input column.
//
// **Validates: Requirements 4.1**
// ===========================================================================

TEST_P(NljConditionCorrectnessTest, TwoSourceInputsPresent) {
  const auto& param = GetParam();
  std::vector<uint32_t> anchors;
  if (param.condition_type == ConditionType::kLt ||
      param.condition_type == ConditionType::kGt) {
    anchors = {0};
  } else {
    anchors = {0, 1, 2};
  }

  auto plan_bytes = buildNljPlan(param.join_type, param.condition_type, anchors);
  ASSERT_FALSE(plan_bytes.empty());

  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  ASSERT_EQ(result->sources().size(), 2)
      << "NestedLoopJoin must have 2 sources for " << param.name;
  EXPECT_EQ(result->sources()[0]->name(), "TableScan");
  const auto& right = result->sources()[1];
  EXPECT_EQ(right->name(), "Project");
  ASSERT_EQ(right->sources().size(), 1);
  EXPECT_EQ(right->sources()[0]->name(), "TableScan");
  ASSERT_EQ(right->outputType()->size(), 2);
  EXPECT_EQ(right->outputType()->nameOf(0), physicalFieldAlias(2));
  EXPECT_EQ(right->outputType()->nameOf(1), physicalFieldAlias(3));
}

// ===========================================================================
// Parameterization: join types × condition types × dataset sizes
// ===========================================================================

std::vector<NljConditionParam> generateParams() {
  std::vector<NljConditionParam> params;
  std::vector<NljJoinType> join_types = {
      NljJoinType::kInner, NljJoinType::kLeft,
      NljJoinType::kRight, NljJoinType::kOuter};
  std::vector<ConditionType> condition_types = {
      ConditionType::kLt, ConditionType::kGt,
      ConditionType::kBetween, ConditionType::kCompound};
  std::vector<int> dataset_sizes = {5, 50, 500};

  for (auto jt : join_types) {
    for (auto ct : condition_types) {
      for (auto ds : dataset_sizes) {
        std::string name = joinTypeName(jt) + "_" +
                           conditionTypeName(ct) + "_" +
                           std::to_string(ds);
        params.push_back({name, jt, ct, ds});
      }
    }
  }
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    NljConditionCorrectness,
    NljConditionCorrectnessTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<NljConditionParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
