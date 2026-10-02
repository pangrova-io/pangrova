// NljColumnReferenceResolutionTest: Parameterized tests for NestedLoopJoin
// column reference resolution in the combined output schema.
//
// Property 9: NestedLoopJoin Column Reference Resolution
//
// TEST_P over left/right schema widths (1-5 columns) × condition column
// positions. Verifies that the filter expression in NestedLoopJoinNode uses
// correct field indices for the combined schema (left cols 0..N-1, right cols
// N..N+M-1).
//
// **Validates: Requirements 4.4**

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "CanonicalOutputAssertions.h"
#include "velox/plan/VeloxPlanConverter.h"

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include "velox/core/PlanNode.h"
#include "velox/core/Expressions.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

// ===========================================================================
// Test parameters: left_width × right_width × condition_left_col ×
//                  condition_right_col
// ===========================================================================

struct NljColumnRefParam {
  std::string name;
  int left_width;          // Number of columns in left schema (1-5).
  int right_width;         // Number of columns in right schema (1-5).
  int condition_left_col;  // Left column index used in condition (0-based).
  int condition_right_col; // Right column index used in condition (0-based).
};

// ===========================================================================
// Helpers
// ===========================================================================

namespace {

void addI64Column(substrait::NamedStruct* schema, const std::string& name) {
  schema->add_names(name);
  schema->mutable_struct_()->add_types()->mutable_i64();
}

void setFieldRef(substrait::Expression* expr, int field_index) {
  auto* ref = expr->mutable_selection();
  ref->mutable_root_reference();
  ref->mutable_direct_reference()->mutable_struct_field()->set_field(
      field_index);
}

// Generate column name for left side: l0, l1, l2, ...
std::string leftColName(int idx) {
  return "l" + std::to_string(idx);
}

// Generate column name for right side: r0, r1, r2, ...
std::string rightColName(int idx) {
  return "r" + std::to_string(idx);
}

// Build a Substrait Plan with a JoinRel that has a condition comparing
// a left column to a right column using "lt" (less than).
// Left schema has left_width columns, right schema has right_width columns.
// Condition: left[condition_left_col] < right[condition_right_col]
// In the combined schema, left columns are at indices 0..left_width-1,
// right columns are at indices left_width..left_width+right_width-1.
std::vector<uint8_t> buildNljPlan(
    int left_width,
    int right_width,
    int condition_left_col,
    int condition_right_col) {
  substrait::Plan plan;

  // Register "lt" function with anchor 0.
  auto* ext = plan.add_extensions();
  ext->mutable_extension_function()->set_function_anchor(0);
  ext->mutable_extension_function()->set_name("lt:i64_i64");

  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  for (int i = 0; i < left_width; ++i) root->add_names(leftColName(i));
  for (int i = 0; i < right_width; ++i) root->add_names(rightColName(i));
  auto* join = root->mutable_input()->mutable_join();
  join->set_type(substrait::JoinRel::JOIN_TYPE_INNER);

  // Left input with left_width columns.
  auto* left_read = join->mutable_left()->mutable_read();
  left_read->mutable_named_table()->add_names("left_table");
  auto* left_schema = left_read->mutable_base_schema();
  for (int i = 0; i < left_width; ++i) {
    addI64Column(left_schema, leftColName(i));
  }

  // Right input with right_width columns.
  auto* right_read = join->mutable_right()->mutable_read();
  right_read->mutable_named_table()->add_names("right_table");
  auto* right_schema = right_read->mutable_base_schema();
  for (int i = 0; i < right_width; ++i) {
    addI64Column(right_schema, rightColName(i));
  }

  // Condition: left[condition_left_col] < right[condition_right_col]
  // In combined schema:
  //   left col index = condition_left_col (unchanged, 0-based in combined)
  //   right col index = left_width + condition_right_col
  auto* expr = join->mutable_expression();
  auto* func = expr->mutable_scalar_function();
  func->set_function_reference(0);  // anchor for "lt"
  func->mutable_output_type()->mutable_bool_();
  // Arg 0: left column at combined index = condition_left_col.
  setFieldRef(func->add_arguments()->mutable_value(), condition_left_col);
  // Arg 1: right column at combined index = left_width + condition_right_col.
  setFieldRef(
      func->add_arguments()->mutable_value(),
      left_width + condition_right_col);

  std::string bytes;
  plan.SerializeToString(&bytes);
  return {bytes.begin(), bytes.end()};
}

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class NljColumnReferenceResolutionTest
    : public ::testing::TestWithParam<NljColumnRefParam> {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
};

// ===========================================================================
// Property 9.1: Filter Expression Uses Correct Field Indices
//
// For any left schema width N and right schema width M, when the condition
// references left column at index L and right column at index R, the
// converted filter expression must resolve to:
//   - Left field name = "l{L}" (at combined index L)
//   - Right field name = "r{R}" (at combined index N+R)
//
// **Validates: Requirements 4.4**
// ===========================================================================

TEST_P(NljColumnReferenceResolutionTest, FilterUsesCorrectFieldIndices) {
  const auto& param = GetParam();
  auto plan_bytes = buildNljPlan(
      param.left_width,
      param.right_width,
      param.condition_left_col,
      param.condition_right_col);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name(), "NestedLoopJoin");

  // Verify output schema has correct total width.
  auto output_type = result->outputType();
  int expected_total = param.left_width + param.right_width;
  ASSERT_EQ(static_cast<int>(output_type->size()), expected_total)
      << "Combined schema should have " << expected_total << " columns for "
      << param.name;

  // Verify left columns are at indices 0..N-1.
  for (int i = 0; i < param.left_width; ++i) {
    expectCanonicalOutputField(converter, output_type, i, leftColName(i));
  }

  // Verify right columns are at indices N..N+M-1.
  for (int i = 0; i < param.right_width; ++i) {
    expectCanonicalOutputField(converter, output_type, param.left_width + i, rightColName(i));
  }

  // Extract the NestedLoopJoinNode and verify filter.
  auto nlj = std::dynamic_pointer_cast<
      const facebook::velox::core::NestedLoopJoinNode>(result);
  ASSERT_NE(nlj, nullptr);
  ASSERT_NE(nlj->joinCondition(), nullptr)
      << "NLJ should have a join condition for " << param.name;

  // The filter should be a CallTypedExpr for "lt" with two FieldAccessTypedExpr.
  auto call = std::dynamic_pointer_cast<
      const facebook::velox::core::CallTypedExpr>(nlj->joinCondition());
  ASSERT_NE(call, nullptr)
      << "Filter should be a CallTypedExpr for " << param.name;
  EXPECT_EQ(call->name(), "lt");
  ASSERT_EQ(call->inputs().size(), 2u);

  // Verify left field reference resolves to the correct column name.
  auto left_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[0]);
  ASSERT_NE(left_ref, nullptr)
      << "First arg should be FieldAccessTypedExpr for " << param.name;
  EXPECT_EQ(left_ref->name(), physicalFieldAlias(param.condition_left_col))
      << "Left field should reference column "
      << leftColName(param.condition_left_col)
      << " (combined index " << param.condition_left_col << ")";

  // Verify right field reference resolves to the correct column name.
  auto right_ref = std::dynamic_pointer_cast<
      const facebook::velox::core::FieldAccessTypedExpr>(call->inputs()[1]);
  ASSERT_NE(right_ref, nullptr)
      << "Second arg should be FieldAccessTypedExpr for " << param.name;
  EXPECT_EQ(right_ref->name(), physicalFieldAlias(param.left_width + param.condition_right_col))
      << "Right field should reference column "
      << rightColName(param.condition_right_col)
      << " (combined index "
      << (param.left_width + param.condition_right_col) << ")";
}

// ===========================================================================
// Property 9.2: Combined Schema Column Ordering Is Left-Then-Right
//
// For any left schema width N and right schema width M, the combined output
// schema must have exactly N+M columns with left columns at positions
// 0..N-1 and right columns at positions N..N+M-1.
//
// **Validates: Requirements 4.4**
// ===========================================================================

TEST_P(NljColumnReferenceResolutionTest, CombinedSchemaOrderIsLeftThenRight) {
  const auto& param = GetParam();
  auto plan_bytes = buildNljPlan(
      param.left_width,
      param.right_width,
      param.condition_left_col,
      param.condition_right_col);
  ASSERT_FALSE(plan_bytes.empty());

  VeloxPlanConverter converter;
  auto result = converter.convert(plan_bytes, {});
  ASSERT_NE(result, nullptr);

  auto output_type = result->outputType();
  int expected_total = param.left_width + param.right_width;
  ASSERT_EQ(static_cast<int>(output_type->size()), expected_total);

  // All columns should be I64 type.
  for (int i = 0; i < expected_total; ++i) {
    EXPECT_TRUE(output_type->childAt(i)->equivalent(*BIGINT()))
        << "Column at index " << i << " should be BIGINT for " << param.name;
  }

  // Verify the ordering: left columns first, then right columns.
  for (int i = 0; i < param.left_width; ++i) {
    expectCanonicalOutputField(converter, output_type, i, leftColName(i));
  }
  for (int i = 0; i < param.right_width; ++i) {
    expectCanonicalOutputField(converter, output_type, param.left_width + i, rightColName(i));
  }
}

// ===========================================================================
// Parameterization: left_width × right_width × condition column positions
//
// Covers all combinations of schema widths 1-5 with various condition
// column positions (first, last, middle where applicable).
// ===========================================================================

std::vector<NljColumnRefParam> generateParams() {
  std::vector<NljColumnRefParam> params;

  // For each left_width (1-5) × right_width (1-5), test condition referencing:
  //   - First left col (0) + first right col (0)
  //   - Last left col (N-1) + last right col (M-1)
  //   - Middle positions where applicable
  for (int lw = 1; lw <= 5; ++lw) {
    for (int rw = 1; rw <= 5; ++rw) {
      // First left × first right.
      params.push_back({
          "L" + std::to_string(lw) + "_R" + std::to_string(rw) +
              "_FirstFirst",
          lw, rw, 0, 0});

      // Last left × last right.
      params.push_back({
          "L" + std::to_string(lw) + "_R" + std::to_string(rw) +
              "_LastLast",
          lw, rw, lw - 1, rw - 1});

      // Middle left × middle right (only when width >= 3).
      if (lw >= 3 && rw >= 3) {
        int mid_l = lw / 2;
        int mid_r = rw / 2;
        params.push_back({
            "L" + std::to_string(lw) + "_R" + std::to_string(rw) +
                "_MidMid",
            lw, rw, mid_l, mid_r});
      }
    }
  }

  return params;
}

INSTANTIATE_TEST_SUITE_P(
    NljColumnRefResolution,
    NljColumnReferenceResolutionTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<NljColumnRefParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
