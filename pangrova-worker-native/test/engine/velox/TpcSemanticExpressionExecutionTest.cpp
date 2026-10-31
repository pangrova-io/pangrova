// Actual canonical converter and published-registry Task value/type oracles.
// These are Native component tests, not public SQL/JNI or distributed E2E.
#include "CanonicalPublishedTaskTest.h"

#include <optional>
#include <string>
#include <vector>

#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

class TpcSemanticExpressionExecutionTest : public CanonicalPublishedTaskTest {
 protected:
  struct CaseRow {
    std::optional<bool> condition;
    std::optional<int64_t> price;
    std::optional<int64_t> discount;
  };

  static substrait::Expression scalar(uint32_t anchor,
      const substrait::Type& output, const std::vector<substrait::Expression>& arguments) {
    substrait::Expression expression;
    auto* function = expression.mutable_scalar_function();
    function->set_function_reference(anchor);
    *function->mutable_output_type() = output;
    for (const auto& argument : arguments) *function->add_arguments()->mutable_value() = argument;
    return expression;
  }

  static substrait::Expression literal(const substrait::Expression::Literal& value) {
    substrait::Expression expression;
    *expression.mutable_literal() = value;
    return expression;
  }

  static substrait::Plan casePlan(const std::vector<CaseRow>& rows,
      bool sum = false, int productPrecision = 31, int sumPrecision = 38) {
    substrait::Plan plan;
    declare(plan, 1, "subtract");
    declare(plan, 2, "multiply");
    auto* root = plan.add_relations()->mutable_root();
    root->add_names(sum ? "revenue" : "case_amount");
    auto* projectRel = root->mutable_input();
    if (sum) {
      declare(plan, 3, "sum");
      auto* aggregate = projectRel->mutable_aggregate();
      aggregate->add_groupings();
      auto* function = aggregate->add_measures()->mutable_measure();
      function->set_function_reference(3);
      function->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
      function->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
      *function->mutable_output_type() = decimalType(sumPrecision, 4);
      *function->add_arguments()->mutable_value() = field(0);
      projectRel = aggregate->mutable_input();
    }
    auto* project = projectRel->mutable_project();
    auto* read = project->mutable_input()->mutable_read();
    auto* schema = read->mutable_base_schema();
    for (const auto* name : {"condition", "price", "discount"}) schema->add_names(name);
    schema->mutable_struct_()->add_types()->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    for (int ordinal = 0; ordinal < 2; ++ordinal) *schema->mutable_struct_()->add_types() = decimalType(15, 2);
    read->mutable_virtual_table();
    for (const auto& row : rows) {
      auto* record = read->mutable_virtual_table()->add_expressions();
      auto* condition = record->add_fields()->mutable_literal();
      if (row.condition) {
        condition->set_boolean(*row.condition);
        condition->set_nullable(true);
      } else *condition->mutable_null() = schema->struct_().types(0);
      for (const auto& value : {row.price, row.discount}) {
        auto* cell = record->add_fields()->mutable_literal();
        if (value) *cell = decimalValue(15, 2, *value);
        else *cell->mutable_null() = decimalType(15, 2);
      }
    }
    const auto discountFactor = scalar(1, decimalType(16, 2),
        {literal(decimalValue(15, 2, 100)), field(2)});
    const auto revenue = scalar(2, decimalType(productPrecision, 4), {field(1), discountFactor});
    auto* caseExpression = project->add_expressions()->mutable_if_then();
    auto* clause = caseExpression->add_ifs();
    *clause->mutable_if_() = field(0);
    *clause->mutable_then() = revenue;
    *caseExpression->mutable_else_() = literal(decimalValue(31, 4, 0));
    project->mutable_common()->mutable_emit()->add_output_mapping(3);
    return plan;
  }

  static std::vector<std::optional<int128_t>> decimalRows(
      const CanonicalTaskOutput& output, int precision, int scale) {
    std::vector<std::optional<int128_t>> rows;
    for (const auto& batch : output.batches) {
      if (batch->childrenSize() != 1) {
        ADD_FAILURE() << "CASE decimal oracle must retain one actual output column";
        return {};
      }
      const auto& child = batch->childAt(0);
      if (*child->type() != *DECIMAL(precision, scale)) {
        ADD_FAILURE() << "CASE/SUM decimal result precision or scale drifted";
        return {};
      }
      DecodedVector decoded(*child);
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        rows.push_back(decoded.isNullAt(row) ? std::optional<int128_t>{}
            : std::optional<int128_t>{decoded.valueAt<int128_t>(row)});
      }
    }
    return rows;
  }

  static substrait::Plan substringPlan() {
    substrait::Plan plan;
    declare(plan, 0, "substring");
    auto* root = plan.add_relations()->mutable_root();
    root->add_names("prefix");
    auto* project = root->mutable_input()->mutable_project();
    auto* read = project->mutable_input()->mutable_read();
    auto* schema = read->mutable_base_schema();
    for (const auto* name : {"text", "position", "length"}) schema->add_names(name);
    schema->mutable_struct_()->add_types()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_i32()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    schema->mutable_struct_()->add_types()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    const std::vector<std::optional<std::string>> texts{
        "abcdef", "80348xxx", "A\xE4\xB8\xAD\xF0\x9F\x98\x80" "Z", std::nullopt, "abcdef", "abc"};
    const std::vector<std::optional<int32_t>> positions{1, 1, 2, 1, std::nullopt, -2};
    const std::vector<int64_t> lengths{2, 5, 2, 2, 2, 2};
    for (size_t index = 0; index < texts.size(); ++index) {
      auto* row = read->mutable_virtual_table()->add_expressions();
      auto* text = row->add_fields()->mutable_literal();
      if (texts[index]) { text->set_string(*texts[index]); text->set_nullable(true); }
      else *text->mutable_null() = schema->struct_().types(0);
      auto* position = row->add_fields()->mutable_literal();
      if (positions[index]) { position->set_i32(*positions[index]); position->set_nullable(true); }
      else *position->mutable_null() = schema->struct_().types(1);
      row->add_fields()->mutable_literal()->set_i64(lengths[index]);
    }
    substrait::Expression position;
    auto* cast = position.mutable_cast();
    *cast->mutable_input() = field(1);
    cast->mutable_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    cast->set_failure_behavior(substrait::Expression::Cast::FAILURE_BEHAVIOR_THROW_EXCEPTION);
    auto* output = project->add_expressions()->mutable_scalar_function();
    output->set_function_reference(0);
    output->mutable_output_type()->mutable_string()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    for (const auto& argument : {field(0), position, field(2)}) *output->add_arguments()->mutable_value() = argument;
    project->mutable_common()->mutable_emit()->add_output_mapping(3);
    return plan;
  }
};

TEST_F(TpcSemanticExpressionExecutionTest, TpcCaseUsesThirtyOneDigitProductAndPreservesNullAndElseValues) {
  const auto output = run(casePlan({{true, 10000, 5}, {false, 10000, 5},
      {true, std::nullopt, 5}, {std::nullopt, 10000, 5}, {true, 10000, std::nullopt}}));
  // 100.00 * (1.00 - 0.05) = 95.0000; decimals are compared unscaled.
  EXPECT_EQ(decimalRows(output, 31, 4), (std::vector<std::optional<int128_t>>{
      int128_t{950000}, int128_t{0}, std::nullopt, int128_t{0}, std::nullopt}));
}

TEST_F(TpcSemanticExpressionExecutionTest, TpcCaseSumUsesThirtyEightDigitsAndPreservesEmptyAndAllNull) {
  EXPECT_EQ(decimalRows(run(casePlan({{true, 10000, 5}, {false, 10000, 5},
      {true, std::nullopt, 5}}, true)), 38, 4),
      (std::vector<std::optional<int128_t>>{int128_t{950000}}));
  EXPECT_EQ(decimalRows(run(casePlan({{true, std::nullopt, 5}}, true)), 38, 4),
      (std::vector<std::optional<int128_t>>{std::nullopt}));
  EXPECT_EQ(decimalRows(run(casePlan({}, true)), 38, 4),
      (std::vector<std::optional<int128_t>>{std::nullopt}));
}

TEST_F(TpcSemanticExpressionExecutionTest, MisdeclaredTpcProductAndSumReturnTypesAreRejectedPrecisely) {
  rejectContract(casePlan({{true, 10000, 5}}, false, 19), "scalar output contract mismatch: multiply");
  rejectContract(casePlan({{true, 10000, 5}}, true, 31, 19), "aggregate output contract mismatch: sum");
}

TEST_F(TpcSemanticExpressionExecutionTest, CanonicalSubstringExplicitBigintCastPreservesUtf8AndNullValues) {
  const auto plan = substringPlan();
  const auto canonical = pangrova::test::canonicalTestPlan(plan);
  ASSERT_EQ(canonical.extensions_size(), 1);
  EXPECT_EQ(canonical.extensions(0).extension_function().name(), "substring");
  const auto& function = canonical.relations(0).root().input().project().expressions(0).scalar_function();
  ASSERT_TRUE(function.arguments(1).value().has_cast());
  EXPECT_TRUE(function.arguments(1).value().cast().type().has_i64());
  const auto output = run(plan);
  std::vector<std::optional<std::string>> values;
  for (const auto& batch : output.batches) {
    ASSERT_EQ(batch->childrenSize(), 1);
    EXPECT_EQ(*batch->childAt(0)->type(), *VARCHAR());
    DecodedVector decoded(*batch->childAt(0));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      values.push_back(decoded.isNullAt(row) ? std::optional<std::string>{}
          : std::optional<std::string>{decoded.valueAt<StringView>(row).str()});
    }
  }
  EXPECT_EQ(values, (std::vector<std::optional<std::string>>{
      "ab", "80348", "\xE4\xB8\xAD\xF0\x9F\x98\x80", std::nullopt, std::nullopt, "bc"}));
}

TEST_F(TpcSemanticExpressionExecutionTest, MisdeclaredSubstringReturnTypeCannotSelectAnotherOverload) {
  auto plan = substringPlan();
  auto* output = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()
      ->mutable_expressions(0)->mutable_scalar_function()->mutable_output_type();
  output->Clear();
  output->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  rejectContract(plan, "scalar output contract mismatch: substr");
}
}  // namespace
}  // namespace pangrova::velox::test
