// Canonical proto -> production converter -> actual Task DISTINCT and FILTER.
#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <set>

#include "../TestMemoryHelper.h"
#include "substrait/plan.pb.h"
#include "CanonicalTaskExecution.h"
#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Rows = std::vector<std::vector<Cell>>;
enum class ValueDistribution { kUnique, kManyDuplicates, kAllSame };
enum class FilterMode { kNone, kColumn, kFalse };

struct MarkDistinctParam {
  std::string name;
  int distinct_col_count;
  int grouping_key_count;
  int input_size;
  ValueDistribution distribution;
};

substrait::Expression field(int ordinal) {
  substrait::Expression result;
  auto* reference = result.mutable_selection();
  reference->mutable_root_reference();
  reference->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
  return result;
}

Rows inputRows(const MarkDistinctParam& parameter, int distinctCount) {
  Rows rows;
  for (int row = 0; row < parameter.input_size; ++row) {
    std::vector<Cell> values;
    for (int key = 0; key < parameter.grouping_key_count; ++key) {
      values.push_back(key == 0 ? row % 3 : (row / 3) % 2);
    }
    for (int column = 0; column < distinctCount; ++column) {
      int64_t value = 7;
      if (parameter.distribution == ValueDistribution::kUnique) value = row + column;
      if (parameter.distribution == ValueDistribution::kManyDuplicates) value = (row / 2 + column) % 5;
      values.push_back(row % 11 == 0 ? Cell{} : Cell{value});
    }
    values.push_back(row % 9 == 0 ? Cell{} : Cell{row % 4 == 0 ? 0 : 1});
    rows.push_back(std::move(values));
  }
  return rows;
}

Rows expectedRows(const MarkDistinctParam& parameter, const Rows& input,
                  int distinctCount, FilterMode mode) {
  std::map<std::vector<Cell>, std::vector<std::set<int64_t>>> groups;
  for (const auto& row : input) {
    std::vector<Cell> key(row.begin(), row.begin() + parameter.grouping_key_count);
    auto [group, inserted] = groups.try_emplace(key, distinctCount);
    (void)inserted;
    if (mode == FilterMode::kFalse ||
        (mode == FilterMode::kColumn && (!row.back() || *row.back() == 0))) continue;
    for (int column = 0; column < distinctCount; ++column) {
      const auto& value = row[parameter.grouping_key_count + column];
      if (value) group->second[column].insert(*value);
    }
  }
  Rows result;
  for (const auto& [key, values] : groups) {
    auto row = key;
    for (const auto& distinct : values) row.push_back(static_cast<int64_t>(distinct.size()));
    result.push_back(std::move(row));
  }
  return result;
}

substrait::Plan aggregatePlan(const MarkDistinctParam& parameter, const Rows& rows,
                             int distinctCount, FilterMode mode) {
  substrait::Plan plan;
  auto* function = plan.add_extensions()->mutable_extension_function();
  function->set_function_anchor(1);
  function->set_name("count:i64");
  auto* root = plan.add_relations()->mutable_root();
  for (int i = 0; i < parameter.grouping_key_count + distinctCount; ++i) root->add_names("result");
  auto* aggregate = root->mutable_input()->mutable_aggregate();
  auto* read = aggregate->mutable_input()->mutable_read();
  auto* schema = read->mutable_base_schema();
  schema->mutable_struct_();
  const int arity = parameter.grouping_key_count + distinctCount + 1;
  for (int column = 0; column < arity; ++column) {
    schema->add_names("input");
    auto* type = schema->mutable_struct_()->add_types();
    if (column == arity - 1) type->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    else type->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  }
  read->mutable_virtual_table();
  for (const auto& row : rows) {
    auto* record = read->mutable_virtual_table()->add_expressions();
    for (int column = 0; column < arity; ++column) {
      auto* literal = record->add_fields()->mutable_literal();
      if (!row[column]) literal->mutable_null()->CopyFrom(schema->struct_().types(column));
      else if (column == arity - 1) literal->set_boolean(*row[column] != 0);
      else literal->set_i64(*row[column]);
    }
  }
  auto* grouping = aggregate->add_groupings();
  for (int key = 0; key < parameter.grouping_key_count; ++key) {
    aggregate->add_grouping_expressions()->CopyFrom(field(key));
    grouping->add_expression_references(key);
  }
  for (int column = 0; column < distinctCount; ++column) {
    auto* measure = aggregate->add_measures();
    auto* call = measure->mutable_measure();
    call->set_function_reference(1);
    call->set_phase(substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT);
    call->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);
    call->add_arguments()->mutable_value()->CopyFrom(field(parameter.grouping_key_count + column));
    call->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
    if (mode == FilterMode::kColumn) measure->mutable_filter()->CopyFrom(field(arity - 1));
    if (mode == FilterMode::kFalse) measure->mutable_filter()->mutable_literal()->set_boolean(false);
  }
  return plan;
}

void collectNodes(const core::PlanNodePtr& node, std::string_view name,
                  std::vector<core::PlanNodePtr>& result) {
  if (node->name() == name) result.push_back(node);
  for (const auto& child : node->sources()) collectNodes(child, name, result);
}

class MarkDistinctCorrectnessTest : public ::testing::TestWithParam<MarkDistinctParam> {
 protected:
  void SetUp() override { pangrova::test::testMemoryManager(); }

  void verify(int distinctCount, FilterMode mode) {
    const auto& parameter = GetParam();
    const auto source = inputRows(parameter, distinctCount);
    ASSERT_EQ(source.size(), parameter.input_size);
    const auto encoded = pangrova::test::canonicalTestPlan(aggregatePlan(parameter, source, distinctCount, mode)).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    const auto node = converter.convert({encoded.begin(), encoded.end()}, {});
    ASSERT_NE(node, nullptr);
    const auto arity = parameter.grouping_key_count + distinctCount;
    ASSERT_EQ(node->outputType()->size(), arity);
    EXPECT_EQ(converter.resultColumnLabels(), std::vector<std::string>(arity, "result"));
    for (int i = 0; i < arity; ++i) {
      EXPECT_EQ(node->outputType()->nameOf(i), physicalFieldAlias(i));
      EXPECT_TRUE(node->outputType()->childAt(i)->isBigint());
    }
    std::vector<core::PlanNodePtr> aggregationNodes, markers, values;
    collectNodes(node, "Aggregation", aggregationNodes);
    collectNodes(node, "MarkDistinct", markers);
    collectNodes(node, "Values", values);
    ASSERT_EQ(aggregationNodes.size(), 1);
    EXPECT_TRUE(markers.empty());
    ASSERT_EQ(values.size(), 1);
    const auto* aggregate = dynamic_cast<const core::AggregationNode*>(aggregationNodes.front().get());
    ASSERT_NE(aggregate, nullptr);
    EXPECT_EQ(aggregate->step(), core::AggregationNode::Step::kSingle);
    ASSERT_EQ(aggregate->aggregates().size(), distinctCount);
    EXPECT_EQ(aggregate->groupingKeys().size(), parameter.grouping_key_count);
    for (const auto& measure : aggregate->aggregates()) {
      EXPECT_TRUE(measure.distinct);
      ASSERT_EQ(measure.call->inputs().size(), 1);
      EXPECT_EQ(bool(measure.mask), mode != FilterMode::kNone);
      if (measure.mask) EXPECT_TRUE(measure.mask->type()->isBoolean());
    }
    auto actual = execute(node, converter_registry);
    EXPECT_EQ(actual, expectedRows(parameter, source, distinctCount, mode));
  }

  Rows execute(const core::PlanNodePtr& node,
      const std::shared_ptr<const exec::FunctionRegistrySnapshot>& registry) {
    const auto execution = executeCanonicalTask(node, registry);
    Rows result;
    for (const auto& batch : execution.batches) {
      for (vector_size_t row = 0; row < batch->size(); ++row) {
        std::vector<Cell> cells;
        for (const auto& child : batch->children()) {
          DecodedVector decoded(*child);
          cells.push_back(decoded.isNullAt(row) ? Cell{} : Cell{decoded.valueAt<int64_t>(row)});
        }
        result.push_back(std::move(cells));
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }
};

TEST_P(MarkDistinctCorrectnessTest, DistinctMeasuresShareOneActualAggregation) {
  verify(GetParam().distinct_col_count, FilterMode::kNone);
}
TEST_P(MarkDistinctCorrectnessTest, AggregationConsumesActualNullableInput) {
  verify(GetParam().distinct_col_count, FilterMode::kColumn);
}
TEST_P(MarkDistinctCorrectnessTest, MasksAreBooleanAndAppliedBeforeDistinct) {
  verify(GetParam().distinct_col_count, FilterMode::kColumn);
}
TEST_P(MarkDistinctCorrectnessTest, FinalOutputHasOnlyKeysAndCounts) {
  verify(GetParam().distinct_col_count, FilterMode::kNone);
}
TEST_P(MarkDistinctCorrectnessTest, ConstantFalseMaskRetainsGroupsWithZeroCounts) {
  verify(GetParam().distinct_col_count, FilterMode::kFalse);
}
TEST_P(MarkDistinctCorrectnessTest, SingleDistinctUsesSameNativeOperator) {
  verify(1, FilterMode::kColumn);
}

std::vector<MarkDistinctParam> generateParams() {
  std::vector<MarkDistinctParam> result;
  for (int distinct : {2, 3}) {
    for (int grouping : {0, 1, 2}) {
      for (int size : {10, 100, 500}) {
        for (auto distribution : {ValueDistribution::kUnique,
                 ValueDistribution::kManyDuplicates, ValueDistribution::kAllSame}) {
          const auto suffix = distribution == ValueDistribution::kUnique ? "Unique" :
              distribution == ValueDistribution::kManyDuplicates ? "ManyDuplicates" : "AllSame";
          result.push_back({"DC" + std::to_string(distinct) + "_GK" + std::to_string(grouping) +
              "_N" + std::to_string(size) + "_" + suffix, distinct, grouping, size, distribution});
        }
      }
    }
  }
  return result;
}

INSTANTIATE_TEST_SUITE_P(MarkDistinctCorrectness, MarkDistinctCorrectnessTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<MarkDistinctParam>& info) { return info.param.name; });
}  // namespace
}  // namespace pangrova::velox::test
