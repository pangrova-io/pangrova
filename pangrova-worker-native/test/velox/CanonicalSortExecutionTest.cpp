// Actual component Task values/topology, not a scan-parallel or SQL E2E claim.
#include <gtest/gtest.h>
#include "../CanonicalTestPlan.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "CanonicalOutputAssertions.h"
#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Rows = std::vector<std::vector<Cell>>;
const Rows kRows{{1, 3, 10}, {std::nullopt, 2, 11}, {-1, 2, 12}, {2, 1, 13},
                 {std::nullopt, 1, 14}, {1, 1, 15}, {1, std::nullopt, 16}};

void field(substrait::Expression* value, int ordinal) {
  auto* reference = value->mutable_selection();
  reference->mutable_root_reference();
  reference->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
}

substrait::Plan sortPlan(substrait::SortField::SortDirection direction, bool fetch) {
  substrait::Plan plan;
  auto* function = plan.add_extensions()->mutable_extension_function();
  function->set_function_anchor(1);
  function->set_name("add:i64_i64");
  auto* root = plan.add_relations()->mutable_root();
  for (const auto* name : {"x", "y", "id"}) root->add_names(name);
  auto* input = root->mutable_input();
  if (fetch) {
    auto* limit = input->mutable_fetch();
    limit->mutable_count_expr()->mutable_literal()->set_i64(3);
    input = limit->mutable_input();
  }
  auto* sort = input->mutable_sort();
  auto* primary = sort->add_sorts();
  primary->set_direction(direction);
  auto* computed = primary->mutable_expr()->mutable_scalar_function();
  computed->set_function_reference(1);
  computed->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  field(computed->add_arguments()->mutable_value(), 0);
  field(computed->add_arguments()->mutable_value(), 1);
  auto* secondary = sort->add_sorts();
  secondary->set_direction(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST);
  field(secondary->mutable_expr(), 2);
  auto* read = sort->mutable_input()->mutable_read();
  for (const auto* name : {"x", "y", "id"}) {
    read->mutable_base_schema()->add_names(name);
    read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
        ->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  }
  for (const auto& row : kRows) {
    auto* record = read->mutable_virtual_table()->add_expressions();
    for (const auto& cell : row) {
      auto* literal = record->add_fields()->mutable_literal();
      if (cell) literal->set_i64(*cell);
      else literal->mutable_null()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    }
  }
  return plan;
}

Rows expected(bool ascending, bool nullFirst, bool fetch) {
  auto rows = kRows;
  std::sort(rows.begin(), rows.end(), [=](const auto& left, const auto& right) {
    const Cell a = left[0] && left[1] ? Cell{*left[0] + *left[1]} : Cell{};
    const Cell b = right[0] && right[1] ? Cell{*right[0] + *right[1]} : Cell{};
    if (a == b) return left[2] < right[2];
    if (!a || !b) return !a == nullFirst;
    return ascending ? *a < *b : *a > *b;
  });
  if (fetch) rows.resize(3);
  return rows;
}

Rows execute(const core::PlanNodePtr& node, const FunctionRegistryOwner& registry) {
  const auto result = executeCanonicalTask(node, registry);
  Rows rows;
  for (const auto& batch : result.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<Cell> values;
      for (const auto& child : batch->children()) {
        DecodedVector decoded(*child);
        values.push_back(decoded.isNullAt(row) ? Cell{} : Cell{decoded.valueAt<int64_t>(row)});
      }
      rows.push_back(std::move(values));
    }
  }
  return rows;
}

size_t countNodes(const core::PlanNodePtr& node, const std::string& name) {
  size_t count = node->name() == name ? 1 : 0;
  for (const auto& source : node->sources()) count += countNodes(source, name);
  return count;
}

class CanonicalSortExecutionTest : public ::testing::TestWithParam<substrait::SortField::SortDirection> {
 protected:
  void SetUp() override { pangrova::test::testMemoryManager(); }
};

TEST_P(CanonicalSortExecutionTest, ComputedKeysPreserveEveryValueNullDirectionAndTopNSchema) {
  const auto direction = GetParam();
  const bool ascending = direction == substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST ||
      direction == substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST;
  const bool nullFirst = direction == substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST ||
      direction == substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST;
  for (bool fetch : {false, true}) {
    const auto bytes = pangrova::test::canonicalTestPlan(sortPlan(direction, fetch)).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    const auto node = converter.convert({bytes.begin(), bytes.end()}, {});
    ASSERT_EQ(node->outputType()->size(), 3);
    expectCanonicalOutputField(converter, node->outputType(), 0, "x");
    expectCanonicalOutputField(converter, node->outputType(), 1, "y");
    expectCanonicalOutputField(converter, node->outputType(), 2, "id");
    const auto restored = std::dynamic_pointer_cast<const core::ProjectNode>(node);
    ASSERT_NE(restored, nullptr);
    ASSERT_EQ(restored->projections().size(), 3);
    core::PlanNodePtr prepared;
    if (fetch) {
      const auto final = std::dynamic_pointer_cast<const core::TopNNode>(restored->sources().front());
      ASSERT_NE(final, nullptr);
      EXPECT_FALSE(final->isPartial());
      EXPECT_TRUE(final->requiresSingleThread());
      EXPECT_EQ(final->count(), 3);
      const auto gather = std::dynamic_pointer_cast<const core::LocalPartitionNode>(final->sources().front());
      ASSERT_NE(gather, nullptr);
      EXPECT_EQ(gather->type(), core::LocalPartitionNode::Type::kGather);
      ASSERT_EQ(gather->sources().size(), 1);
      const auto partial = std::dynamic_pointer_cast<const core::TopNNode>(gather->sources().front());
      ASSERT_NE(partial, nullptr);
      EXPECT_TRUE(partial->isPartial());
      EXPECT_FALSE(partial->requiresSingleThread());
      EXPECT_EQ(partial->count(), 3);
      prepared = partial->sources().front();
    } else {
      const auto order = std::dynamic_pointer_cast<const core::OrderByNode>(restored->sources().front());
      ASSERT_NE(order, nullptr);
      ASSERT_EQ(order->sortingKeys().size(), 2);
      EXPECT_EQ(order->sortingKeys()[0]->name(), physicalFieldAlias(3));
      EXPECT_EQ(order->sortingKeys()[1]->name(), physicalFieldAlias(2));
      EXPECT_EQ(order->sortingOrders()[0], core::SortOrder(ascending, nullFirst));
      prepared = order->sources().front();
    }
    const auto preparation = std::dynamic_pointer_cast<const core::ProjectNode>(prepared);
    ASSERT_NE(preparation, nullptr);
    ASSERT_EQ(preparation->projections().size(), 4);
    const auto computed = std::dynamic_pointer_cast<const core::CallTypedExpr>(preparation->projections().at(3));
    ASSERT_NE(computed, nullptr);
    // Canonical Substrait arithmetic.add is bound to the registered Velox plus.
    EXPECT_EQ(computed->name(), "plus");
    for (size_t ordinal = 0; ordinal < 3; ++ordinal) {
      const auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(restored->projections().at(ordinal));
      ASSERT_NE(field, nullptr);
      EXPECT_TRUE(field->inputs().empty());
      EXPECT_EQ(field->name(), prepared->outputType()->nameOf(ordinal));
      EXPECT_TRUE(field->type()->equivalent(*prepared->outputType()->childAt(ordinal)));
    }
    EXPECT_EQ(execute(node, converter_registry), expected(ascending, nullFirst, fetch));
  }
}

INSTANTIATE_TEST_SUITE_P(FourDirections, CanonicalSortExecutionTest, ::testing::Values(
    substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST,
    substrait::SortField::SORT_DIRECTION_ASC_NULLS_LAST,
    substrait::SortField::SORT_DIRECTION_DESC_NULLS_FIRST,
    substrait::SortField::SORT_DIRECTION_DESC_NULLS_LAST));

TEST(CanonicalSortValidationTest, MissingUnknownClusteredAndCustomComparatorFailClosed) {
  pangrova::test::testMemoryManager();
  for (int invalid = 0; invalid < 5; ++invalid) {
    auto plan = sortPlan(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST, false);
    auto* sort = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_sort()->mutable_sorts(0);
    if (invalid == 0) sort->clear_direction();
    if (invalid == 1) sort->set_direction(substrait::SortField::SORT_DIRECTION_UNSPECIFIED);
    if (invalid == 2) sort->set_direction(substrait::SortField::SORT_DIRECTION_CLUSTERED);
    if (invalid == 3) sort->set_direction(static_cast<substrait::SortField::SortDirection>(999));
    if (invalid == 4) sort->set_comparison_function_reference(1);
    const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    try {
      (void)converter.convert({bytes.begin(), bytes.end()}, {});
      FAIL() << "Unsupported sort kind was silently accepted: " << invalid;
    } catch (const std::runtime_error& error) {
      EXPECT_NE(std::string(error.what()).find("Sort"), std::string::npos);
    }
  }
}

TEST(CanonicalSortValidationTest, MissingExpressionAndEmptyKeyListAreNotSilentlyIgnored) {
  pangrova::test::testMemoryManager();
  for (bool empty : {false, true}) {
    auto plan = sortPlan(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST, false);
    auto* sort = plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_sort();
    if (empty) sort->clear_sorts();
    else sort->mutable_sorts(0)->clear_expr();
    const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    const auto converter_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    VeloxPlanConverter converter(converter_registry);
    EXPECT_THROW(converter.convert({bytes.begin(), bytes.end()}, {}), std::runtime_error);
  }
}

TEST(CanonicalSortLimitExecutionTest, ComputedOutputProjectionCannotBeTreatedAsOrderingIdentity) {
  pangrova::test::testMemoryManager();
  auto proto = sortPlan(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST, true);
  auto* fetch = proto.mutable_relations(0)->mutable_root()->mutable_input()->mutable_fetch();
  const auto original = fetch->input();
  auto* project = fetch->mutable_input()->mutable_project();
  *project->mutable_input() = original;
  auto* changed = project->add_expressions()->mutable_scalar_function();
  changed->set_function_reference(1);
  changed->mutable_output_type()->mutable_i64()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  field(changed->add_arguments()->mutable_value(), 1);
  changed->add_arguments()->mutable_value()->mutable_literal()->set_i64(1);
  for (int ordinal : {0, 3, 2}) project->mutable_common()->mutable_emit()->add_output_mapping(ordinal);
  const auto bytes = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto node = converter.convert({bytes.begin(), bytes.end()}, {});
  EXPECT_EQ(countNodes(node, "TopN"), 0);
  EXPECT_EQ(countNodes(node, "Limit"), 2);
  auto rows = expected(true, true, true);
  for (auto& row : rows) if (row[1]) ++*row[1];
  EXPECT_EQ(execute(node, converter_registry), rows);
}

TEST(CanonicalSortLimitExecutionTest, LimitHasARealGatherAndAppliesOffsetOnlyOnce) {
  pangrova::test::testMemoryManager();
  auto proto = sortPlan(substrait::SortField::SORT_DIRECTION_ASC_NULLS_FIRST, true);
  auto* fetch = proto.mutable_relations(0)->mutable_root()->mutable_input()->mutable_fetch();
  fetch->mutable_offset_expr()->mutable_literal()->set_i64(2);
  const auto bytes = pangrova::test::canonicalTestPlan(proto).SerializeAsString();
  const auto converter_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  VeloxPlanConverter converter(converter_registry);
  const auto node = converter.convert({bytes.begin(), bytes.end()}, {});
  const auto final = std::dynamic_pointer_cast<const core::LimitNode>(node);
  ASSERT_NE(final, nullptr);
  EXPECT_EQ(final->offset(), 2);
  EXPECT_EQ(final->count(), 3);
  EXPECT_FALSE(final->isPartial());
  const auto gather = std::dynamic_pointer_cast<const core::LocalPartitionNode>(final->sources().front());
  ASSERT_NE(gather, nullptr);
  EXPECT_EQ(gather->type(), core::LocalPartitionNode::Type::kGather);
  const auto partial = std::dynamic_pointer_cast<const core::LimitNode>(gather->sources().front());
  ASSERT_NE(partial, nullptr);
  EXPECT_EQ(partial->offset(), 0);
  EXPECT_EQ(partial->count(), 5);
  EXPECT_TRUE(partial->isPartial());
  auto rows = expected(true, true, false);
  rows = Rows(rows.begin() + 2, rows.begin() + 5);
  EXPECT_EQ(execute(node, converter_registry), rows);
}
}  // namespace
}  // namespace pangrova::velox::test
