#include <gtest/gtest.h>

#include <optional>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "CanonicalTaskExecution.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Pair = std::pair<int64_t, int32_t>;
enum class Input { Values, NullPayload, NullKeys, Empty };

core::PlanNodePtr selection(const std::string& function, Input input, bool partial,
    const FunctionRegistryOwner& registry, facebook::velox::memory::MemoryPool* pool) {
  auto valueType = ROW({"source_id", "spec_id"}, {BIGINT(), INTEGER()});
  auto type = ROW({"value", "key"}, {valueType, BIGINT()});
  const auto count = input == Input::Empty ? 0 : 3;
  auto rows = std::dynamic_pointer_cast<RowVector>(BaseVector::create(type, count, pool));
  auto values = std::dynamic_pointer_cast<RowVector>(rows->childAt(0));
  for (vector_size_t row = 0; row < count; ++row) {
    values->childAt(0)->asFlatVector<int64_t>()->set(row, (row + 1) * 101);
    values->childAt(1)->asFlatVector<int32_t>()->set(row, row == 0 ? 9 : row == 1 ? 3 : 7);
    rows->childAt(1)->asFlatVector<int64_t>()->set(row, row + 1);
    if (row == 2 || input == Input::NullKeys) rows->childAt(1)->setNull(row, true);
  }
  if (input == Input::NullPayload) values->setNull(function == "max_by" ? 1 : 0, true);
  auto source = std::make_shared<core::ValuesNode>("selection-input", std::vector<RowVectorPtr>{rows});
  auto binding = registry->bindAggregate(function, {valueType, BIGINT()});
  if (!binding) throw std::runtime_error("Actual selection aggregate registration is missing");
  auto outputType = partial ? binding->intermediateType : binding->resultType;
  core::AggregationNode::Aggregate aggregate;
  aggregate.rawInputTypes = {valueType, BIGINT()};
  aggregate.call = std::make_shared<core::CallTypedExpr>(outputType,
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(valueType, "value"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "key")}, function);
  auto node = std::make_shared<core::AggregationNode>("selection-initial",
      partial ? core::AggregationNode::Step::kPartial : core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"chosen"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, source);
  if (!partial) return node;
  aggregate.call = std::make_shared<core::CallTypedExpr>(binding->resultType,
      std::vector<core::TypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(outputType, "chosen")},
      function);
  return std::make_shared<core::AggregationNode>("selection-final", core::AggregationNode::Step::kFinal,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"chosen"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, core::LocalPartitionNode::gather("selection-gather", {node}));
}

std::optional<Pair> execute(const core::PlanNodePtr& node, const FunctionRegistryOwner& registry) {
  const auto output = executeCanonicalTask(node, registry);
  EXPECT_EQ(output.batches.size(), 1);
  if (output.batches.size() != 1 || output.batches[0]->size() != 1) {
    ADD_FAILURE() << "Global selection aggregate must emit one row";
    return std::nullopt;
  }
  DecodedVector value(*output.batches[0]->childAt(0));
  if (value.isNullAt(0)) return std::nullopt;
  auto* row = value.base()->as<RowVector>();
  DecodedVector id(*row->childAt(0)), spec(*row->childAt(1));
  return Pair{id.valueAt<int64_t>(value.index(0)), spec.valueAt<int32_t>(value.index(0))};
}

TEST(SelectionAggregateExecutionTest, WholeRowsSurviveSingleAndPartialFinalAggregation) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto pool = pangrova::test::testLeafPool("selection-values");
  for (const auto& function : {"max_by", "min_by"}) {
    for (bool partial : {false, true}) {
      EXPECT_EQ(execute(selection(function, Input::Values, partial, registry, pool.get()), registry),
          std::optional<Pair>(function == std::string("max_by") ? Pair{202, 3} : Pair{101, 9}));
    }
  }
}

TEST(SelectionAggregateExecutionTest, NullValuesNullKeysAndEmptyInputsRemainNull) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto pool = pangrova::test::testLeafPool("selection-nulls");
  for (const auto& function : {"max_by", "min_by"}) {
    for (bool partial : {false, true}) {
      for (Input input : {Input::NullPayload, Input::NullKeys, Input::Empty}) {
        EXPECT_EQ(execute(selection(function, input, partial, registry, pool.get()), registry), std::nullopt);
      }
    }
  }
}
}  // namespace
}  // namespace pangrova::velox::test
