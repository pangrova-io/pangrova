// Actual SDK operator/serde value oracles; not public SQL or distributed E2E.
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "CanonicalTaskExecution.h"
#include "capabilities/capability/RegisteredFunctionBinding.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
using Cell = std::optional<int64_t>;
using Cells = std::vector<Cell>;
using Rows = std::vector<std::vector<Cell>>;
using VeloxMemoryPool = facebook::velox::memory::MemoryPool;

core::PlanNodePtr input(const std::string& name, const std::vector<Cells>& batches,
                        VeloxMemoryPool* pool) {
  std::vector<RowVectorPtr> vectors;
  for (const auto& batch : batches) {
    auto column = BaseVector::create<FlatVector<int64_t>>(BIGINT(), batch.size(), pool);
    for (size_t row = 0; row < batch.size(); ++row) {
      if (batch[row]) column->set(row, *batch[row]);
      else column->setNull(row, true);
    }
    vectors.push_back(std::make_shared<RowVector>(pool, ROW({name}, {BIGINT()}), nullptr,
        batch.size(), std::vector<VectorPtr>{column}));
  }
  return std::make_shared<core::ValuesNode>(name + "-values", std::move(vectors));
}

core::TypedExprPtr makeComparison(const FunctionRegistryOwner& registry, const std::string& name) {
  auto probe = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "probe");
  auto build = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "build");
  return capability::bindRegisteredScalar(*registry, name, BOOLEAN(), {probe, build});
}

Rows collectRows(const core::PlanNodePtr& node, const FunctionRegistryOwner& registry) {
  const auto result = executeCanonicalTask(node, registry);
  Rows rows;
  for (const auto& batch : result.batches) {
    EXPECT_EQ(batch->childrenSize(), 2);
    DecodedVector probe(*batch->childAt(0)), marker(*batch->childAt(1));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      rows.push_back({probe.isNullAt(row) ? Cell{} : Cell{probe.valueAt<int64_t>(row)},
          marker.isNullAt(row) ? Cell{} : Cell{int64_t{marker.valueAt<bool>(row)}}});
    }
  }
  std::sort(rows.begin(), rows.end());
  return rows;
}

class NullAwareMarkExecutionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("actual-null-aware-marker");
    registry_ = exec::FunctionRegistrySnapshot::capture();
  }

  core::TypedExprPtr comparison(const std::string& name) {
    return makeComparison(registry_, name);
  }

  Rows output(const core::PlanNodePtr& node) {
    return collectRows(node, registry_);
  }

  std::shared_ptr<const core::NestedLoopJoinNode> join(
      const std::vector<Cells>& probe, const std::vector<Cells>& build,
      core::TypedExprPtr predicate, bool nullAware = true) {
    return std::make_shared<core::NestedLoopJoinNode>("actual-marker",
        core::JoinType::kLeftSemiProject, std::move(predicate),
        input("probe", probe, pool_.get()), input("build", build, pool_.get()),
        ROW({"probe", "marker"}, {BIGINT(), BOOLEAN()}), nullAware);
  }

  std::shared_ptr<VeloxMemoryPool> pool_;
  FunctionRegistryOwner registry_;
};

TEST_F(NullAwareMarkExecutionTest, NonEquiPredicateRetainsUnknownAndEmptySetIsFalse) {
  const std::vector<Cells> probes{{std::nullopt, 1, 2}};
  EXPECT_EQ(output(join(probes, {{std::nullopt, 1}}, comparison("gt"))),
      (Rows{{std::nullopt, std::nullopt}, {1, std::nullopt}, {2, 1}}));
  EXPECT_EQ(output(join(probes, {{2, 3}}, comparison("gt"))),
      (Rows{{std::nullopt, std::nullopt}, {1, 0}, {2, 0}}));
  EXPECT_EQ(output(join(probes, {{}}, comparison("gt"))),
      (Rows{{std::nullopt, 0}, {1, 0}, {2, 0}}));
}

TEST_F(NullAwareMarkExecutionTest, TrueDominatesUnknownRegardlessOfBuildBatchOrder) {
  const std::vector<Cells> probes{{std::nullopt, 1}, {2}};
  for (const auto& build : {std::vector<Cells>{{std::nullopt}, {0}},
                           std::vector<Cells>{{0}, {std::nullopt}}}) {
    EXPECT_EQ(output(join(probes, build, comparison("gt"))),
        (Rows{{std::nullopt, std::nullopt}, {1, 1}, {2, 1}}));
  }
}

TEST_F(NullAwareMarkExecutionTest, UnknownPersistsAcrossBuildVectorsAndResetsBetweenProbeRows) {
  EXPECT_EQ(output(join({{1, 2}, {std::nullopt}}, {{3}, {std::nullopt}, {1}}, comparison("gt"))),
      (Rows{{std::nullopt, std::nullopt}, {1, std::nullopt}, {2, 1}}));
}

TEST_F(NullAwareMarkExecutionTest, OrdinarySemiMarkerStaysTwoValued) {
  EXPECT_EQ(output(join({{std::nullopt, 1, 2}}, {{std::nullopt, 1}}, comparison("eq"), false)),
      (Rows{{std::nullopt, 0}, {1, 1}, {2, 0}}));
}

TEST_F(NullAwareMarkExecutionTest, NoPredicateAndConstantPredicatesRespectEmptyInputs) {
  const std::vector<Cells> probes{{std::nullopt, 1}};
  auto unknown = std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), variant::null(TypeKind::BOOLEAN));
  auto never = std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), variant(false));
  EXPECT_EQ(output(join(probes, {{2}}, unknown)),
      (Rows{{std::nullopt, std::nullopt}, {1, std::nullopt}}));
  EXPECT_EQ(output(join(probes, {{2}}, never)), (Rows{{std::nullopt, 0}, {1, 0}}));
  EXPECT_EQ(output(join(probes, {{2}}, nullptr)), (Rows{{std::nullopt, 1}, {1, 1}}));
  EXPECT_EQ(output(join(probes, {{}}, nullptr)), (Rows{{std::nullopt, 0}, {1, 0}}));
}

TEST_F(NullAwareMarkExecutionTest, BuilderAndSerdeKeepTheSameActualThreeValuedExecution) {
  Type::registerSerDe();
  core::PlanNode::registerSerDe();
  core::ITypedExpr::registerSerDe();
  auto original = join({{1, 2}}, {{std::nullopt}, {1}}, comparison("eq"));
  auto copied = core::NestedLoopJoinNode::Builder(*original).id("copied-marker").build();
  EXPECT_TRUE(copied->nullAwareMarker());
  const auto serialized = copied->serialize();
  EXPECT_TRUE(serialized["nullAwareMarker"].asBool());
  auto decoded = ISerializable::deserialize<core::PlanNode>(serialized, pool_.get());
  auto restored = std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(decoded);
  ASSERT_NE(restored, nullptr);
  EXPECT_TRUE(restored->nullAwareMarker());
  EXPECT_EQ(output(original), (Rows{{1, 1}, {2, std::nullopt}}));
  EXPECT_EQ(output(copied), output(original));
  EXPECT_EQ(output(restored), output(original));
  auto malformed = serialized;
  malformed.erase("nullAwareMarker");
  EXPECT_THROW(ISerializable::deserialize<core::PlanNode>(malformed, pool_.get()), std::exception);
}

TEST_F(NullAwareMarkExecutionTest, ThreeValuedFlagCannotBeAttachedToAnOrdinaryJoin) {
  EXPECT_THROW(std::make_shared<core::NestedLoopJoinNode>("invalid-null-marker",
      core::JoinType::kInner, comparison("eq"), input("probe", {{1}}, pool_.get()),
      input("build", {{1}}, pool_.get()), ROW({"probe", "build"}, {BIGINT(), BIGINT()}), true),
      VeloxUserError);
}
}  // namespace
}  // namespace pangrova::velox::test
