// Actual Vector/Task/Arrow lifetime oracles; not public SQL or distributed E2E.
#include <gtest/gtest.h>

#include "CanonicalTaskExecution.h"
#include "substrait/plan.pb.h"
#include "velox/plan/VeloxPlanConverter.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

struct ArrowExport {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ArrowExport() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

core::PlanNodePtr booleanValues() {
  substrait::Plan plan;
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("predicate");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("predicate");
  auto* type = read->mutable_base_schema()->mutable_struct_()->add_types();
  type->mutable_bool_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  for (int row = 0; row < 10; ++row) {
    auto* literal = read->mutable_virtual_table()->add_expressions()->add_fields()->mutable_literal();
    if (row % 3 == 0) literal->mutable_null()->CopyFrom(*type);
    else {
      literal->set_boolean(row % 2 == 0);
      literal->set_nullable(true);
    }
  }
  const auto bytes = plan.SerializeAsString();
  VeloxPlanConverter converter;
  return converter.convert({bytes.begin(), bytes.end()}, {});
}

const core::ValuesNode* valuesNode(const core::PlanNodePtr& node) {
  if (auto* values = dynamic_cast<const core::ValuesNode*>(node.get())) return values;
  for (const auto& source : node->sources()) {
    if (auto* values = valuesNode(source)) return values;
  }
  return nullptr;
}

VectorPtr integers(memory::MemoryPool* pool) {
  auto vector = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 2, pool);
  vector->set(0, 11);
  vector->set(1, 23);
  return vector;
}

TEST(CanonicalVectorPoolOwnershipTest, ConverterTaskProjectionAndUnalignedBooleanSliceOwnThePool) {
  auto runtime = pangrova::test::testLeafPool("vector-owner-runtime");
  auto node = booleanValues(); // The production converter is already destroyed.
  auto* values = valuesNode(node);
  ASSERT_NE(values, nullptr);
  std::weak_ptr<memory::MemoryPool> weak = values->values().front()->pool()->shared_from_this();
  EXPECT_TRUE(values->values().front()->ownsMemoryPool());
  auto output = executeCanonicalTask(node);
  ASSERT_EQ(output.batches.size(), 1);
  auto child = output.batches.front()->childAt(0);
  auto slice = child->slice(1, 8); // Both boolean values and nulls allocate shifted bitmaps.
  EXPECT_TRUE(slice->ownsMemoryPool());
  node.reset();
  output.batches.clear();
  output.task.reset();
  child.reset();
  EXPECT_FALSE(weak.expired());
  for (int row = 0; row < 8; ++row) {
    EXPECT_EQ(slice->isNullAt(row), (row + 1) % 3 == 0);
    if (!slice->isNullAt(row)) EXPECT_EQ(slice->asFlatVector<bool>()->valueAt(row), (row + 1) % 2 == 0);
  }
  slice.reset();
  EXPECT_TRUE(weak.expired());
}

TEST(CanonicalVectorPoolOwnershipTest, ArrayElementsAndMapChildrenEscapeTheirParentTask) {
  auto pool = pangrova::test::testLeafPool("nested-vector-escape");
  std::weak_ptr<memory::MemoryPool> weak = pool;
  auto array = BaseVector::create<ArrayVector>(ARRAY(BIGINT()), 1, pool.get());
  array->setElements(integers(pool.get()));
  array->setOffsetAndSize(0, 0, 2);
  auto map = BaseVector::create<MapVector>(MAP(BIGINT(), BIGINT()), 1, pool.get());
  map->setKeysAndValues(integers(pool.get()), integers(pool.get()));
  map->setOffsetAndSize(0, 0, 2);
  auto row = std::make_shared<RowVector>(pool.get(), ROW({"array", "map"}, {array->type(), map->type()}),
      nullptr, 1, std::vector<VectorPtr>{array, map});
  auto node = std::make_shared<core::ValuesNode>("nested-owner-values", std::vector<RowVectorPtr>{row});
  auto output = executeCanonicalTask(node);
  ASSERT_EQ(output.batches.size(), 1);
  auto elements = std::dynamic_pointer_cast<ArrayVector>(output.batches.front()->childAt(0))->elements();
  auto keys = std::dynamic_pointer_cast<MapVector>(output.batches.front()->childAt(1))->mapKeys();
  auto vals = std::dynamic_pointer_cast<MapVector>(output.batches.front()->childAt(1))->mapValues();
  node.reset(); row.reset(); array.reset(); map.reset();
  output.batches.clear(); output.task.reset(); pool.reset();
  EXPECT_FALSE(weak.expired());
  EXPECT_EQ(elements->asFlatVector<int64_t>()->valueAt(1), 23);
  EXPECT_EQ(keys->asFlatVector<int64_t>()->valueAt(0), 11);
  EXPECT_EQ(vals->asFlatVector<int64_t>()->valueAt(1), 23);
  elements.reset(); keys.reset();
  EXPECT_FALSE(weak.expired());
  vals.reset();
  EXPECT_TRUE(weak.expired());
}

TEST(CanonicalVectorPoolOwnershipTest, ArrowExportOwnsDifferentSourceChildAndAllocationPools) {
  auto parent = pangrova::test::testLeafPool("arrow-parent-owner");
  auto child = pangrova::test::testLeafPool("arrow-child-owner");
  auto allocation = pangrova::test::testLeafPool("arrow-allocation-owner");
  std::weak_ptr<memory::MemoryPool> parentWeak = parent, childWeak = child, allocationWeak = allocation;
  auto ints = integers(child.get());
  auto strings = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 2, parent.get());
  strings->set(0, StringView("abc")); strings->set(1, StringView("def"));
  auto row = std::make_shared<RowVector>(parent.get(), ROW({"n", "s"}, {BIGINT(), VARCHAR()}),
      nullptr, 2, std::vector<VectorPtr>{ints, strings});
  auto node = std::make_shared<core::ValuesNode>("arrow-owner-values", std::vector<RowVectorPtr>{row});
  auto output = executeCanonicalTask(node);
  ASSERT_EQ(output.batches.size(), 1);
  ArrowExport exported;
  exportToArrow(output.batches.front(), exported.array, allocation.get());
  exportToArrow(output.batches.front(), exported.schema);
  node.reset(); row.reset(); ints.reset(); strings.reset();
  output.batches.clear(); output.task.reset(); parent.reset(); child.reset(); allocation.reset();
  EXPECT_FALSE(parentWeak.expired()); EXPECT_FALSE(childWeak.expired()); EXPECT_FALSE(allocationWeak.expired());
  ASSERT_EQ(exported.array.n_children, 2);
  EXPECT_EQ(static_cast<const int64_t*>(exported.array.children[0]->buffers[1])[1], 23);
  auto* offsets = static_cast<const int32_t*>(exported.array.children[1]->buffers[1]);
  auto* bytes = static_cast<const char*>(exported.array.children[1]->buffers[2]);
  EXPECT_EQ(std::string(bytes + offsets[1], offsets[2] - offsets[1]), "def");
  exported.array.release(&exported.array);
  EXPECT_TRUE(parentWeak.expired()); EXPECT_TRUE(childWeak.expired()); EXPECT_TRUE(allocationWeak.expired());
}

TEST(CanonicalVectorPoolOwnershipTest, ConstantArrowExportRetainsActualInlineValueStorage) {
  auto pool = pangrova::test::testLeafPool("constant-arrow-owner");
  std::weak_ptr<memory::MemoryPool> weak = pool;
  auto constant = BaseVector::createConstant(BIGINT(), variant(int64_t{42}), 3, pool.get());
  std::weak_ptr<BaseVector> vectorWeak = constant;
  ArrowExport exported;
  exportToArrow(constant, exported.array, pool.get());
  exportToArrow(constant, exported.schema);
  constant.reset(); pool.reset();
  EXPECT_FALSE(vectorWeak.expired()); EXPECT_FALSE(weak.expired());
  ASSERT_EQ(exported.array.n_children, 2); // Actual REE run_ends/values encoding.
  EXPECT_EQ(static_cast<const int64_t*>(exported.array.children[1]->buffers[1])[0], 42);
  exported.array.release(&exported.array);
  EXPECT_TRUE(vectorWeak.expired()); EXPECT_TRUE(weak.expired());
}

TEST(CanonicalVectorPoolOwnershipTest, ZeroColumnRowsHaveAnOwnerWithoutSyntheticBuffers) {
  auto pool = pangrova::test::testLeafPool("zero-column-owner");
  std::weak_ptr<memory::MemoryPool> weak = pool;
  auto row = std::make_shared<RowVector>(pool.get(), ROW({}, {}), nullptr, 4, std::vector<VectorPtr>{});
  auto slice = row->slice(1, 2);
  row.reset(); pool.reset();
  EXPECT_FALSE(weak.expired()); EXPECT_EQ(slice->size(), 2);
  EXPECT_EQ(slice->type()->size(), 0); EXPECT_EQ(slice->nulls(), nullptr);
  slice.reset();
  EXPECT_TRUE(weak.expired());
}

} // namespace
} // namespace pangrova::velox::test
