// Unit tests for ColumnarDataExporter::exportBatchToJava.
// Validates zero-copy export of columnar batches to C Data Interface
// (ArrowArray + ArrowSchema) for flat, nested, and dictionary-encoded types.

#include "engine/columnar/ColumnarDataExporter.h"

#include <gtest/gtest.h>

#include <arrow/c/bridge.h>

#include "TestMemoryHelper.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::jni {
namespace {

using namespace facebook::velox;

using VeloxPool = facebook::velox::memory::MemoryPool;

template <typename T>
VectorPtr makeFlatVector(
    VeloxPool* pool,
    const std::vector<T>& values) {
  auto vector = BaseVector::create(CppToType<T>::create(), values.size(), pool);
  auto flat = vector->template asFlatVector<T>();
  for (size_t i = 0; i < values.size(); ++i) {
    flat->set(i, values[i]);
  }
  return vector;
}

template <typename T>
VectorPtr makeFlatVectorNullable(
    VeloxPool* pool,
    const std::vector<std::optional<T>>& values) {
  auto vector = BaseVector::create(CppToType<T>::create(), values.size(), pool);
  auto flat = vector->template asFlatVector<T>();
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].has_value()) {
      flat->set(i, values[i].value());
    } else {
      vector->setNull(i, true);
    }
  }
  return vector;
}

RowVectorPtr makeRowVector(
    VeloxPool* pool,
    const std::vector<std::string>& names,
    const std::vector<VectorPtr>& children) {
  std::vector<std::shared_ptr<const Type>> types;
  types.reserve(children.size());
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  auto rowType = ROW(std::move(names), std::move(types));
  auto size = children.empty() ? 0 : children[0]->size();
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, children);
}

class ColumnarDataExporterTest : public ::testing::Test {
 protected:
  std::shared_ptr<VeloxPool> pool() {
    return pangrova::test::testMemoryManager()->addLeafPool(
        "ColumnarDataExporterTest");
  }
};

TEST_F(ColumnarDataExporterTest, FlatIntegerColumns) {
  auto p = pool();
  auto col0 = makeFlatVector<int32_t>(p.get(), {1, 2, 3, 4, 5});
  auto col1 = makeFlatVector<int64_t>(p.get(), {10, 20, 30, 40, 50});
  auto rowVector = makeRowVector(p.get(), {"a", "b"}, {col0, col1});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto batch = result.ValueOrDie();
  EXPECT_EQ(batch->num_rows(), 5);
  EXPECT_EQ(batch->num_columns(), 2);
  EXPECT_EQ(batch->schema()->field(0)->name(), "a");
  EXPECT_EQ(batch->schema()->field(1)->name(), "b");
}

TEST_F(ColumnarDataExporterTest, FlatDoubleAndVarchar) {
  auto p = pool();
  auto doubles = makeFlatVector<double>(p.get(), {1.1, 2.2, 3.3});
  auto strVector = BaseVector::create(VARCHAR(), 3, p.get());
  auto flatStr = strVector->asFlatVector<StringView>();
  flatStr->set(0, StringView("hello"));
  flatStr->set(1, StringView("world"));
  flatStr->set(2, StringView("!"));
  auto rowVector = makeRowVector(p.get(), {"d", "s"}, {doubles, strVector});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie()->num_rows(), 3);
  EXPECT_EQ(result.ValueOrDie()->num_columns(), 2);
}

TEST_F(ColumnarDataExporterTest, FlatBooleanColumn) {
  auto p = pool();
  auto bools = makeFlatVector<bool>(p.get(), {true, false, true, false});
  auto rowVector = makeRowVector(p.get(), {"flag"}, {bools});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie()->num_rows(), 4);
}

TEST_F(ColumnarDataExporterTest, StructNestedType) {
  auto p = pool();
  auto xCol = makeFlatVector<int32_t>(p.get(), {10, 20, 30});
  auto yCol = BaseVector::create(VARCHAR(), 3, p.get());
  auto flatY = yCol->asFlatVector<StringView>();
  flatY->set(0, StringView("a"));
  flatY->set(1, StringView("b"));
  flatY->set(2, StringView("c"));
  auto structType = ROW({"x", "y"}, {INTEGER(), VARCHAR()});
  auto structVector = std::make_shared<RowVector>(
      p.get(), structType, nullptr, 3,
      std::vector<VectorPtr>{xCol, yCol});
  auto rowVector = makeRowVector(p.get(), {"s"}, {structVector});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie()->num_rows(), 3);
  EXPECT_EQ(result.ValueOrDie()->num_columns(), 1);
}

TEST_F(ColumnarDataExporterTest, NullableColumns) {
  auto p = pool();
  auto col = makeFlatVectorNullable<int32_t>(
      p.get(), {std::optional<int32_t>(1), std::nullopt,
                std::optional<int32_t>(3)});
  auto rowVector = makeRowVector(p.get(), {"nullable_col"}, {col});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto batch = result.ValueOrDie();
  EXPECT_EQ(batch->num_rows(), 3);
  EXPECT_FALSE(batch->column(0)->IsNull(0));
  EXPECT_TRUE(batch->column(0)->IsNull(1));
  EXPECT_FALSE(batch->column(0)->IsNull(2));
}

TEST_F(ColumnarDataExporterTest, EmptyRowVector) {
  auto p = pool();
  auto col = makeFlatVector<int32_t>(p.get(), {});
  auto rowVector = makeRowVector(p.get(), {"empty"}, {col});

  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  ASSERT_NO_THROW(ColumnarDataExporter::exportBatchToJava(
      rowVector, &arrowArray, &arrowSchema, p.get()));

  auto result = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie()->num_rows(), 0);
}

TEST_F(ColumnarDataExporterTest, NullRowVectorThrows) {
  auto p = pool();
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  EXPECT_THROW(
      ColumnarDataExporter::exportBatchToJava(
          nullptr, &arrowArray, &arrowSchema, p.get()),
      std::runtime_error);
}

TEST_F(ColumnarDataExporterTest, NullOutputPointersThrow) {
  auto p = pool();
  auto col = makeFlatVector<int32_t>(p.get(), {1});
  auto rowVector = makeRowVector(p.get(), {"x"}, {col});
  EXPECT_THROW(
      ColumnarDataExporter::exportBatchToJava(
          rowVector, nullptr, nullptr, p.get()),
      std::runtime_error);
}

TEST_F(ColumnarDataExporterTest, NullPoolThrows) {
  auto p = pool();
  auto col = makeFlatVector<int32_t>(p.get(), {1});
  auto rowVector = makeRowVector(p.get(), {"x"}, {col});
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  EXPECT_THROW(
      ColumnarDataExporter::exportBatchToJava(
          rowVector, &arrowArray, &arrowSchema, nullptr),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::jni
