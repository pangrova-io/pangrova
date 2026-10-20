#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <optional>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/plan/CanonicalPlanSchema.h"
#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
namespace schema = pangrova::velox::schema;

struct ExportedData {
  ArrowSchema schema{};
  ArrowArray array{};
  ~ExportedData() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

class TimeArrowBridgeTest : public ::testing::TestWithParam<int> {
 protected:
  void SetUp() override { pool_ = pangrova::test::testLeafPool("time-arrow-bridge"); }
  TypePtr timeType() const { return GetParam() == 6 ? TypePtr(TIME_MICRO_UTC()) : TypePtr(TIME()); }
  int64_t maximum() const { return GetParam() == 6 ? 86'399'999'999 : 86'399'999; }
  std::shared_ptr<arrow::DataType> arrowType() const {
    return GetParam() == 6 ? arrow::time64(arrow::TimeUnit::MICRO) : arrow::time32(arrow::TimeUnit::MILLI);
  }
  substrait::Type logicalTime() const {
    substrait::Type type;
    type.mutable_precision_time()->set_precision(GetParam());
    type.mutable_precision_time()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    return type;
  }
  ArrowBatchVectorSerde::StreamOptions options() const {
    const auto time = logicalTime();
    substrait::Type nested, array, map;
    *nested.mutable_struct_()->add_types() = time;
    nested.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    *array.mutable_list()->mutable_type() = time;
    array.mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    *map.mutable_map()->mutable_key() = schema::primitive("i32", substrait::Type::NULLABILITY_REQUIRED);
    *map.mutable_map()->mutable_value() = time;
    map.mutable_map()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    ArrowBatchVectorSerde::StreamOptions result;
    result.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named({
        schema::computed("scalar", time), {"nested", nested, {"value"}},
        schema::computed("items", array), schema::computed("mapping", map)}));
    return result;
  }
  RowVectorPtr values() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto times = maker.flatVectorNullable<int64_t>({0, 1, maximum(), std::nullopt}, timeType());
    auto nested = maker.rowVector({"value"}, {times});
    nested->setNull(2, true);
    const std::vector<vector_size_t> offsets{0, 1, 2, 3};
    return maker.rowVector({"scalar", "nested", "items", "mapping"}, {
        times, nested, maker.arrayVector(offsets, times),
        maker.mapVector(offsets, maker.flatVector<int32_t>({0, 1, 2, 3}), times)});
  }
  void checkRows(const RowVectorPtr& expected, const RowVectorPtr& actual,
      const std::vector<vector_size_t>& rows) {
    ASSERT_TRUE(actual);
    ASSERT_EQ(actual->size(), rows.size());
    EXPECT_EQ(*actual->type(), *expected->type());
    for (vector_size_t i = 0; i < actual->size(); ++i) {
      EXPECT_TRUE(expected->equalValueAt(actual.get(), rows[i], i)) << "row " << i;
    }
  }
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_P(TimeArrowBridgeTest, ExportsDeclaredUnitsAndExactValuesToStandardArrow) {
  auto input = values();
  const auto* originalPayload = input->childAt(0)->values()->as<uint8_t>();
  ExportedData data;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  exportToArrow(input, data.array, pool_.get(), options);
  exportToArrow(input, data.schema, options);
  auto imported = arrow::ImportRecordBatch(&data.array, &data.schema);
  ASSERT_TRUE(imported.ok()) << imported.status();
  const auto& batch = *imported;
  input.reset();
  ASSERT_TRUE(batch->column(0)->type()->Equals(arrowType()));
  ASSERT_TRUE(batch->ValidateFull().ok()) << batch->ValidateFull();
  ASSERT_TRUE(batch->column(0)->IsNull(3));
  if (GetParam() == 6) {
    EXPECT_EQ(batch->column(0)->data()->buffers[1]->data(), originalPayload);
    const auto times = std::static_pointer_cast<arrow::Time64Array>(batch->column(0));
    EXPECT_EQ(times->Value(1), 1);
    EXPECT_EQ(times->Value(2), maximum());
  } else {
    const auto times = std::static_pointer_cast<arrow::Time32Array>(batch->column(0));
    EXPECT_EQ(times->Value(1), 1);
    EXPECT_EQ(times->Value(2), maximum());
  }
  EXPECT_TRUE(batch->schema()->field(1)->type()->field(0)->type()->Equals(arrowType()));
  EXPECT_TRUE(batch->schema()->field(2)->type()->field(0)->type()->Equals(arrowType()));
  EXPECT_TRUE(batch->schema()->field(3)->type()->field(0)->type()->field(1)->type()->Equals(arrowType()));
}

TEST_P(TimeArrowBridgeTest, ImportsIndependentStandardArrowWithoutReducingPrecision) {
  std::shared_ptr<arrow::Array> times;
  if (GetParam() == 6) {
    arrow::Time64Builder builder(arrowType(), arrow::default_memory_pool());
    ASSERT_TRUE(builder.AppendValues({0, 1, maximum()}).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Finish(&times).ok());
  } else {
    arrow::Time32Builder builder(arrowType(), arrow::default_memory_pool());
    ASSERT_TRUE(builder.AppendValues({0, 1, static_cast<int32_t>(maximum())}).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Finish(&times).ok());
  }
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrowType())}), 4, {times});
  const auto* originalPayload = times->data()->buffers[1]->data();
  ExportedData data;
  ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &data.array, &data.schema).ok());
  const auto imported = std::dynamic_pointer_cast<RowVector>(
      importFromArrowAsOwner(data.schema, data.array, pool_.get()));
  times.reset();
  batch.reset();
  ASSERT_TRUE(imported);
  ASSERT_EQ(*imported->childAt(0)->type(), *timeType());
  if (GetParam() == 6) EXPECT_EQ(imported->childAt(0)->values()->as<uint8_t>(), originalPayload);
  const auto* actual = imported->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(actual->valueAt(0), 0);
  EXPECT_EQ(actual->valueAt(1), 1);
  EXPECT_EQ(actual->valueAt(2), maximum());
  EXPECT_TRUE(actual->isNullAt(3));
}

TEST_P(TimeArrowBridgeTest, CanonicalBatchSelectionPreservesNestedTimes) {
  const auto input = values();
  const auto declared = options();
  ArrowBatchVectorSerde serde;
  const auto writer = serde.createBatchSerializer(pool_.get(), &declared);
  const IndexRange ranges[]{{3, 1}, {1, 2}};
  IOBufOutputStream stream(*pool_);
  writer->serialize(input, folly::Range<const IndexRange*>(ranges, 2), &stream);
  auto wire = stream.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  serde.deserialize(&source, pool_.get(), input->rowType(), &result, &declared);
  checkRows(input, result, {3, 1, 2});
  EXPECT_TRUE(source.atEnd());
}

TEST_P(TimeArrowBridgeTest, CanonicalIterativePagesPreserveNestedDictionaryAndConstantTimes) {
  const auto input = values();
  const auto declared = options();
  ArrowBatchVectorSerde serde;
  StreamArena arena(pool_.get());
  const auto writer = serde.createIterativeSerializer(input->rowType(), 0, &arena, &declared);
  auto indices = AlignedBuffer::allocate<vector_size_t>(3, pool_.get());
  auto* raw = indices->asMutable<vector_size_t>();
  raw[0] = 3; raw[1] = 2; raw[2] = 1;
  const auto dictionary = std::dynamic_pointer_cast<RowVector>(BaseVector::create(input->type(), 3, pool_.get()));
  for (uint32_t i = 0; i < input->childrenSize(); ++i) {
    dictionary->childAt(i) = BaseVector::wrapInDictionary(nullptr, indices, 3, input->childAt(i));
  }
  writer->append(dictionary);
  const auto constant = std::dynamic_pointer_cast<RowVector>(BaseVector::create(input->type(), 2, pool_.get()));
  for (uint32_t i = 0; i < input->childrenSize(); ++i) {
    constant->childAt(i) = BaseVector::wrapInConstant(2, 1, input->childAt(i));
  }
  writer->append(constant);
  IOBufOutputStream stream(*pool_);
  writer->flush(&stream);
  auto wire = stream.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  serde.deserialize(&source, pool_.get(), input->rowType(), &result, &declared);
  checkRows(input, result, {3, 2, 1, 1, 1});
  EXPECT_TRUE(source.atEnd());
}

INSTANTIATE_TEST_SUITE_P(Precision, TimeArrowBridgeTest, ::testing::Values(3, 6),
    [](const auto& info) { return info.param == 6 ? "Microseconds" : "Milliseconds"; });
}  // namespace
}  // namespace pangrova::velox::test
