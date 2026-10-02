#include "columnar/ColumnarImportLayout.h"

#include "../TestMemoryHelper.h"
#include <arrow/api.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

namespace pangrova::columnar {
namespace {
std::shared_ptr<arrow::Array> fixedVectors() {
  arrow::DoubleBuilder builder;
  for (const auto value : {1.0000000001, 0.0, -4.0, 7.0, 0.0, 9.0, 3.0, 4.0, 5.0}) {
    EXPECT_TRUE(builder.Append(value).ok());
  }
  auto values = builder.Finish().ValueOrDie();
  auto data = values->data()->Copy();
  data->buffers[0] = arrow::Buffer::FromString(std::string(1, static_cast<char>(0xed)) + std::string(1, '\1'));
  data->SetNullCount(2);
  values = arrow::MakeArray(data);
  auto type = arrow::fixed_size_list(arrow::field("element", arrow::float64(), true,
      arrow::key_value_metadata({"element-metadata"}, {"preserved"})), 3);
  return arrow::FixedSizeListArray::FromArrays(values, type,
      arrow::Buffer::FromString(std::string(1, '\5')), 1).ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> batch(const std::shared_ptr<arrow::Array>& vectors) {
  auto field = arrow::field("embedding", vectors->type(), true,
      arrow::key_value_metadata({"PARQUET:field_id"}, {"9"}));
  return arrow::RecordBatch::Make(arrow::schema({field}), vectors->length(), {vectors});
}

TEST(ColumnarImportLayoutTest, FixedVectorSharesDoublePayloadAndBothNullKinds) {
  auto source = fixedVectors();
  auto task = pangrova::test::testLeafPool("fixed-list-import");
  auto pool = std::make_shared<memory::ColumnarMemoryPool>(task);
  ColumnarImportUsage usage;
  auto converted = normalizeImportLayout(*batch(source), pool, usage).ValueOrDie();
  auto actual = std::static_pointer_cast<arrow::ListArray>(converted->column(0));
  auto values = std::static_pointer_cast<arrow::DoubleArray>(actual->values());
  EXPECT_EQ(actual->type_id(), arrow::Type::LIST);
  EXPECT_EQ(actual->data()->buffers[0], source->data()->buffers[0]);
  EXPECT_EQ(values->data()->buffers[1]->data(), source->data()->child_data[0]->buffers[1]->data());
  EXPECT_EQ(values->data()->buffers[0], source->data()->child_data[0]->buffers[0]);
  EXPECT_DOUBLE_EQ(values->Value(0), 1.0000000001);
  EXPECT_TRUE(values->IsNull(1));
  EXPECT_FALSE(actual->IsNull(0));
  EXPECT_TRUE(actual->IsNull(1));
  EXPECT_EQ(actual->value_length(1), 3); // NULL parent still owns three physical slots.
  EXPECT_EQ(actual->value_offset(2), 6);
  EXPECT_EQ(usage.copiedBytes, 0);
  EXPECT_EQ(usage.allocatedOffsetBytes, actual->data()->buffers[1]->capacity());
  EXPECT_GE(usage.allocatedOffsetBytes, 4 * sizeof(int32_t));
  EXPECT_EQ(pool->total_bytes_allocated(), usage.allocatedOffsetBytes);
  EXPECT_EQ(converted->schema()->field(0)->metadata()->Get("PARQUET:field_id").ValueOrDie(), "9");
  EXPECT_TRUE(static_cast<const arrow::ListType&>(*actual->type()).value_field()->nullable());
  auto allocator = std::weak_ptr<memory::ColumnarMemoryPool>(pool);
  pool.reset();
  EXPECT_FALSE(allocator.expired()); // Offset buffer retains allocator and actual task pool.
  converted.reset();
  actual.reset();
  values.reset();
  EXPECT_TRUE(allocator.expired());
  EXPECT_EQ(task->usedBytes(), 0);
}

TEST(ColumnarImportLayoutTest, SlicedVectorUsesRealPayloadOffsetAndCountsOnlyBitmapCopies) {
  auto full = fixedVectors();
  auto source = full->Slice(1, 2);
  auto task = pangrova::test::testLeafPool("sliced-fixed-list-import");
  auto pool = std::make_shared<memory::ColumnarMemoryPool>(task);
  ColumnarImportUsage usage;
  auto converted = normalizeImportLayout(*batch(source), pool, usage).ValueOrDie();
  auto actual = std::static_pointer_cast<arrow::ListArray>(converted->column(0));
  auto values = std::static_pointer_cast<arrow::DoubleArray>(actual->values());
  EXPECT_EQ(actual->offset(), 0);
  EXPECT_EQ(values->offset(), 0);
  EXPECT_EQ(values->data()->buffers[1]->data(),
      full->data()->child_data[0]->buffers[1]->data() + 3 * sizeof(double));
  EXPECT_TRUE(actual->IsNull(0));
  EXPECT_FALSE(actual->IsNull(1));
  EXPECT_TRUE(values->IsNull(1));
  EXPECT_DOUBLE_EQ(values->Value(3), 3.0);
  EXPECT_DOUBLE_EQ(values->Value(5), 5.0);
  EXPECT_EQ(actual->value_offset(1), 3);
  EXPECT_EQ(usage.allocatedOffsetBytes, actual->data()->buffers[1]->capacity());
  EXPECT_GE(usage.allocatedOffsetBytes, 3 * sizeof(int32_t));
  EXPECT_EQ(usage.copiedBytes, 2); // Two actual one-byte CopyBitmap outputs, not values.
  EXPECT_EQ(pool->total_bytes_allocated(), usage.allocatedOffsetBytes +
      actual->data()->buffers[0]->capacity() + values->data()->buffers[0]->capacity());
}

TEST(ColumnarImportLayoutTest, NestedStructSlicePreservesFieldIdentityAndOffsets) {
  auto source = fixedVectors();
  auto rows = arrow::StructArray::Make({source}, {arrow::field("vector-field", source->type(), true)}).ValueOrDie();
  auto sliced = rows->Slice(1, 2);
  auto pool = std::make_shared<memory::ColumnarMemoryPool>(pangrova::test::testLeafPool("nested-fixed-list-import"));
  ColumnarImportUsage usage;
  auto converted = normalizeImportLayout(*batch(sliced), pool, usage).ValueOrDie();
  auto actual = std::static_pointer_cast<arrow::StructArray>(converted->column(0));
  auto vectors = std::static_pointer_cast<arrow::ListArray>(actual->field(0));
  EXPECT_EQ(actual->offset(), 0);
  EXPECT_EQ(actual->type()->field(0)->name(), "vector-field");
  EXPECT_TRUE(vectors->IsNull(0));
  EXPECT_FALSE(vectors->IsNull(1));
  auto values = std::static_pointer_cast<arrow::DoubleArray>(vectors->values());
  EXPECT_DOUBLE_EQ(values->Value(5), 5.0);
  EXPECT_EQ(values->data()->buffers[1]->data(),
      source->data()->child_data[0]->buffers[1]->data() + 3 * sizeof(double));
}

TEST(ColumnarImportLayoutTest, MalformedFixedVectorRefusesBeforeConversionAndZeroColumnsRetainRows) {
  auto vectors = fixedVectors();
  auto malformed = vectors->data()->Copy();
  malformed->length = 4; // Actual nine-value child cannot hold four dimension-three rows.
  auto invalid = arrow::MakeArray(malformed);
  auto pool = std::make_shared<memory::ColumnarMemoryPool>(pangrova::test::testLeafPool("invalid-fixed-list-import"));
  ColumnarImportUsage usage;
  EXPECT_FALSE(normalizeImportLayout(*batch(invalid), pool, usage).ok());
  EXPECT_EQ(usage.allocatedOffsetBytes, 0);
  EXPECT_EQ(usage.copiedBytes, 0);
  auto empty = arrow::RecordBatch::Make(arrow::schema({}), 3, {});
  auto converted = normalizeImportLayout(*empty, pool, usage).ValueOrDie();
  EXPECT_EQ(converted->num_columns(), 0);
  EXPECT_EQ(converted->num_rows(), 3);
}
}  // namespace
}  // namespace pangrova::columnar
