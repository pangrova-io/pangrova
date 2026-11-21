#include "engine/write/PhysicalWriteArrowExport.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <stdexcept>

namespace pangrova::write {
namespace {

std::shared_ptr<arrow::Array> doubles() {
  arrow::DoubleBuilder builder;
  EXPECT_TRUE(builder.Append(1.0000000001).ok());
  EXPECT_TRUE(builder.AppendNull().ok());
  EXPECT_TRUE(builder.Append(-4.0).ok());
  auto result = builder.Finish();
  EXPECT_TRUE(result.ok());
  return *result;
}

TEST(PhysicalWriteArrowExportTest, OrdinalNamesRetainSlicedNestedOwnersAndNullMasks) {
  auto values = doubles();
  auto row_data = arrow::ArrayData::Make(
      arrow::struct_({arrow::field("c0", arrow::float64(), true)}), 3, {nullptr});
  row_data->child_data = {values->data()};
  auto rows = arrow::MakeArray(row_data)->Slice(1, 2);
  auto source = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("ordinal0", rows->type(), true)}), 2, {rows});
  auto target = arrow::schema({arrow::field("records",
      arrow::struct_({arrow::field("quoted\"field", arrow::float64(), true)}), true)});
  auto rebound = bindPhysicalWriteMetadata(*source, *target);
  ASSERT_EQ(rebound->num_columns(), 1);
  EXPECT_EQ(rebound->schema()->field(0)->name(), "records");
  auto actual = rebound->column(0)->data();
  EXPECT_EQ(actual->offset, rows->offset());
  EXPECT_EQ(actual->length, rows->length());
  EXPECT_EQ(actual->buffers, rows->data()->buffers);
  ASSERT_EQ(actual->child_data.size(), 1);
  EXPECT_EQ(actual->child_data[0]->buffers, values->data()->buffers);
  EXPECT_EQ(actual->child_data[0]->offset, values->offset());
  EXPECT_EQ(static_cast<const arrow::StructType&>(*actual->type).field(0)->name(), "quoted\"field");
  auto row = std::static_pointer_cast<arrow::StructArray>(rebound->column(0));
  auto child = std::static_pointer_cast<arrow::DoubleArray>(row->field(0));
  EXPECT_TRUE(child->IsNull(0));
  EXPECT_DOUBLE_EQ(child->Value(1), -4.0);
}

TEST(PhysicalWriteArrowExportTest, VariableVectorOffsetsStayForSoleDimensionAdapter) {
  auto value_builder = std::make_shared<arrow::DoubleBuilder>();
  arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(value_builder->Append(99.0).ok());
  ASSERT_TRUE(value_builder->Append(98.0).ok());
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(value_builder->Append(1.0000000001).ok());
  ASSERT_TRUE(value_builder->AppendNull().ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  auto result = builder.Finish();
  ASSERT_TRUE(result.ok());
  auto vectors = (*result)->Slice(1, 2);
  auto source = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("c0", vectors->type(), true)}), 2, {vectors});
  auto target = arrow::schema({arrow::field("embedding",
      arrow::fixed_size_list(arrow::field("element", arrow::float64(), true), 2), true)});
  auto rebound = bindPhysicalWriteMetadata(*source, *target);
  auto actual = rebound->column(0)->data();
  EXPECT_EQ(actual->type->id(), arrow::Type::LIST);
  EXPECT_EQ(actual->offset, vectors->offset());
  EXPECT_EQ(actual->buffers, vectors->data()->buffers);
  EXPECT_EQ(actual->child_data[0]->buffers, vectors->data()->child_data[0]->buffers);
  auto list = std::static_pointer_cast<arrow::ListArray>(rebound->column(0));
  EXPECT_EQ(list->value_offset(0), 2);
  EXPECT_EQ(list->value_length(0), 2);
  EXPECT_FALSE(list->IsNull(0));
  EXPECT_TRUE(list->IsNull(1));
  auto values = std::static_pointer_cast<arrow::DoubleArray>(list->values());
  EXPECT_DOUBLE_EQ(values->Value(2), 1.0000000001);
  EXPECT_TRUE(values->IsNull(3));
}

TEST(PhysicalWriteArrowExportTest, ScalarPrecisionAndMissingColumnsNeverCoerce) {
  auto source = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("c0", arrow::float64(), true)}), 3, {doubles()});
  auto narrowed = arrow::schema({arrow::field("value", arrow::float32(), true)});
  EXPECT_THROW(bindPhysicalWriteMetadata(*source, *narrowed), std::invalid_argument);
  auto missing = arrow::schema({arrow::field("value", arrow::float64(), true),
      arrow::field("other", arrow::int64(), false)});
  EXPECT_THROW(bindPhysicalWriteMetadata(*source, *missing), std::invalid_argument);
  auto wrong_nested = arrow::schema({arrow::field("value",
      arrow::struct_({arrow::field("value", arrow::float64(), true)}), true)});
  EXPECT_THROW(bindPhysicalWriteMetadata(*source, *wrong_nested), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::write
