#include "engine/columnar/CanonicalArrowValidation.h"

#include <gtest/gtest.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pangrova::columnar::test {
namespace {

std::shared_ptr<arrow::Array> integers(std::initializer_list<std::optional<int64_t>> values) {
  arrow::Int64Builder builder;
  for (const auto value : values) {
    EXPECT_TRUE((value ? builder.Append(*value) : builder.AppendNull()).ok());
  }
  return builder.Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> offsets(std::initializer_list<int32_t> values) {
  arrow::Int32Builder builder;
  for (const auto value : values) EXPECT_TRUE(builder.Append(value).ok());
  return builder.Finish().ValueOrDie();
}

std::shared_ptr<arrow::RecordBatch> batch(
    const std::shared_ptr<arrow::Array>& values, bool nullable) {
  return arrow::RecordBatch::Make(
      arrow::schema({arrow::field("logical", values->type(), nullable)}),
      values->length(), arrow::ArrayVector{values});
}

std::shared_ptr<arrow::Buffer> mutableValidity(uint8_t bits) {
  auto allocated = arrow::AllocateBuffer(1).ValueOrDie();
  allocated->mutable_data()[0] = bits;
  return std::shared_ptr<arrow::Buffer>(std::move(allocated));
}

std::shared_ptr<arrow::ListArray> lists(const std::shared_ptr<arrow::Array>& values,
    const std::shared_ptr<arrow::Array>& positions, std::shared_ptr<arrow::Buffer> validity = nullptr) {
  const auto type = arrow::list(arrow::field("item", values->type(), false));
  return arrow::ListArray::FromArrays(type, *positions, *values,
      arrow::default_memory_pool(), std::move(validity)).ValueOrDie();
}

TEST(CanonicalArrowValidationTest, ExactNamesNullabilityAndMetadataCannotBeRebound) {
  const auto source = batch(integers({7}), false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  const auto wrongName = arrow::schema({source->schema()->field(0)->WithName("wrong")});
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *wrongName).IsTypeError());
  const auto wrongNullable = arrow::schema({source->schema()->field(0)->WithNullable(true)});
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *wrongNullable).IsTypeError());
  const auto wrongMetadata = arrow::schema({source->schema()->field(0)->WithMetadata(
      arrow::key_value_metadata({"authority"}, {"different"}))});
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *wrongMetadata).IsTypeError());
}

TEST(CanonicalArrowValidationTest, RequiredScalarAndNullTypeRefuseActualNullValues) {
  const auto required = batch(integers({7, std::nullopt}), false);
  EXPECT_TRUE(required->ValidateFull().ok());
  EXPECT_TRUE(validateCanonicalArrowBatch(*required, *required->schema()).IsInvalid());
  const auto nullable = batch(required->column(0), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*nullable, *nullable->schema()).ok());
  const auto nulls = batch(std::make_shared<arrow::NullArray>(1), false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*nulls, *nulls->schema()).IsInvalid());
}

TEST(CanonicalArrowValidationTest, NullableParentMasksRequiredChildrenAndSlicesUseActualRows) {
  const auto children = integers({std::nullopt, 17, std::nullopt});
  const auto type = arrow::struct_({arrow::field("child", arrow::int64(), false)});
  const auto structure = std::make_shared<arrow::StructArray>(type, 3,
      arrow::ArrayVector{children}, arrow::Buffer::FromString(std::string(1, '\2')), 2);
  const auto source = batch(structure, true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  const auto sliced = batch(structure->Slice(1, 1), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*sliced, *sliced->schema()).ok());
  const auto invalid = batch(std::make_shared<arrow::StructArray>(type, 3,
      arrow::ArrayVector{children}), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*invalid, *invalid->schema()).IsInvalid());
}

TEST(CanonicalArrowValidationTest, ListChecksOnlyVisibleValidParentOffsets) {
  const auto values = integers({std::nullopt, 31, std::nullopt});
  const auto full = lists(values, offsets({0, 1, 2, 3}),
      arrow::Buffer::FromString(std::string(1, '\2')));
  const auto source = batch(full, true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  const auto sliced = batch(full->Slice(1, 1), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*sliced, *sliced->schema()).ok());
  EXPECT_EQ(std::static_pointer_cast<arrow::ListArray>(sliced->column(0))->value_offset(0), 1);
  const auto invalid = batch(lists(values, offsets({0, 1, 2, 3})), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*invalid, *invalid->schema()).IsInvalid());
}

TEST(CanonicalArrowValidationTest, FullArrowValidationRejectsMalformedOffsetsBeforeTraversal) {
  const auto valid = lists(integers({7}), offsets({0, 1}));
  auto malformed = valid->data()->Copy();
  malformed->length = 2;
  const auto source = batch(arrow::MakeArray(malformed), true);
  EXPECT_FALSE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
}

TEST(CanonicalArrowValidationTest, LargeAndFixedListsRespectVisibleSliceOffsets) {
  const auto values = integers({std::nullopt, 31, std::nullopt});
  const auto validity = arrow::Buffer::FromString(std::string(1, '\2'));
  const auto largeType = arrow::large_list(arrow::field("item", arrow::int64(), false));
  const auto large = arrow::LargeListArray::FromArrays(largeType, *integers({0, 1, 2, 3}),
      *values, arrow::default_memory_pool(), validity).ValueOrDie();
  const auto fixedType = arrow::fixed_size_list(arrow::field("item", arrow::int64(), false), 1);
  const auto fixed = arrow::FixedSizeListArray::FromArrays(values, fixedType, validity, 2).ValueOrDie();
  for (const auto& source : arrow::ArrayVector{large, fixed}) {
    const auto sliced = batch(source->Slice(1, 1), true);
    EXPECT_TRUE(validateCanonicalArrowBatch(*sliced, *sliced->schema()).ok());
    const auto hidden = batch(source, true);
    EXPECT_TRUE(validateCanonicalArrowBatch(*hidden, *hidden->schema()).ok());
  }
  const auto invalid = batch(arrow::FixedSizeListArray::FromArrays(values, fixedType).ValueOrDie(), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*invalid, *invalid->schema()).IsInvalid());
}

TEST(CanonicalArrowValidationTest, DictionaryRequirednessUsesReferencedValuesAndIndexNulls) {
  const auto dictionary = integers({std::nullopt, 23});
  const auto good = arrow::DictionaryArray::FromArrays(integers({1, 1}), dictionary).ValueOrDie();
  const auto source = batch(good, false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  const auto valueNull = batch(
      arrow::DictionaryArray::FromArrays(integers({1, 0}), dictionary).ValueOrDie(), false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*valueNull, *valueNull->schema()).IsInvalid());
  const auto indexNull = batch(
      arrow::DictionaryArray::FromArrays(integers({1, std::nullopt}), dictionary).ValueOrDie(), false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*indexNull, *indexNull->schema()).IsInvalid());
  const auto nullable = batch(valueNull->column(0), true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*nullable, *nullable->schema()).ok());
}

TEST(CanonicalArrowValidationTest, MapKeysAndEntriesRemainRequiredAndValuesMayBeNull) {
  const auto keys = integers({11, 12});
  const auto values = integers({std::nullopt, 29});
  const auto entriesType = arrow::struct_({arrow::field("key", arrow::int64(), false),
      arrow::field("value", arrow::int64(), true)});
  const auto entryValidity = mutableValidity(3);
  const auto entries = std::make_shared<arrow::StructArray>(entriesType, 2,
      arrow::ArrayVector{keys, values}, entryValidity, 0);
  const auto mapType = std::make_shared<arrow::MapType>(arrow::field("entries", entriesType, false), false);
  const auto positions = offsets({0, 1, 2});
  auto maps = std::make_shared<arrow::MapArray>(mapType, 2,
      positions->data()->buffers[1], entries);
  const auto entryData = maps->values()->data();
  ASSERT_EQ(entryData.get(), maps->data()->child_data[0].get());
  entryData->buffers[0] = entryValidity;
  // Rebox the still-valid data so Array's cached bitmap pointer sees the attached buffer.
  maps = std::make_shared<arrow::MapArray>(maps->data());
  const auto source = batch(maps, true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  EXPECT_EQ(maps->values()->data()->buffers[0].get(), entryValidity.get());
  ASSERT_EQ(maps->values()->data().get(), entryData.get());
  ASSERT_EQ(maps->values()->null_bitmap_data(), entryValidity->data());
  entryValidity->mutable_data()[0] = 2;
  entryData->SetNullCount(1);
  ASSERT_TRUE(maps->values()->IsNull(0));
  ASSERT_FALSE(maps->values()->IsNull(1));
  EXPECT_FALSE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
}

TEST(CanonicalArrowValidationTest, NullMapKeyCannotPassAnOtherwiseExactSchema) {
  const auto originalKeys = integers({11, 12});
  auto keyData = originalKeys->data()->Copy();
  const auto validity = mutableValidity(3);
  keyData->buffers[0] = validity;
  keyData->SetNullCount(0);
  const auto keys = arrow::MakeArray(keyData);
  const auto maps = arrow::MapArray::FromArrays(offsets({0, 1, 2}), keys, integers({7, 8})).ValueOrDie();
  const auto source = batch(maps, true);
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
  validity->mutable_data()[0] = 2;
  std::static_pointer_cast<arrow::MapArray>(maps)->keys()->data()->SetNullCount(1);
  EXPECT_FALSE(validateCanonicalArrowBatch(*source, *source->schema()).ok());
}

TEST(CanonicalArrowValidationTest, LateBatchFailureLeavesEarlierActualBufferOwnerUnchanged) {
  const auto first = batch(integers({47}), false);
  const auto owner = first->column(0)->data()->buffers[1];
  EXPECT_TRUE(validateCanonicalArrowBatch(*first, *first->schema()).ok());
  const auto late = batch(integers({std::nullopt}), false);
  EXPECT_TRUE(validateCanonicalArrowBatch(*late, *first->schema()).IsInvalid());
  EXPECT_EQ(first->column(0)->data()->buffers[1].get(), owner.get());
  EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(first->column(0))->Value(0), 47);
}

TEST(CanonicalArrowValidationTest, ZeroColumnBatchRetainsActualNonzeroCardinality) {
  const auto schema = arrow::schema({});
  const auto source = arrow::RecordBatch::Make(schema, 3, arrow::ArrayVector{});
  EXPECT_TRUE(validateCanonicalArrowBatch(*source, *schema).ok());
  EXPECT_EQ(source->num_rows(), 3);
}

}  // namespace
}  // namespace pangrova::columnar::test
