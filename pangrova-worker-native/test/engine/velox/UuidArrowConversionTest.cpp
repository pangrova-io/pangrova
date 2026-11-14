#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>

#include <bit>
#include <optional>

#include "TestMemoryHelper.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

struct UuidExport {
  ArrowSchema schema{};
  ArrowArray array{};
  ~UuidExport() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

class UuidArrowBridgeTest : public ::testing::Test {
 protected:
  void SetUp() override { pool_ = pangrova::test::testLeafPool("uuid-arrow-bridge"); }

  // Independent big-endian wire oracle; includes every bit set, not just RFC versions.
  const std::vector<std::string> bytes_{
      std::string(16, '\0'), std::string(16, '\xff'),
      std::string("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff", 16)};

  int128_t value(size_t index) const {
    __uint128_t bits = 0;
    for (const auto byte : bytes_.at(index)) bits = (bits << 8) | static_cast<uint8_t>(byte);
    return std::bit_cast<int128_t>(bits);
  }

  RowVectorPtr values() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto ids = maker.flatVectorNullable<int128_t>({value(0), value(1), value(2), std::nullopt}, UUID());
    auto nested = maker.rowVector({"id"}, {ids});
    nested->setNull(2, true);
    const std::vector<vector_size_t> offsets{0, 1, 2, 3};
    return maker.rowVector({"scalar", "nested", "items", "mapping"}, {
        ids, nested, maker.arrayVector(offsets, ids),
        maker.mapVector(offsets, maker.flatVector<int32_t>({0, 1, 2, 3}), ids)});
  }

  ArrowBatchVectorSerde::StreamOptions options() const {
    substrait::Type uuid, nested, list, map;
    uuid.mutable_uuid()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    *nested.mutable_struct_()->add_types() = uuid;
    nested.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    *list.mutable_list()->mutable_type() = uuid;
    list.mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    *map.mutable_map()->mutable_key() = schema::primitive("i32", substrait::Type::NULLABILITY_REQUIRED);
    *map.mutable_map()->mutable_value() = uuid;
    map.mutable_map()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    ArrowBatchVectorSerde::StreamOptions result;
    result.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named({
        schema::computed("scalar", uuid), {"nested", nested, {"id"}},
        schema::computed("items", list), schema::computed("mapping", map)}));
    return result;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(UuidArrowBridgeTest, ExportsCanonicalExtensionAndBigEndianBytesIncludingNestedValues) {
  auto input = values();
  UuidExport data;
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  exportToArrow(input, data.array, pool_.get(), options);
  exportToArrow(input, data.schema, options);
  auto imported = arrow::ImportRecordBatch(&data.array, &data.schema);
  ASSERT_TRUE(imported.ok()) << imported.status();
  input.reset();
  const auto& batch = *imported;
  ASSERT_TRUE(batch->ValidateFull().ok()) << batch->ValidateFull();
  ASSERT_TRUE(batch->column(0)->type()->Equals(arrow::extension::uuid()));
  auto storage = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
      std::static_pointer_cast<arrow::ExtensionArray>(batch->column(0))->storage());
  for (size_t i = 0; i < bytes_.size(); ++i) EXPECT_EQ(storage->GetView(i), bytes_[i]);
  EXPECT_TRUE(storage->IsNull(3));
  EXPECT_TRUE(batch->schema()->field(1)->type()->field(0)->type()->Equals(arrow::extension::uuid()));
  EXPECT_TRUE(batch->schema()->field(2)->type()->field(0)->type()->Equals(arrow::extension::uuid()));
  EXPECT_TRUE(batch->schema()->field(3)->type()->field(0)->type()->field(1)->type()->Equals(arrow::extension::uuid()));
}

TEST_F(UuidArrowBridgeTest, NativeVectorOrderingAgreesWithUuidUnsignedComparison) {
  auto ids = values()->childAt(0);
  for (int left = 0; left < 3; ++left) {
    for (int right = 0; right < 3; ++right) {
      const auto a = static_cast<__uint128_t>(value(left));
      const auto b = static_cast<__uint128_t>(value(right));
      const int expected = a < b ? -1 : a > b ? 1 : 0;
      const auto actual = ids->compare(ids.get(), left, right, CompareFlags{}).value();
      EXPECT_EQ(actual < 0 ? -1 : actual > 0 ? 1 : 0, expected) << left << " versus " << right;
    }
  }
}

TEST_F(UuidArrowBridgeTest, ImportsIndependentCanonicalExtensionWithoutStringOrDecimalCoercion) {
  arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(16));
  for (const auto& bytes : bytes_) ASSERT_TRUE(builder.Append(bytes).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  std::shared_ptr<arrow::Array> storage;
  ASSERT_TRUE(builder.Finish(&storage).ok());
  auto ids = arrow::ExtensionType::WrapArray(arrow::extension::uuid(), storage);
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("id", ids->type())}), 4, {ids});
  UuidExport data;
  ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &data.array, &data.schema).ok());
  auto imported = std::dynamic_pointer_cast<RowVector>(importFromArrowAsOwner(data.schema, data.array, pool_.get()));
  storage.reset();
  ids.reset();
  batch.reset();
  ASSERT_TRUE(imported);
  ASSERT_TRUE(imported->childAt(0)->type()->equivalent(*UUID()));
  auto* actual = imported->childAt(0)->asFlatVector<int128_t>();
  for (size_t i = 0; i < bytes_.size(); ++i) EXPECT_EQ(actual->valueAt(i), value(i));
  EXPECT_TRUE(actual->isNullAt(3));
}

TEST_F(UuidArrowBridgeTest, EmptyAndAllNullUuidExportsRetainTheCanonicalSchema) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  for (size_t count : {0, 3}) {
    auto ids = maker.flatVectorNullable<int128_t>(std::vector<std::optional<int128_t>>(count), UUID());
    auto input = maker.rowVector({"id"}, {ids});
    UuidExport data;
    exportToArrow(input, data.array, pool_.get());
    exportToArrow(input, data.schema);
    auto imported = arrow::ImportRecordBatch(&data.array, &data.schema);
    ASSERT_TRUE(imported.ok()) << imported.status();
    ASSERT_TRUE((*imported)->ValidateFull().ok());
    EXPECT_EQ((*imported)->num_rows(), count);
    EXPECT_EQ((*imported)->column(0)->null_count(), count);
    EXPECT_TRUE((*imported)->column(0)->type()->Equals(arrow::extension::uuid()));
  }
}

TEST_F(UuidArrowBridgeTest, CanonicalExtensionRejectsMalformedWidthsAndMetadataWithoutBinaryInference) {
  for (int width : {15, 16, 17}) {
    UuidExport data;
    auto field = arrow::field("id", arrow::fixed_size_binary(width), true,
        arrow::key_value_metadata({"ARROW:extension:name", "ARROW:extension:metadata"},
            {"arrow.uuid", width == 16 ? "unexpected" : ""}));
    ASSERT_TRUE(arrow::ExportField(*field, &data.schema).ok());
    EXPECT_ANY_THROW(importFromArrow(data.schema));
  }
  UuidExport untagged;
  ASSERT_TRUE(arrow::ExportField(*arrow::field("binary", arrow::fixed_size_binary(16)), &untagged.schema).ok());
  EXPECT_ANY_THROW(importFromArrow(untagged.schema));
}

TEST_F(UuidArrowBridgeTest, CanonicalSelectionAndIterativeEncodingPreserveNestedUuidValues) {
  const auto input = values();
  const auto declared = options();
  ArrowBatchVectorSerde serde;
  const IndexRange ranges[]{{3, 1}, {1, 2}};
  for (bool iterative : {false, true}) {
    IOBufOutputStream stream(*pool_);
    StreamArena arena(pool_.get());
    if (iterative) {
      auto writer = serde.createIterativeSerializer(input->rowType(), 0, &arena, &declared);
      auto indices = AlignedBuffer::allocate<vector_size_t>(3, pool_.get());
      auto* raw = indices->asMutable<vector_size_t>();
      raw[0] = 3; raw[1] = 1; raw[2] = 2;
      auto dictionary = std::dynamic_pointer_cast<RowVector>(BaseVector::create(input->type(), 3, pool_.get()));
      for (uint32_t i = 0; i < input->childrenSize(); ++i) {
        dictionary->childAt(i) = BaseVector::wrapInDictionary(nullptr, indices, 3, input->childAt(i));
      }
      writer->append(dictionary);
      auto constant = std::dynamic_pointer_cast<RowVector>(BaseVector::create(input->type(), 2, pool_.get()));
      for (uint32_t i = 0; i < input->childrenSize(); ++i) {
        constant->childAt(i) = BaseVector::wrapInConstant(2, 2, input->childAt(i));
      }
      writer->append(constant);
      writer->flush(&stream);
    } else {
      auto writer = serde.createBatchSerializer(pool_.get(), &declared);
      writer->serialize(input, folly::Range<const IndexRange*>(ranges, 2), &stream);
    }
    auto wire = stream.getIOBuf();
    BufferInputStream source(byteRangesFromIOBuf(wire.get()));
    RowVectorPtr actual;
    serde.deserialize(&source, pool_.get(), input->rowType(), &actual, &declared);
    ASSERT_TRUE(actual);
    ASSERT_EQ(actual->size(), iterative ? 5 : 3);
    EXPECT_EQ(*actual->type(), *input->type());
    for (int i = 0; i < actual->size(); ++i) {
      EXPECT_TRUE(input->equalValueAt(actual.get(), i == 0 ? 3 : i < 3 ? i : 2, i));
    }
    EXPECT_TRUE(source.atEnd());
  }
}

}  // namespace
}  // namespace pangrova::velox::test
