#include "engine/write/WriteBatchStream.h"
#include "engine/write/WriteArrowExport.h"

#include "TestMemoryHelper.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/message.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::write::test {
namespace {

// Exercise the actual production codec and task-owned allocation without a JVM.
arrow::Result<std::shared_ptr<arrow::Buffer>> encodeActualWriteBatch(
    const arrow::RecordBatch& batch) {
  auto pool = pangrova::test::testLeafPool("actual-write-stream");
  return encodeWriteBatch(batch, std::make_shared<memory::ColumnarMemoryPool>(pool));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> makeSourceBatch() {
  arrow::StringBuilder names;
  ARROW_RETURN_NOT_OK(names.Append("last"));
  ARROW_RETURN_NOT_OK(names.AppendNull());
  ARROW_RETURN_NOT_OK(names.Append(""));
  ARROW_RETURN_NOT_OK(names.Append("first"));
  ARROW_ASSIGN_OR_RAISE(auto nameValues, names.Finish());

  arrow::Int64Builder ids;
  ARROW_RETURN_NOT_OK(ids.Append(std::numeric_limits<int64_t>::min()));
  ARROW_RETURN_NOT_OK(ids.Append(-1));
  ARROW_RETURN_NOT_OK(ids.Append(std::numeric_limits<int64_t>::max()));
  ARROW_RETURN_NOT_OK(ids.AppendNull());
  ARROW_ASSIGN_OR_RAISE(auto idValues, ids.Finish());

  return arrow::RecordBatch::Make(
      arrow::schema({arrow::field("name", arrow::utf8()),
                     arrow::field("id", arrow::int64())}),
      4, {nameValues, idValues});
}

void assertRoundTrip(const arrow::RecordBatch& batch) {
  const auto serialized = encodeActualWriteBatch(batch);
  ASSERT_TRUE(serialized.ok()) << serialized.status();
  const auto buffer = *serialized;

  // A stream reader accepts EOF without EOS, so verify explicit framing too.
  constexpr std::array<uint8_t, 8> kEndOfStream{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0};
  ASSERT_GE(buffer->size(), static_cast<int64_t>(kEndOfStream.size()));
  EXPECT_TRUE(std::equal(kEndOfStream.begin(), kEndOfStream.end(),
                        buffer->data() + buffer->size() - kEndOfStream.size()));

  const auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  const auto opened = arrow::ipc::RecordBatchStreamReader::Open(input);
  ASSERT_TRUE(opened.ok()) << opened.status();
  const auto reader = *opened;
  ASSERT_TRUE(reader->schema()->Equals(*batch.schema(), true));
  std::shared_ptr<arrow::RecordBatch> decoded;
  ASSERT_TRUE(reader->ReadNext(&decoded).ok());
  ASSERT_NE(decoded, nullptr);
  EXPECT_EQ(decoded->num_rows(), batch.num_rows());
  EXPECT_EQ(decoded->num_columns(), batch.num_columns());
  EXPECT_TRUE(decoded->Equals(batch, true));

  ASSERT_TRUE(reader->ReadNext(&decoded).ok());
  EXPECT_EQ(decoded, nullptr);
  EXPECT_EQ(reader->stats().num_record_batches, 1);
  EXPECT_EQ(reader->stats().num_dictionary_batches, 0);
  EXPECT_EQ(reader->stats().num_messages, 2);
  const auto position = input->Tell();
  ASSERT_TRUE(position.ok()) << position.status();
  EXPECT_EQ(*position, buffer->size());
  EXPECT_TRUE(reader->Close().ok());
}

TEST(WriteBatchStreamTest, RoundTripsNullsEmptyStringsAndIntegerBoundaries) {
  const auto batch = makeSourceBatch();
  ASSERT_TRUE(batch.ok()) << batch.status();
  assertRoundTrip(**batch);
}

TEST(WriteBatchStreamTest, PreservesReorderedSourceColumns) {
  const auto source = makeSourceBatch();
  ASSERT_TRUE(source.ok()) << source.status();
  const auto reordered = (*source)->SelectColumns({1, 0});
  ASSERT_TRUE(reordered.ok()) << reordered.status();
  ASSERT_EQ((*reordered)->schema()->field(0)->name(), "id");
  ASSERT_EQ((*reordered)->schema()->field(1)->name(), "name");
  assertRoundTrip(**reordered);
}

TEST(WriteBatchStreamTest, PreservesDuplicateSourceColumnNames) {
  const auto source = makeSourceBatch();
  ASSERT_TRUE(source.ok()) << source.status();
  const auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("value", arrow::utf8()),
                     arrow::field("value", arrow::int64())}),
      (*source)->num_rows(), (*source)->columns());
  assertRoundTrip(*batch);
}

TEST(WriteBatchStreamTest, RoundTripsNestedStructsListsAndNulls) {
  const auto items = std::make_shared<arrow::Int32Builder>();
  const auto lists = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), items);
  const auto labels = std::make_shared<arrow::StringBuilder>();
  const auto type = arrow::struct_({arrow::field("items", lists->type()),
                                    arrow::field("label", arrow::utf8())});
  arrow::StructBuilder builder(type, arrow::default_memory_pool(), {lists, labels});
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(lists->Append().ok());
  ASSERT_TRUE(items->Append(7).ok());
  ASSERT_TRUE(items->AppendNull().ok());
  ASSERT_TRUE(items->Append(-9).ok());
  ASSERT_TRUE(labels->Append("nested").ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(lists->AppendNull().ok());
  ASSERT_TRUE(labels->AppendNull().ok());
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(lists->Append().ok());
  ASSERT_TRUE(labels->Append("").ok());
  const auto values = builder.Finish();
  ASSERT_TRUE(values.ok()) << values.status();
  const auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("payload", type)}), 4, {*values});
  assertRoundTrip(*batch);
}

TEST(WriteBatchStreamTest, FlattensConstantArrayBeforeArrowExport) {
  pangrova::test::testMemoryManager();
  auto pool = pangrova::test::testLeafPool("WriteBatchStreamNestedArrowExport");
  facebook::velox::test::VectorMaker maker(pool.get());
  auto array = maker.arrayVector<double>({{1.0, 2.0, 3.0}});
  auto constant = facebook::velox::BaseVector::wrapInConstant(2, 0, array);
  auto batch = maker.rowVector({"embedding"}, {constant});

  auto normalized = prepareWriteArrowExport(batch);
  auto row = std::dynamic_pointer_cast<facebook::velox::RowVector>(normalized);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->encoding(),
            facebook::velox::VectorEncoding::Simple::ARRAY);

  ArrowArray outArray{};
  ArrowSchema outSchema{};
  ArrowOptions options;
  options.flattenDictionary = true;
  options.flattenConstant = true;
  facebook::velox::exportToArrow(normalized, outArray, pool.get(), options);
  facebook::velox::exportToArrow(normalized, outSchema, options);
  const auto imported = arrow::ImportRecordBatch(&outArray, &outSchema);
  ASSERT_TRUE(imported.ok()) << imported.status();
  ASSERT_EQ((*imported)->num_rows(), 2);
  ASSERT_EQ((*imported)->column(0)->type_id(), arrow::Type::LIST);
  EXPECT_TRUE((*imported)->column(0)->GetScalar(0).ValueOrDie()->Equals(
      *(*imported)->column(0)->GetScalar(1).ValueOrDie()));
}

TEST(WriteBatchStreamTest, RoundTripsMapsWithTypedKeysNullValuesAndEmptyEntries) {
  const auto keys = std::make_shared<arrow::Int32Builder>();
  const auto values = std::make_shared<arrow::StringBuilder>();
  arrow::MapBuilder builder(arrow::default_memory_pool(), keys, values);
  ASSERT_TRUE(builder.Append().ok());
  ASSERT_TRUE(keys->Append(-1).ok());
  ASSERT_TRUE(values->Append("value").ok());
  ASSERT_TRUE(keys->Append(2).ok());
  ASSERT_TRUE(values->AppendNull().ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  ASSERT_TRUE(builder.Append().ok());
  const auto maps = builder.Finish();
  ASSERT_TRUE(maps.ok()) << maps.status();
  const auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("entries", (*maps)->type())}), 3, {*maps});
  assertRoundTrip(*batch);
}

TEST(WriteBatchStreamTest, ZeroRowBatchStillContainsSchemaBatchAndEndOfStream) {
  const auto source = makeSourceBatch();
  ASSERT_TRUE(source.ok()) << source.status();
  assertRoundTrip(*(*source)->Slice(0, 0));
}

TEST(WriteBatchStreamTest, PreservesZeroColumnBatchRowCountsIncludingInt32Limit) {
  for (const int64_t rowCount : {int64_t{0}, int64_t{3},
                                 int64_t{std::numeric_limits<int32_t>::max()}}) {
    SCOPED_TRACE(rowCount);
    const auto batch = arrow::RecordBatch::Make(
        arrow::schema({}), rowCount, std::vector<std::shared_ptr<arrow::Array>>{});
    assertRoundTrip(*batch);
  }
}

TEST(WriteBatchStreamTest, EmitsUncompressedRecordBatchBody) {
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.Append(std::string(16384, 'x')).ok());
  const auto values = builder.Finish();
  ASSERT_TRUE(values.ok()) << values.status();
  const auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("compressible", arrow::utf8())}), 1, {*values});
  const auto serialized = encodeActualWriteBatch(*batch);
  ASSERT_TRUE(serialized.ok()) << serialized.status();
  const auto input = std::make_shared<arrow::io::BufferReader>(*serialized);
  const auto messages = arrow::ipc::MessageReader::Open(input);
  const auto schema = messages->ReadNextMessage();
  ASSERT_TRUE(schema.ok()) << schema.status();
  ASSERT_NE(*schema, nullptr);
  ASSERT_EQ((*schema)->type(), arrow::ipc::MessageType::SCHEMA);
  const auto body = messages->ReadNextMessage();
  ASSERT_TRUE(body.ok()) << body.status();
  ASSERT_NE(*body, nullptr);
  ASSERT_EQ((*body)->type(), arrow::ipc::MessageType::RECORD_BATCH);

  // Compare metadata and body with an explicitly uncompressed standalone message.
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.codec = nullptr;
  const auto bare = arrow::ipc::SerializeRecordBatch(*batch, options);
  ASSERT_TRUE(bare.ok()) << bare.status();
  const auto bareInput = std::make_shared<arrow::io::BufferReader>(*bare);
  const auto bareMessages = arrow::ipc::MessageReader::Open(bareInput);
  const auto expected = bareMessages->ReadNextMessage();
  ASSERT_TRUE(expected.ok()) << expected.status();
  ASSERT_NE(*expected, nullptr);
  EXPECT_TRUE((*body)->Equals(**expected));
}

TEST(WriteBatchStreamTest, StreamReaderIndependentlyRejectsBareRecordBatch) {
  const auto batch = makeSourceBatch();
  ASSERT_TRUE(batch.ok()) << batch.status();
  const auto bare = arrow::ipc::SerializeRecordBatch(
      **batch, arrow::ipc::IpcWriteOptions::Defaults());
  ASSERT_TRUE(bare.ok()) << bare.status();
  const auto input = std::make_shared<arrow::io::BufferReader>(*bare);

  // This is the reader itself rejecting missing schema, not a production guard.
  const auto opened = arrow::ipc::RecordBatchStreamReader::Open(input);
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.status().IsIOError()) << opened.status();
  EXPECT_NE(opened.status().ToString().find("Expected IPC message of type schema"),
            std::string::npos);
}

TEST(WriteBatchStreamTest, RejectsMismatchedRowCountsAndColumnTypes) {
  const auto source = makeSourceBatch();
  ASSERT_TRUE(source.ok()) << source.status();
  const auto wrongLength = arrow::RecordBatch::Make(
      (*source)->schema(), 3, (*source)->columns());
  const auto wrongTypes = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("name", arrow::int64()),
                     arrow::field("id", arrow::utf8())}),
      4, (*source)->columns());
  for (const auto& batch : {wrongLength, wrongTypes}) {
    const auto result = encodeActualWriteBatch(*batch);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsInvalid()) << result.status();
  }
}

TEST(WriteBatchStreamTest, RejectsNegativeOrOverflowingRowCounts) {
  for (const int64_t rowCount : {int64_t{-1},
                                 int64_t{std::numeric_limits<int32_t>::max()} + 1}) {
    SCOPED_TRACE(rowCount);
    const auto batch = arrow::RecordBatch::Make(
        arrow::schema({}), rowCount, std::vector<std::shared_ptr<arrow::Array>>{});
    const auto result = encodeActualWriteBatch(*batch);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsInvalid()) << result.status();
  }
}

TEST(WriteBatchStreamTest, RejectsInvalidUtf8Values) {
  arrow::StringBuilder builder;
  ASSERT_TRUE(builder.Append(std::string(1, static_cast<char>(0xff))).ok());
  const auto values = builder.Finish();
  ASSERT_TRUE(values.ok()) << values.status();
  const auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("text", arrow::utf8())}), 1, {*values});
  const auto result = encodeActualWriteBatch(*batch);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid()) << result.status();
}

TEST(WriteBatchStreamTest, RejectsUnflattenedDictionariesIncludingNestedFields) {
  arrow::StringDictionaryBuilder builder;
  ASSERT_TRUE(builder.Append("value").ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  const auto dictionary = builder.Finish();
  ASSERT_TRUE(dictionary.ok()) << dictionary.status();
  ASSERT_EQ((*dictionary)->type_id(), arrow::Type::DICTIONARY);
  const auto field = arrow::field("encoded", (*dictionary)->type());
  const auto nested = arrow::StructArray::Make({*dictionary}, arrow::FieldVector{field});
  ASSERT_TRUE(nested.ok()) << nested.status();
  const auto plainBatch = arrow::RecordBatch::Make(arrow::schema({field}), 2, {*dictionary});
  const auto nestedBatch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("payload", (*nested)->type())}), 2, {*nested});
  for (const auto& batch : {plainBatch, nestedBatch}) {
    const auto result = encodeActualWriteBatch(*batch);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsInvalid()) << result.status();
  }
}

}  // namespace
}  // namespace pangrova::write::test
