#include <gtest/gtest.h>
#include <arrow/record_batch.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include "engine/columnar/CanonicalBatchStream.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <stdexcept>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;
namespace schema = pangrova::velox::schema;

ArrowBatchVectorSerde::StreamOptions declaredOptions(const std::vector<schema::Field>& fields) {
  ArrowBatchVectorSerde::StreamOptions options;
  options.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named(fields));
  return options;
}

ArrowBatchVectorSerde::StreamOptions integerOptions(const std::string& name) {
  return declaredOptions({schema::computed(name,
      schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE))});
}

// Real SDK append API and official Arrow streams, not a distributed task test.
class ArrowBatchAppendTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("arrow_append");
    options_ = integerOptions("id");
  }

  RowVectorPtr values(const std::vector<std::optional<int64_t>>& input) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    return maker.rowVector({"id"}, {maker.flatVectorNullable<int64_t>(input)});
  }

  std::unique_ptr<folly::IOBuf> encode(const std::vector<RowVectorPtr>& batches) {
    return encode(batches, options_);
  }

  std::unique_ptr<folly::IOBuf> encode(const std::vector<RowVectorPtr>& batches,
      const ArrowBatchVectorSerde::StreamOptions& options) {
    StreamArena arena(pool_.get());
    auto writer = serde_.createIterativeSerializer(
        std::dynamic_pointer_cast<const RowType>(batches.front()->type()), 0, &arena, &options);
    for (const auto& batch : batches) writer->append(batch);
    IOBufOutputStream stream(*pool_);
    writer->flush(&stream);
    return stream.getIOBuf();
  }

  void append(const RowVectorPtr& batch, RowVectorPtr& result, vector_size_t offset) {
    auto wire = encode({batch});
    BufferInputStream input(byteRangesFromIOBuf(wire.get()));
    // Select precisely the SDK overload used by Exchange::getOutput.
    static_cast<VectorSerde&>(serde_).deserialize(&input, pool_.get(),
        std::dynamic_pointer_cast<const RowType>(batch->type()), &result, offset, &options_);
    EXPECT_TRUE(input.atEnd());
  }

  void checkValues(const RowVectorPtr& actual,
      const std::vector<std::optional<int64_t>>& expected) {
    const auto reference = values(expected);
    ASSERT_EQ(actual->size(), reference->size());
    ASSERT_EQ(actual->type()->asRow().names(), reference->type()->asRow().names());
    for (vector_size_t row = 0; row < actual->size(); ++row) {
      EXPECT_TRUE(actual->equalValueAt(reference.get(), row, row)) << row;
    }
  }

  void reject(BufferInputStream& input, const RowTypePtr& type,
      RowVectorPtr& result, vector_size_t offset, const std::string& reason) {
    try {
      static_cast<VectorSerde&>(serde_).deserialize(&input, pool_.get(), type, &result, offset, &options_);
      FAIL() << "Expected explicit append rejection: " << reason;
    } catch (const VeloxException& error) {
      EXPECT_NE(std::string(error.what()).find(reason), std::string::npos) << error.what();
    }
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  pangrova::velox::ArrowBatchVectorSerde serde_;
  ArrowBatchVectorSerde::StreamOptions options_;
};

TEST_F(ArrowBatchAppendTest, SerializesFourMiBPageWithTwoMiBOfScratchCapacity) {
  constexpr vector_size_t rows = 524288;
  constexpr int64_t bodyBytes = rows * sizeof(int64_t);
  auto serializationTask = pangrova::test::testMemoryManager()->addRootPool(
      "arrow-direct-page-task", bodyBytes / 2);
  auto serializationPool = serializationTask->addLeafChild("serialization");
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto ids = maker.flatVector<int64_t>(rows, [](auto row) { return row * 3; });
  auto input = maker.rowVector({"id"}, {ids});
  const auto type = std::dynamic_pointer_cast<const RowType>(input->type());
  const IndexRange range{0, rows};
  Scratch scratch;
  IOBufOutputStream batchOutput(*pool_);
  auto batchWriter = serde_.createBatchSerializer(serializationPool.get(), &options_);
  ASSERT_NO_THROW(batchWriter->serialize(input, folly::Range(&range, 1), scratch, &batchOutput));
  EXPECT_GT(static_cast<int64_t>(batchOutput.tellp()), bodyBytes);
  EXPECT_EQ(serializationPool->usedBytes(), 0);

  StreamArena serializationArena(serializationPool.get());
  auto iterative = serde_.createIterativeSerializer(type, 0, &serializationArena, &options_);
  iterative->append(input);
  size_t pageSize = 0;
  ASSERT_NO_THROW(pageSize = iterative->maxSerializedSize());
  EXPECT_GT(pageSize, bodyBytes);
  IOBufOutputStream iterativeOutput(*pool_);
  ASSERT_NO_THROW(iterative->flush(&iterativeOutput));
  EXPECT_EQ(static_cast<size_t>(iterativeOutput.tellp()), pageSize);
  EXPECT_EQ(serializationPool->usedBytes(), 0);
  EXPECT_EQ(iterative->maxSerializedSize(), 0);

  for (auto* output : {&batchOutput, &iterativeOutput}) {
    auto wire = output->getIOBuf();
    BufferInputStream stream(byteRangesFromIOBuf(wire.get()));
    RowVectorPtr decoded;
    serde_.deserialize(&stream, pool_.get(), type, &decoded, &options_);
    ASSERT_NE(decoded, nullptr);
    ASSERT_EQ(decoded->size(), rows);
    EXPECT_TRUE(decoded->equalValueAt(input.get(), 0, 0));
    EXPECT_TRUE(decoded->equalValueAt(input.get(), rows - 1, rows - 1));
    EXPECT_TRUE(stream.atEnd());
  }
}

TEST_F(ArrowBatchAppendTest, WireLimitLeavesBothVeloxOutputsUntouched) {
  options_.maximum_bytes = 1024;
  const auto input = values(std::vector<std::optional<int64_t>>(8192, 7));
  const auto type = std::dynamic_pointer_cast<const RowType>(input->type());
  const IndexRange range{0, input->size()};
  Scratch scratch;
  IOBufOutputStream stream(*pool_);
  auto batchWriter = serde_.createBatchSerializer(pool_.get(), &options_);
  EXPECT_THROW(batchWriter->serialize(input, folly::Range(&range, 1), scratch, &stream),
      ArrowBatchWireLimitException);
  EXPECT_EQ(stream.tellp(), 0);

  StreamArena arena(pool_.get());
  auto iterative = serde_.createIterativeSerializer(type, 0, &arena, &options_);
  iterative->append(input);
  EXPECT_THROW(iterative->maxSerializedSize(), ArrowBatchWireLimitException);
  EXPECT_THROW(iterative->flush(&stream), ArrowBatchWireLimitException);
  EXPECT_EQ(stream.tellp(), 0);
  iterative->clear();
  iterative->append(values({42}));
  ASSERT_NO_THROW(iterative->flush(&stream));
  EXPECT_GT(stream.tellp(), 0);
}

TEST_F(ArrowBatchAppendTest, AppendsTwoActualPagesWithoutChangingPublishedPrefix) {
  EXPECT_TRUE(serde_.supportsAppendInDeserialize());
  RowVectorPtr result;
  append(values({10, std::nullopt, -3}), result, 0);
  const auto published = result;
  append(values({std::nullopt, 42}), result, 3);
  checkValues(result, {10, std::nullopt, -3, std::nullopt, 42});
  checkValues(published, {10, std::nullopt, -3});
}

TEST_F(ArrowBatchAppendTest, AppendsEveryBatchOfOneCanonicalStream) {
  auto result = values({7});
  auto wire = encode({values({8, std::nullopt}), values({9, 10})});
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  static_cast<VectorSerde&>(serde_).deserialize(&input, pool_.get(),
      std::dynamic_pointer_cast<const RowType>(result->type()), &result, 1, &options_);
  EXPECT_TRUE(input.atEnd());
  checkValues(result, {7, 8, std::nullopt, 9, 10});
}

TEST_F(ArrowBatchAppendTest, PreservesOnlyTheRequestedPrefixNotOldTail) {
  auto result = values({1, 2, 3, 4});
  const auto published = result;
  append(values({99}), result, 2);
  checkValues(result, {1, 2, 99});
  checkValues(published, {1, 2, 3, 4});
}

TEST_F(ArrowBatchAppendTest, ZeroColumnRowsKeepTheirCardinalityAcrossPages) {
  options_ = declaredOptions({});
  const auto type = ROW(std::vector<std::string>{}, std::vector<TypePtr>{});
  auto first = std::make_shared<RowVector>(pool_.get(), type, nullptr, 3, std::vector<VectorPtr>{});
  auto second = std::make_shared<RowVector>(pool_.get(), type, nullptr, 5, std::vector<VectorPtr>{});
  RowVectorPtr result;
  append(first, result, 0);
  append(second, result, 3);
  ASSERT_EQ(result->size(), 8);
  EXPECT_EQ(result->childrenSize(), 0);
}

TEST_F(ArrowBatchAppendTest, InvalidOffsetsFailBeforeConsumingInputOrChangingResult) {
  const auto batch = values({2});
  for (const auto offset : {-1, 2}) {
    auto result = values({1});
    const auto original = result;
    auto wire = encode({batch});
    BufferInputStream input(byteRangesFromIOBuf(wire.get()));
    const auto remaining = input.remainingSize();
    reject(input, std::dynamic_pointer_cast<const RowType>(batch->type()), result, offset,
        "Arrow append result offset");
    EXPECT_EQ(input.remainingSize(), remaining);
    EXPECT_EQ(result, original);
  }
}

TEST_F(ArrowBatchAppendTest, NonzeroOffsetRequiresAnExistingOutput) {
  auto batch = values({2});
  auto wire = encode({batch});
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  reject(input, std::dynamic_pointer_cast<const RowType>(batch->type()), result, 1,
      "Arrow append nonzero offset requires an existing output");
  EXPECT_EQ(result, nullptr);
}

TEST_F(ArrowBatchAppendTest, ExactInputSchemaMismatchDoesNotOverwritePublishedOutput) {
  auto result = values({1});
  const auto original = result;
  const auto wrong = std::make_shared<RowVector>(pool_.get(), ROW({"renamed"}, {BIGINT()}),
      nullptr, 1, std::vector<VectorPtr>{values({2})->childAt(0)});
  auto wire = encode({wrong}, integerOptions("renamed"));
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  reject(input, std::dynamic_pointer_cast<const RowType>(original->type()), result, 1,
      "Arrow wire schema differs from its admitted canonical schema");
  EXPECT_EQ(result, original);
  checkValues(result, {1});
}

TEST_F(ArrowBatchAppendTest, TruncatedCommittedStreamDoesNotOverwritePublishedOutput) {
  auto result = values({1});
  const auto original = result;
  auto wire = encode({values({2})});
  wire->coalesce();
  wire->trimEnd(1);
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  reject(input, std::dynamic_pointer_cast<const RowType>(result->type()), result, 1,
      "missing its committed Arrow EOF marker");
  EXPECT_EQ(result, original);
  checkValues(result, {1});
}

TEST_F(ArrowBatchAppendTest, TotalRowOverflowFailsWithoutAllocatingOrChangingThePrefix) {
  options_ = declaredOptions({});
  const auto type = ROW(std::vector<std::string>{}, std::vector<TypePtr>{});
  const auto maximum = std::numeric_limits<vector_size_t>::max();
  auto result = std::make_shared<RowVector>(pool_.get(), type, nullptr, maximum, std::vector<VectorPtr>{});
  const auto original = result;
  auto next = std::make_shared<RowVector>(pool_.get(), type, nullptr, 1, std::vector<VectorPtr>{});
  auto wire = encode({next});
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  reject(input, type, result, maximum, "Arrow append total rows exceed vector row capacity");
  EXPECT_EQ(result, original);
  EXPECT_EQ(result->size(), maximum);
}

TEST_F(ArrowBatchAppendTest, IncomingInt64CardinalityIsRejectedBeforeSdkNarrowing) {
  options_ = declaredOptions({});
  const auto type = ROW(std::vector<std::string>{}, std::vector<TypePtr>{});
  auto result = std::make_shared<RowVector>(pool_.get(), type, nullptr, 1, std::vector<VectorPtr>{});
  const auto original = result;
  auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
  const auto rows = static_cast<int64_t>(std::numeric_limits<vector_size_t>::max()) + 1;
  auto encoded = columnar::encodeBatchStream(
      {arrow::RecordBatch::Make(arrow::schema({}), rows, arrow::ArrayVector{})}, allocator);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto wire = folly::IOBuf::copyBuffer((*encoded)->data(), (*encoded)->size());
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  reject(input, type, result, 0, "Arrow append total rows exceed vector row capacity");
  EXPECT_EQ(result, original);
  EXPECT_EQ(result->size(), 1);
}

TEST_F(ArrowBatchAppendTest, ExistingSchemaMismatchFailsBeforeConsumingInput) {
  const auto batch = values({2});
  auto result = std::make_shared<RowVector>(pool_.get(), ROW({"renamed"}, {BIGINT()}),
      nullptr, 1, std::vector<VectorPtr>{values({1})->childAt(0)});
  const auto original = result;
  auto wire = encode({batch});
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  const auto remaining = input.remainingSize();
  reject(input, std::dynamic_pointer_cast<const RowType>(batch->type()), result, 1,
      "Arrow append output schema differs from task schema");
  EXPECT_EQ(input.remainingSize(), remaining);
  EXPECT_EQ(result, original);
}

TEST_F(ArrowBatchAppendTest, ZeroOffsetReplacesOutputWithoutMutatingItsPublishedOwner) {
  auto result = values({1, 2, 3});
  const auto original = result;
  append(values({std::nullopt, 4}), result, 0);
  checkValues(result, {std::nullopt, 4});
  checkValues(original, {1, 2, 3});
}

TEST_F(ArrowBatchAppendTest, AppendedStringsNestedValuesAndNullsOutliveBothWirePages) {
  substrait::Type items;
  items.mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *items.mutable_list()->mutable_type() = schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED);
  options_ = declaredOptions({
      schema::computed("text", schema::primitive("string", substrait::Type::NULLABILITY_NULLABLE)),
      schema::computed("items", items)});
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto first = maker.rowVector({"text", "items"}, {
      maker.flatVectorNullable<std::string>({std::string("多字节"), std::nullopt}),
      maker.arrayVector<int64_t>(std::vector<std::vector<int64_t>>{{1, 2}, {}})});
  auto nested = maker.arrayVector<int64_t>(std::vector<std::vector<int64_t>>{{3}, {4, 5}});
  nested->setNull(1, true);
  auto second = maker.rowVector({"text", "items"}, {
      maker.flatVector<std::string>({std::string(200, 'x'), std::string("空")}), nested});
  RowVectorPtr result;
  append(first, result, 0);
  const auto published = result;
  append(second, result, 2);
  ASSERT_EQ(result->size(), 4);
  for (vector_size_t row = 0; row < 2; ++row) {
    EXPECT_TRUE(result->equalValueAt(first.get(), row, row));
    EXPECT_TRUE(result->equalValueAt(second.get(), row + 2, row));
    EXPECT_TRUE(published->equalValueAt(first.get(), row, row));
  }
}

TEST_F(ArrowBatchAppendTest, SparseRowsPreserveOrderDuplicatesNullsAndOriginalOwner) {
  auto input = values({10, std::nullopt, 30, 40, 50});
  StreamArena arena(pool_.get());
  auto writer = serde_.createIterativeSerializer(
      std::dynamic_pointer_cast<const RowType>(input->type()), 0, &arena, &options_);
  ASSERT_TRUE(writer->supportsAppendRows());
  std::vector<vector_size_t> selected{4, 1, 4, 0};
  Scratch scratch;
  writer->append(input, folly::Range<const vector_size_t*>(selected.data(), selected.size()), scratch);
  IOBufOutputStream output(*pool_);
  writer->flush(&output);
  auto wire = output.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  serde_.deserialize(&source, pool_.get(), std::dynamic_pointer_cast<const RowType>(input->type()), &result, &options_);
  wire.reset();
  checkValues(result, {50, std::nullopt, 50, 10});
  checkValues(input, {10, std::nullopt, 30, 40, 50});
}

TEST_F(ArrowBatchAppendTest, FailedLateSelectionDoesNotAppendAnUncommittedPrefix) {
  auto original = values({7, std::nullopt});
  auto next = values({10, 20, 30});
  StreamArena arena(pool_.get());
  auto writer = serde_.createIterativeSerializer(
      std::dynamic_pointer_cast<const RowType>(original->type()), 0, &arena, &options_);
  writer->append(original);
  const auto committedSize = writer->maxSerializedSize();
  std::vector<IndexRange> invalid{{0, 1}, {3, 1}};
  Scratch scratch;
  EXPECT_THROW(writer->append(next,
      folly::Range<const IndexRange*>(invalid.data(), invalid.size()), scratch), VeloxException);
  EXPECT_EQ(writer->maxSerializedSize(), committedSize);
  IOBufOutputStream output(*pool_);
  writer->flush(&output);
  auto wire = output.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  serde_.deserialize(&source, pool_.get(),
      std::dynamic_pointer_cast<const RowType>(original->type()), &result, &options_);
  EXPECT_TRUE(source.atEnd());
  checkValues(result, {7, std::nullopt});
}

TEST_F(ArrowBatchAppendTest, SizeEstimatePointersAreSelectionPositionsNotSourceRows) {
  auto input = values({10, 20, 30, 40, 50});
  std::vector<vector_size_t> rows{4, 1, 4};
  vector_size_t rowSizes[]{7, 11, 13};
  vector_size_t* rowPointers[]{&rowSizes[0], &rowSizes[1], &rowSizes[2]};
  Scratch scratch;
  serde_.estimateSerializedSize(input.get(),
      folly::Range<const vector_size_t*>(rows.data(), rows.size()), rowPointers, scratch);
  EXPECT_GT(rowSizes[0], 7);
  EXPECT_EQ(rowSizes[0] - 7, rowSizes[1] - 11);
  EXPECT_EQ(rowSizes[1] - 11, rowSizes[2] - 13);
  const IndexRange ranges[]{{3, 2}, {0, 1}};
  vector_size_t canaries[]{123456, 5, 9, 654321};
  vector_size_t* rangePointers[]{&canaries[1], &canaries[2]};
  auto batchWriter = serde_.createBatchSerializer(pool_.get(), &options_);
  batchWriter->estimateSerializedSize(input, folly::Range<const IndexRange*>(ranges, 2), rangePointers, scratch);
  EXPECT_EQ(canaries[0], 123456);
  EXPECT_EQ(canaries[3], 654321);
  EXPECT_GT(canaries[2], 9);
  EXPECT_EQ(canaries[1] - 5, 2 * (canaries[2] - 9));
}

TEST_F(ArrowBatchAppendTest, NestedFieldNameDriftCannotPassWeakTypeEquivalence) {
  substrait::Type nestedType;
  nestedType.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *nestedType.mutable_struct_()->add_types() = schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED);
  const auto sourceOptions = declaredOptions({{"outer", nestedType, {"source"}}});
  options_ = declaredOptions({{"outer", nestedType, {"different"}}});
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto nested = maker.rowVector({"source"}, {maker.flatVector<int64_t>({42})});
  auto input = maker.rowVector({"outer"}, {nested});
  auto wire = encode({input}, sourceOptions);
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  const auto expected = ROW({"outer"}, {ROW({"different"}, {BIGINT()})});
  ASSERT_TRUE(input->type()->equivalent(*expected));
  RowVectorPtr result;
  reject(source, expected, result, 0, "Arrow wire schema differs from its admitted canonical schema");
  EXPECT_EQ(result, nullptr);
}

TEST_F(ArrowBatchAppendTest, BatchSerializerPreservesNonzeroRangesDuplicatesAndNulls) {
  auto input = values({10, std::nullopt, 30, 40, 50});
  auto writer = serde_.createBatchSerializer(pool_.get(), &options_);
  const IndexRange ranges[]{{3, 2}, {1, 1}, {3, 1}};
  IOBufOutputStream output(*pool_);
  writer->serialize(input, folly::Range<const IndexRange*>(ranges, 3), &output);
  auto wire = output.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr result;
  serde_.deserialize(&source, pool_.get(), std::dynamic_pointer_cast<const RowType>(input->type()), &result, &options_);
  checkValues(result, {40, 50, std::nullopt, 40});
  checkValues(input, {10, std::nullopt, 30, 40, 50});
}

TEST_F(ArrowBatchAppendTest, LateRequiredNullDoesNotPublishTheEarlierValidBatch) {
  options_ = declaredOptions({schema::computed("id",
      schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))});
  auto result = values({41});
  const auto published = result;
  auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
  const auto wireSchema = arrow::schema({arrow::field("id", arrow::int64(), false)});
  arrow::Int64Builder builder(allocator.get());
  ASSERT_TRUE(builder.Append(42).ok());
  std::shared_ptr<arrow::Array> first;
  ASSERT_TRUE(builder.Finish(&first).ok());
  ASSERT_TRUE(builder.AppendNull().ok());
  std::shared_ptr<arrow::Array> second;
  ASSERT_TRUE(builder.Finish(&second).ok());
  const auto encoded = columnar::encodeBatchStream({
      arrow::RecordBatch::Make(wireSchema, 1, {first}),
      arrow::RecordBatch::Make(wireSchema, 1, {second})}, allocator);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto wire = folly::IOBuf::copyBuffer((*encoded)->data(), (*encoded)->size());
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  EXPECT_THROW(static_cast<VectorSerde&>(serde_).deserialize(&input, pool_.get(),
      std::dynamic_pointer_cast<const RowType>(result->type()), &result, 1, &options_), std::runtime_error);
  EXPECT_EQ(result, published);
  checkValues(result, {41});
}

TEST_F(ArrowBatchAppendTest, EqualPhysicalTypesCannotHideWireNullabilityDrift) {
  auto result = values({41});
  const auto published = result;
  const auto required = declaredOptions({schema::computed("id",
      schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))});
  auto wire = encode({values({42})}, required);
  BufferInputStream input(byteRangesFromIOBuf(wire.get()));
  EXPECT_THROW(static_cast<VectorSerde&>(serde_).deserialize(&input, pool_.get(),
      std::dynamic_pointer_cast<const RowType>(result->type()), &result, 1, &options_), VeloxException);
  EXPECT_EQ(result, published);
  checkValues(result, {41});
}

TEST_F(ArrowBatchAppendTest, MapWireIgnoresUnreferencedNullKeysWithoutMutatingSource) {
  substrait::Type mapType;
  mapType.mutable_map()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *mapType.mutable_map()->mutable_key() = schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE);
  *mapType.mutable_map()->mutable_value() = schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE);
  options_ = declaredOptions({schema::computed("mapping", mapType)});
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool_.get());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool_.get());
  offsets->asMutable<vector_size_t>()[0] = 1;
  sizes->asMutable<vector_size_t>()[0] = 1;
  auto keys = maker.flatVectorNullable<int64_t>({std::nullopt, 17, std::nullopt});
  auto items = maker.flatVector<int64_t>({23, 31, 47});
  auto mapping = std::make_shared<MapVector>(pool_.get(), MAP(BIGINT(), BIGINT()),
      nullptr, 1, offsets, sizes, keys, items);
  auto source = maker.rowVector({"mapping"}, {mapping});
  RowVectorPtr result;
  append(source, result, 0);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->equalValueAt(source.get(), 0, 0));
  EXPECT_TRUE(keys->isNullAt(0));
  EXPECT_TRUE(keys->isNullAt(2));
  EXPECT_EQ(mapping->offsetAt(0), 1);
  EXPECT_EQ(keys->valueAt(1), 17);
}

TEST_F(ArrowBatchAppendTest, NullMapParentMasksStoredNullKeyAtActualWireBoundary) {
  substrait::Type mapType;
  mapType.mutable_map()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *mapType.mutable_map()->mutable_key() = schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE);
  *mapType.mutable_map()->mutable_value() = schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE);
  options_ = declaredOptions({schema::computed("mapping", mapType)});
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto offsets = AlignedBuffer::allocate<vector_size_t>(2, pool_.get());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(2, pool_.get());
  offsets->asMutable<vector_size_t>()[0] = 0;
  offsets->asMutable<vector_size_t>()[1] = 1;
  sizes->asMutable<vector_size_t>()[0] = 1;
  sizes->asMutable<vector_size_t>()[1] = 1;
  auto keys = maker.flatVectorNullable<int64_t>({std::nullopt, 17});
  auto mapping = std::make_shared<MapVector>(pool_.get(), MAP(BIGINT(), BIGINT()),
      nullptr, 2, offsets, sizes, keys, maker.flatVector<int64_t>({23, 31}));
  mapping->setNull(0, true);
  auto source = maker.rowVector({"mapping"}, {mapping});
  RowVectorPtr result;
  append(source, result, 0);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->equalValueAt(source.get(), 0, 0));
  EXPECT_TRUE(result->equalValueAt(source.get(), 1, 1));
  EXPECT_TRUE(keys->isNullAt(0));
  EXPECT_EQ(mapping->sizeAt(0), 1);
}
TEST_F(ArrowBatchAppendTest, ReusedNestedValuesBindCanonicalNamesBeforeWritingAndPreserveTheOwner) {
  substrait::Type nestedType;
  nestedType.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *nestedType.mutable_struct_()->add_types() = schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE);
  options_ = declaredOptions({{"outer", nestedType, {"field_0"}}});
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto child = maker.flatVectorNullable<int64_t>({41, std::nullopt});
  auto nested = maker.rowVector({"region"}, {child});
  auto input = maker.rowVector({"outer"}, {nested});
  auto wire = encode({input});
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  auto type = ROW({"outer"}, {ROW({"field_0"}, {BIGINT()})});
  RowVectorPtr result;
  serde_.deserialize(&source, pool_.get(), type, &result, &options_);
  ASSERT_EQ(result->size(), 2);
  auto* actual = result->childAt(0)->as<RowVector>();
  EXPECT_EQ(actual->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 41);
  EXPECT_TRUE(actual->childAt(0)->isNullAt(1));
  EXPECT_EQ(actual->type()->asRow().nameOf(0), "field_0");
  EXPECT_EQ(nested->type()->asRow().nameOf(0), "region");
  EXPECT_EQ(nested->childAt(0), child);
}

}  // namespace
}  // namespace pangrova::velox::test
