#include <gtest/gtest.h>
#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <cstring>
#include <limits>

#include "../TestMemoryHelper.h"
#include "columnar/CanonicalBatchStream.h"

namespace pangrova::columnar::test {
namespace {
using memory::ColumnarMemoryPool;

std::shared_ptr<arrow::RecordBatch> batch(
    const std::shared_ptr<ColumnarMemoryPool>& pool, const std::vector<int64_t>& values) {
  arrow::Int64Builder builder(pool.get());
  const auto appended = builder.AppendValues(values);
  EXPECT_TRUE(appended.ok()) << appended.ToString();
  auto array = builder.Finish();
  EXPECT_TRUE(array.ok()) << array.status().ToString();
  return memory::retainColumnarBatch(arrow::RecordBatch::Make(
      arrow::schema({arrow::field("id", arrow::int64())}), values.size(), {*array}), pool);
}

class CanonicalBatchStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    task_pool_ = pangrova::test::testRootPool("canonical-columnar-task");
    leaf_ = task_pool_->addLeafChild("columnar");
    pool_ = std::make_shared<ColumnarMemoryPool>(leaf_);
  }
  std::shared_ptr<facebook::velox::memory::MemoryPool> task_pool_, leaf_;
  std::shared_ptr<ColumnarMemoryPool> pool_;
};

TEST_F(CanonicalBatchStreamTest, PlainArrowStreamPreservesEveryBatchAndExactSchema) {
  auto encoded = encodeBatchStream({batch(pool_, {1, 2}), batch(pool_, {3})}, pool_);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = decodeBatchStream(*encoded, pool_);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_EQ(decoded->size(), 2);
  ASSERT_EQ((*decoded)[0]->num_rows(), 2);
  ASSERT_EQ((*decoded)[1]->num_rows(), 1);
  EXPECT_TRUE((*decoded)[0]->schema()->Equals(*(*decoded)[1]->schema(), true));
  auto first = std::static_pointer_cast<arrow::Int64Array>((*decoded)[0]->column(0));
  auto second = std::static_pointer_cast<arrow::Int64Array>((*decoded)[1]->column(0));
  EXPECT_EQ(first->Value(0), 1);
  EXPECT_EQ(first->Value(1), 2);
  EXPECT_EQ(second->Value(0), 3);
}

TEST_F(CanonicalBatchStreamTest, ZeroColumnBatchesRetainIndependentRowCounts) {
  auto schema = arrow::schema({});
  auto encoded = encodeBatchStream({arrow::RecordBatch::Make(schema, 2, arrow::ArrayVector{}),
      arrow::RecordBatch::Make(schema, 4, arrow::ArrayVector{})}, pool_);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto decoded = decodeBatchStream(*encoded, pool_);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_EQ(decoded->size(), 2);
  EXPECT_EQ((*decoded)[0]->num_columns(), 0);
  EXPECT_EQ((*decoded)[0]->num_rows(), 2);
  EXPECT_EQ((*decoded)[1]->num_rows(), 4);
}

TEST_F(CanonicalBatchStreamTest, RejectsSchemaDriftTruncationTrailingBytesAndPrivatePrefix) {
  auto original = batch(pool_, {1});
  auto renamed = arrow::RecordBatch::Make(arrow::schema({arrow::field("wrong", arrow::int64())}),
      original->num_rows(), original->columns());
  EXPECT_FALSE(encodeBatchStream({original, renamed}, pool_).ok());
  auto encoded = encodeBatchStream({original}, pool_);
  ASSERT_TRUE(encoded.ok());
  const auto& buffer = *encoded;
  EXPECT_FALSE(decodeBatchStream(arrow::SliceBuffer(buffer, 0, buffer->size() - 4), pool_).ok());
  auto allocation = arrow::AllocateBuffer(buffer->size() + 8, pool_.get());
  ASSERT_TRUE(allocation.ok());
  auto malformed = memory::retainColumnarBuffer(
      std::shared_ptr<arrow::Buffer>(std::move(*allocation)), pool_);
  std::memcpy(malformed->mutable_data(), buffer->data(), buffer->size());
  std::memcpy(malformed->mutable_data() + buffer->size(), buffer->data() + buffer->size() - 8, 8);
  EXPECT_FALSE(decodeBatchStream(malformed, pool_).ok());
  std::memset(malformed->mutable_data(), 0, 8);
  std::memcpy(malformed->mutable_data() + 8, buffer->data(), buffer->size());
  EXPECT_FALSE(decodeBatchStream(malformed, pool_).ok());
}

TEST_F(CanonicalBatchStreamTest, DecodedBuffersRetainAllocatorAndTaskAncestors) {
  std::weak_ptr<ColumnarMemoryPool> allocator;
  std::weak_ptr<facebook::velox::memory::MemoryPool> task;
  std::vector<std::shared_ptr<arrow::RecordBatch>> decoded;
  {
    auto root = pangrova::test::testRootPool("columnar-owned-task");
    auto leaf = root->addLeafChild("owned-columnar");
    auto pool = std::make_shared<ColumnarMemoryPool>(leaf);
    task = root;
    allocator = pool;
    auto encoded = encodeBatchStream({batch(pool, {42})}, pool);
    ASSERT_TRUE(encoded.ok());
    auto result = decodeBatchStream(*encoded, pool);
    ASSERT_TRUE(result.ok());
    decoded = std::move(*result);
  }
  EXPECT_FALSE(allocator.expired());
  EXPECT_FALSE(task.expired());
  EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(decoded[0]->column(0))->Value(0), 42);
  decoded.clear();
  EXPECT_TRUE(allocator.expired());
  EXPECT_TRUE(task.expired());
}

TEST_F(CanonicalBatchStreamTest, RealTaskCapRejectsAllocationWithoutLeakingUsage) {
  auto root = pangrova::test::testMemoryManager()->addRootPool("columnar-capped-task", 1024 * 1024);
  auto leaf = root->addLeafChild("capped-columnar");
  ColumnarMemoryPool allocator(leaf);
  uint8_t* data = nullptr;
  ASSERT_TRUE(allocator.Allocate(1024, 64, &data).ok());
  std::memset(data, 0x3a, 1024);
  ASSERT_TRUE(allocator.Reallocate(1024, 2048, 64, &data).ok());
  EXPECT_EQ(data[100], 0x3a);
  EXPECT_EQ(allocator.copiedBytes(), 1024);
  EXPECT_EQ(allocator.bytes_allocated(), 2048);
  uint8_t* rejected = nullptr;
  EXPECT_TRUE(allocator.Allocate(2 * 1024 * 1024, 64, &rejected).IsOutOfMemory());
  EXPECT_EQ(rejected, nullptr);
  EXPECT_EQ(allocator.bytes_allocated(), 2048);
  allocator.Free(data, 2048, 64);
  EXPECT_EQ(allocator.bytes_allocated(), 0);
  EXPECT_EQ(leaf->usedBytes(), 0);
}

TEST_F(CanonicalBatchStreamTest, StandardCompressedStreamsAreReadableWithoutPrivateCodecOptions) {
  const std::vector<int64_t> input(4096, 42);
  auto original = batch(pool_, input);
  auto plain = encodeBatchStream({original}, pool_);
  ASSERT_TRUE(plain.ok());
  for (const auto codec : {arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
    BatchStreamOptions config;
    config.compression = codec;
    auto encoded = encodeBatchStream({original}, pool_, config);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    EXPECT_LT((*encoded)->size(), (*plain)->size());
    auto readOptions = arrow::ipc::IpcReadOptions::Defaults();
    readOptions.memory_pool = pool_.get();
    readOptions.use_threads = false;
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(*encoded), readOptions);
    ASSERT_TRUE(reader.ok()) << reader.status().ToString();
    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_TRUE((*reader)->ReadNext(&actual).ok());
    ASSERT_NE(actual, nullptr);
    EXPECT_TRUE(actual->Equals(*original));
    ASSERT_TRUE((*reader)->ReadNext(&actual).ok());
    EXPECT_EQ(actual, nullptr);
  }
}

TEST_F(CanonicalBatchStreamTest, RejectsUnsupportedCodecInvalidSavingsAndNullBatch) {
  BatchStreamOptions config;
  config.compression = arrow::Compression::SNAPPY;
  EXPECT_TRUE(encodeBatchStream({batch(pool_, {1})}, pool_, config).status().IsInvalid());
  config.compression = arrow::Compression::UNCOMPRESSED;
  for (const auto savings : {-0.1, 1.1, std::numeric_limits<double>::quiet_NaN()}) {
    config.minimum_space_savings = savings;
    EXPECT_TRUE(encodeBatchStream({batch(pool_, {1})}, pool_, config).status().IsInvalid());
  }
  EXPECT_TRUE(encodeBatchStream({nullptr}, pool_).status().IsInvalid());
}

TEST_F(CanonicalBatchStreamTest, WireCeilingRejectsBeforeGrowingOutputAndReleasesAllocator) {
  const auto original = batch(pool_, std::vector<int64_t>(8192, 7));
  const auto baseline = pool_->bytes_allocated();
  BatchStreamOptions config;
  config.maximum_bytes = 1024;
  auto encoded = encodeBatchStream({original}, pool_, config);
  ASSERT_TRUE(encoded.status().IsCapacityError()) << encoded.status().ToString();
  EXPECT_EQ(pool_->bytes_allocated(), baseline);
}
}  // namespace
}  // namespace pangrova::columnar::test
