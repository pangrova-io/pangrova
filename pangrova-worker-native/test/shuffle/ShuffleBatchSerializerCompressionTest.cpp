// ShuffleBatchSerializerCompressionTest: LZ4/ZSTD/None compression verification.
// Validates: Requirements Req-1, Req-4

#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "shuffle/ShuffleBatchSerializer.h"
#include "velox/plan/CanonicalPlanSchema.h"

#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::shuffle {
namespace {

class ShuffleBatchSerializerCompressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testRootPool(
        "ShuffleBatchSerializerCompressionTest");
    leafPool_ = pool_->addLeafChild("leaf");
  }

  facebook::velox::RowVectorPtr makeCompressibleVector(int numRows) {
    facebook::velox::test::VectorMaker maker(leafPool_.get());
    std::vector<int64_t> values(numRows);
    std::vector<facebook::velox::StringView> strings(numRows);
    for (int i = 0; i < numRows; ++i) {
      values[i] = i % 100;
      strings[i] = facebook::velox::StringView("repeated_value_for_compression");
    }
    auto bigintVec = maker.flatVector<int64_t>(values);
    auto strVec = maker.flatVector<facebook::velox::StringView>(strings);
    return maker.rowVector(
        {"col_bigint", "col_str"}, {bigintVec, strVec});
  }

  facebook::velox::RowTypePtr rowType() {
    return facebook::velox::ROW(
        {"col_bigint", "col_str"},
        {facebook::velox::BIGINT(), facebook::velox::VARCHAR()});
  }

  ShuffleBatchSerializer::Config config(const std::string& compression) {
    namespace schema = pangrova::velox::schema;
    ShuffleBatchSerializer::Config result{compression, 10000};
    result.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named({
        schema::computed("col_bigint", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED)),
        schema::computed("col_str", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
    return result;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> leafPool_;
};

TEST_F(ShuffleBatchSerializerCompressionTest, Lz4CompressionReducesSize) {
  constexpr int kNumRows = 5000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer noneWriter(config("none"));
  auto noneBytes = noneWriter.serialize(vector, leafPool_.get());
  auto noneSize = noneBytes->computeChainDataLength();

  ShuffleBatchSerializer lz4Writer(config("lz4"));
  auto lz4Bytes = lz4Writer.serialize(vector, leafPool_.get());
  auto lz4Size = lz4Bytes->computeChainDataLength();

  EXPECT_LT(lz4Size, noneSize)
      << "LZ4 compressed size should be smaller than uncompressed";
}

TEST_F(ShuffleBatchSerializerCompressionTest, ZstdCompressionReducesSize) {
  constexpr int kNumRows = 5000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer noneWriter(config("none"));
  auto noneBytes = noneWriter.serialize(vector, leafPool_.get());
  auto noneSize = noneBytes->computeChainDataLength();

  ShuffleBatchSerializer zstdWriter(config("zstd"));
  auto zstdBytes = zstdWriter.serialize(vector, leafPool_.get());
  auto zstdSize = zstdBytes->computeChainDataLength();

  EXPECT_LT(zstdSize, noneSize)
      << "ZSTD compressed size should be smaller than uncompressed";
}

TEST_F(ShuffleBatchSerializerCompressionTest, NoneCompressionPreservesSize) {
  constexpr int kNumRows = 500;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer writer(config("none"));
  auto bytes = writer.serialize(vector, leafPool_.get());
  auto size = bytes->computeChainDataLength();

  EXPECT_GT(size, 0u);
}

TEST_F(ShuffleBatchSerializerCompressionTest, Lz4RoundTrip) {
  constexpr int kNumRows = 1000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer writer(config("lz4"));
  auto serialized = writer.serialize(vector, leafPool_.get());
  auto deserialized = writer.deserialize(std::move(*serialized), rowType(), leafPool_.get());

  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), kNumRows);

  auto result = deserialized->childAt(0)->asFlatVector<int64_t>();
  for (int i = 0; i < kNumRows; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 100);
  }
}

TEST_F(ShuffleBatchSerializerCompressionTest, ZstdRoundTrip) {
  constexpr int kNumRows = 1000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer writer(config("zstd"));
  auto serialized = writer.serialize(vector, leafPool_.get());
  auto deserialized = writer.deserialize(std::move(*serialized), rowType(), leafPool_.get());

  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), kNumRows);

  auto result = deserialized->childAt(0)->asFlatVector<int64_t>();
  for (int i = 0; i < kNumRows; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 100);
  }
}

TEST_F(ShuffleBatchSerializerCompressionTest, NoneRoundTrip) {
  constexpr int kNumRows = 1000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer writer(config("none"));
  auto serialized = writer.serialize(vector, leafPool_.get());
  auto deserialized = writer.deserialize(std::move(*serialized), rowType(), leafPool_.get());

  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), kNumRows);

  auto result = deserialized->childAt(0)->asFlatVector<int64_t>();
  for (int i = 0; i < kNumRows; ++i) {
    EXPECT_EQ(result->valueAt(i), i % 100);
  }
}

TEST_F(ShuffleBatchSerializerCompressionTest, ZstdGenerallySmallerThanLz4) {
  constexpr int kNumRows = 5000;
  auto vector = makeCompressibleVector(kNumRows);

  ShuffleBatchSerializer lz4Writer(config("lz4"));
  auto lz4Bytes = lz4Writer.serialize(vector, leafPool_.get());
  auto lz4Size = lz4Bytes->computeChainDataLength();

  ShuffleBatchSerializer zstdWriter(config("zstd"));
  auto zstdBytes = zstdWriter.serialize(vector, leafPool_.get());
  auto zstdSize = zstdBytes->computeChainDataLength();

  EXPECT_LE(zstdSize, lz4Size)
      << "ZSTD should generally produce smaller output than LZ4";
}

}  // namespace
}  // namespace pangrova::shuffle
