// ShuffleBatchSerializerTest: RowVector serialize/deserialize round-trip correctness.
// Validates: Requirements Req-1, Req-4

#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "shuffle/ShuffleBatchSerializer.h"

#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::shuffle {
namespace {

class ShuffleBatchSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testRootPool("ShuffleBatchSerializerTest");
    leafPool_ = pool_->addLeafChild("leaf");
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> leafPool_;
};

TEST_F(ShuffleBatchSerializerTest, SerializeDeserializeRoundTrip) {
  ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{"none", 10000});
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  auto intVector = maker.flatVector<int32_t>({1, 2, 3, 4, 5});
  auto strVector = maker.flatVector<facebook::velox::StringView>(
      {"hello", "world", "foo", "bar", "baz"});

  auto rowType = facebook::velox::ROW(
      {"col_int", "col_str"},
      {facebook::velox::INTEGER(), facebook::velox::VARCHAR()});
  auto rowVector = maker.rowVector(
      {"col_int", "col_str"}, {intVector, strVector});

  auto serialized = writer.serialize(rowVector, leafPool_.get());
  ASSERT_NE(serialized, nullptr);
  ASSERT_GT(serialized->computeChainDataLength(), 0);

  auto deserialized = writer.deserialize(std::move(*serialized), rowType, leafPool_.get());
  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), 5);

  auto resultInt = deserialized->childAt(0)->asFlatVector<int32_t>();
  auto resultStr = deserialized->childAt(1)->asFlatVector<facebook::velox::StringView>();

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(resultInt->valueAt(i), intVector->valueAt(i));
    EXPECT_EQ(resultStr->valueAt(i), strVector->valueAt(i));
  }
}

TEST_F(ShuffleBatchSerializerTest, EmptyRowVector) {
  ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{"none", 10000});
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  auto rowType = facebook::velox::ROW(
      {"col_int"}, {facebook::velox::INTEGER()});
  auto emptyInt = maker.flatVector<int32_t>({});
  auto rowVector = maker.rowVector({"col_int"}, {emptyInt});

  auto serialized = writer.serialize(rowVector, leafPool_.get());
  ASSERT_NE(serialized, nullptr);

  auto deserialized = writer.deserialize(std::move(*serialized), rowType, leafPool_.get());
  ASSERT_NE(deserialized, nullptr);
  EXPECT_EQ(deserialized->size(), 0);
}

TEST_F(ShuffleBatchSerializerTest, NullValues) {
  ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{"none", 10000});
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  auto intVector = maker.flatVectorNullable<int32_t>(
      {1, std::nullopt, 3, std::nullopt, 5});
  auto strVector = maker.flatVectorNullable<facebook::velox::StringView>(
      {"hello", std::nullopt, "foo", std::nullopt, "baz"});

  auto rowType = facebook::velox::ROW(
      {"col_int", "col_str"},
      {facebook::velox::INTEGER(), facebook::velox::VARCHAR()});
  auto rowVector = maker.rowVector(
      {"col_int", "col_str"}, {intVector, strVector});

  auto serialized = writer.serialize(rowVector, leafPool_.get());
  ASSERT_NE(serialized, nullptr);

  auto deserialized = writer.deserialize(std::move(*serialized), rowType, leafPool_.get());
  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), 5);

  auto resultInt = deserialized->childAt(0);
  auto resultStr = deserialized->childAt(1);

  EXPECT_FALSE(resultInt->isNullAt(0));
  EXPECT_TRUE(resultInt->isNullAt(1));
  EXPECT_FALSE(resultInt->isNullAt(2));
  EXPECT_TRUE(resultInt->isNullAt(3));
  EXPECT_FALSE(resultInt->isNullAt(4));

  EXPECT_FALSE(resultStr->isNullAt(0));
  EXPECT_TRUE(resultStr->isNullAt(1));
  EXPECT_FALSE(resultStr->isNullAt(2));
  EXPECT_TRUE(resultStr->isNullAt(3));
  EXPECT_FALSE(resultStr->isNullAt(4));

  EXPECT_EQ(resultInt->asFlatVector<int32_t>()->valueAt(0), 1);
  EXPECT_EQ(resultInt->asFlatVector<int32_t>()->valueAt(4), 5);
}

TEST_F(ShuffleBatchSerializerTest, LargeRowVector) {
  ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{"none", 10000});
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  constexpr int kNumRows = 10000;
  std::vector<int64_t> values(kNumRows);
  for (int i = 0; i < kNumRows; ++i) {
    values[i] = static_cast<int64_t>(i) * 100;
  }

  auto bigintVector = maker.flatVector<int64_t>(values);
  auto rowType = facebook::velox::ROW(
      {"col_bigint"}, {facebook::velox::BIGINT()});
  auto rowVector = maker.rowVector({"col_bigint"}, {bigintVector});

  auto serialized = writer.serialize(rowVector, leafPool_.get());
  ASSERT_NE(serialized, nullptr);

  auto deserialized = writer.deserialize(std::move(*serialized), rowType, leafPool_.get());
  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), kNumRows);

  auto result = deserialized->childAt(0)->asFlatVector<int64_t>();
  for (int i = 0; i < kNumRows; ++i) {
    EXPECT_EQ(result->valueAt(i), values[i]);
  }
}

TEST_F(ShuffleBatchSerializerTest, MultipleTypes) {
  ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{"none", 10000});
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  auto bigintVec = maker.flatVector<int64_t>({100L, 200L, 300L});
  auto doubleVec = maker.flatVector<double>({1.1, 2.2, 3.3});
  auto boolVec = maker.flatVector<bool>({true, false, true});
  auto varcharVec = maker.flatVector<facebook::velox::StringView>(
      {"alpha", "beta", "gamma"});

  auto rowType = facebook::velox::ROW(
      {"col_bigint", "col_double", "col_bool", "col_varchar"},
      {facebook::velox::BIGINT(), facebook::velox::DOUBLE(),
       facebook::velox::BOOLEAN(), facebook::velox::VARCHAR()});
  auto rowVector = maker.rowVector(
      {"col_bigint", "col_double", "col_bool", "col_varchar"},
      {bigintVec, doubleVec, boolVec, varcharVec});

  auto serialized = writer.serialize(rowVector, leafPool_.get());
  ASSERT_NE(serialized, nullptr);

  auto deserialized = writer.deserialize(std::move(*serialized), rowType, leafPool_.get());
  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(deserialized->size(), 3);

  auto resBigint = deserialized->childAt(0)->asFlatVector<int64_t>();
  auto resDouble = deserialized->childAt(1)->asFlatVector<double>();
  auto resBool = deserialized->childAt(2)->asFlatVector<bool>();
  auto resVarchar = deserialized->childAt(3)->asFlatVector<facebook::velox::StringView>();

  EXPECT_EQ(resBigint->valueAt(0), 100L);
  EXPECT_EQ(resBigint->valueAt(2), 300L);
  EXPECT_DOUBLE_EQ(resDouble->valueAt(1), 2.2);
  EXPECT_EQ(resBool->valueAt(0), true);
  EXPECT_EQ(resBool->valueAt(1), false);
  EXPECT_EQ(resVarchar->valueAt(2).str(), "gamma");
}

}  // namespace
}  // namespace pangrova::shuffle
