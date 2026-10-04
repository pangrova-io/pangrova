// ShuffleBatchSerializerTest: RowVector serialize/deserialize round-trip correctness.
// Validates: Requirements Req-1, Req-4

#include <gtest/gtest.h>
#include <stdexcept>

#include "../TestMemoryHelper.h"
#include "shuffle/ShuffleBatchSerializer.h"
#include "velox/plan/CanonicalPlanSchema.h"

#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::shuffle {
namespace {
namespace schema = pangrova::velox::schema;

ShuffleBatchSerializer::Config declaredConfig(
    ShuffleBatchSerializer::Config config, const std::vector<schema::Field>& fields) {
  config.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named(fields));
  return config;
}

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
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("col_int", schema::primitive("i32", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("col_str", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
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

TEST_F(ShuffleBatchSerializerTest, ReplicatedUnnestSourceArrayRoundTripsAcrossStage) {
  substrait::Type tagsType;
  tagsType.mutable_list()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *tagsType.mutable_list()->mutable_type() = schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED);
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("id", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("tags", tagsType),
      schema::computed("val", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  auto ids = maker.flatVector<int64_t>({1, 1, 2});
  auto tags = maker.arrayVector<facebook::velox::StringView>(
      {{"alpha", "beta"}, {"alpha", "beta"}, {"gamma"}});
  auto values = maker.flatVector<facebook::velox::StringView>(
      {"alpha", "beta", "gamma"});
  auto produced = maker.rowVector(
      {"id", "tags", "val"}, {ids, tags, values});
  auto expected = facebook::velox::ROW(
      {"id", "tags", "val"},
      {facebook::velox::BIGINT(),
       facebook::velox::ARRAY(facebook::velox::VARCHAR()),
       facebook::velox::VARCHAR()});

  auto serialized = writer.serialize(produced, leafPool_.get());
  ASSERT_NE(serialized, nullptr);
  auto consumed = writer.deserialize(
      std::move(*serialized), expected, leafPool_.get());
  ASSERT_NE(consumed, nullptr);
  ASSERT_EQ(consumed->size(), 3);
  auto sourceArrays = consumed->childAt(1)->as<facebook::velox::ArrayVector>();
  ASSERT_NE(sourceArrays, nullptr);
  EXPECT_EQ(sourceArrays->sizeAt(0), 2);
  EXPECT_EQ(sourceArrays->sizeAt(1), 2);
  EXPECT_EQ(sourceArrays->sizeAt(2), 1);
  auto elements = sourceArrays->elements()->asFlatVector<facebook::velox::StringView>();
  ASSERT_NE(elements, nullptr);
  EXPECT_EQ(elements->valueAt(sourceArrays->offsetAt(0)).str(), "alpha");
  EXPECT_EQ(elements->valueAt(sourceArrays->offsetAt(1) + 1).str(), "beta");
  EXPECT_EQ(elements->valueAt(sourceArrays->offsetAt(2)).str(), "gamma");
  EXPECT_EQ(consumed->childAt(2)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(1).str(), "beta");
}

TEST_F(ShuffleBatchSerializerTest, EmptyRowVector) {
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("col_int", schema::primitive("i32", substrait::Type::NULLABILITY_REQUIRED))}));
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
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("col_int", schema::primitive("i32", substrait::Type::NULLABILITY_NULLABLE)),
      schema::computed("col_str", schema::primitive("string", substrait::Type::NULLABILITY_NULLABLE))}));
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
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("col_bigint", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))}));
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
  ShuffleBatchSerializer writer(declaredConfig({"none", 10000}, {
      schema::computed("col_bigint", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("col_double", schema::primitive("fp64", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("col_bool", schema::primitive("bool", substrait::Type::NULLABILITY_REQUIRED)),
      schema::computed("col_varchar", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
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

TEST(ShuffleBatchSerializerBoundaryTest, PlansMoreThanFourMiBIntoBoundedBatches) {
  const std::vector<uint64_t> estimated_row_bytes(300, 1024 * 1024);
  const auto row_counts = planShuffleBatchRowCounts(
      estimated_row_bytes, kMaxCanonicalShuffleBatchBytes);

  ASSERT_EQ(row_counts.size(), 100);
  for (const auto rows : row_counts) EXPECT_EQ(rows, 3);
}

TEST(ShuffleBatchSerializerBoundaryTest, RejectsPageLimitAboveFlightContract) {
  ShuffleBatchSerializer::Config config;
  config.max_serialized_batch_bytes = kCanonicalShuffleBatchBytes + 1;
  EXPECT_THROW(ShuffleBatchSerializer writer(config), std::exception);
}

TEST(ShuffleBatchSerializerBoundaryTest, MissingContractCannotEnterEvenAnEmptyStream) {
  EXPECT_THROW(ShuffleBatchSerializer writer(ShuffleBatchSerializer::Config{}),
      facebook::velox::VeloxUserError);
}

TEST(ShuffleBatchSerializerBoundaryTest, EmptyStreamCannotHideUnspecifiedChildNullability) {
  ShuffleBatchSerializer::Config config;
  auto malformed = std::make_shared<substrait::NamedStruct>();
  malformed->add_names("value");
  malformed->mutable_struct_()->add_types()->mutable_i64();
  config.canonical_schema = malformed;
  EXPECT_THROW(ShuffleBatchSerializer writer(config), std::invalid_argument);
}

TEST_F(ShuffleBatchSerializerTest, SerializesRowsIntoBoundedCompletePages) {
  ShuffleBatchSerializer::Config config;
  config.compression_codec = "none";
  config.max_serialized_batch_bytes = 128 * 1024;
  ShuffleBatchSerializer writer(declaredConfig(config, {
      schema::computed("payload", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  std::vector<std::string> values(6, std::string(40 * 1024, 'v'));
  std::vector<facebook::velox::StringView> views;
  views.reserve(values.size());
  for (const auto& value : values) {
    views.emplace_back(value);
  }
  auto strings = maker.flatVector<facebook::velox::StringView>(views);
  auto row_vector = maker.rowVector({"payload"}, {strings});

  auto batches = writer.serializeBatches(row_vector, leafPool_.get());
  ASSERT_GT(batches.size(), 1);
  int64_t total_rows = 0;
  for (const auto& batch : batches) {
    ASSERT_NE(batch.data, nullptr);
    EXPECT_LE(batch.data->computeChainDataLength(),
              config.max_serialized_batch_bytes);
    EXPECT_GT(batch.row_count, 0);
    total_rows += batch.row_count;
  }
  EXPECT_EQ(total_rows, row_vector->size());
}

TEST_F(ShuffleBatchSerializerTest, RejectsOneRowWiderThanBatchLimit) {
  ShuffleBatchSerializer::Config config;
  config.compression_codec = "none";
  config.max_serialized_batch_bytes = 128 * 1024;
  ShuffleBatchSerializer writer(declaredConfig(config, {
      schema::computed("payload", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());

  const std::string value(129 * 1024, 'w');
  auto strings = maker.flatVector<facebook::velox::StringView>(
      {facebook::velox::StringView(value)});
  auto row_vector = maker.rowVector({"payload"}, {strings});

  EXPECT_THROW(writer.serializeBatches(row_vector, leafPool_.get()),
               facebook::velox::VeloxUserError);
}

TEST_F(ShuffleBatchSerializerTest, ActualArrowRowBelowLimitIsNotRejectedByEstimatedOverhead) {
  ShuffleBatchSerializer::Config config;
  config.max_serialized_batch_bytes = 128 * 1024;
  ShuffleBatchSerializer writer(declaredConfig(config, {
      schema::computed("payload", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());
  auto input = maker.rowVector({"payload"}, {maker.flatVector<std::string>({std::string(96 * 1024, 'w')})});
  auto pages = writer.serializeBatches(input, leafPool_.get());
  ASSERT_EQ(pages.size(), 1);
  EXPECT_LE(pages[0].data->computeChainDataLength(), config.max_serialized_batch_bytes);
  auto actual = writer.deserialize(std::move(*pages[0].data),
      std::dynamic_pointer_cast<const facebook::velox::RowType>(input->type()), leafPool_.get());
  EXPECT_TRUE(actual->equalValueAt(input.get(), 0, 0));
}

TEST_F(ShuffleBatchSerializerTest, AdmittedRowLimitBoundsEveryCompletePage) {
  ShuffleBatchSerializer writer(declaredConfig({"zstd", 2}, {
      schema::computed("id", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());
  auto input = maker.rowVector({"id"}, {maker.flatVectorNullable<int64_t>({1, std::nullopt, 3, 4, 5})});
  auto pages = writer.serializeBatches(input, leafPool_.get());
  ASSERT_EQ(pages.size(), 3);
  facebook::velox::vector_size_t offset = 0;
  for (auto& page : pages) {
    EXPECT_LE(page.row_count, 2);
    auto actual = writer.deserialize(std::move(*page.data),
        std::dynamic_pointer_cast<const facebook::velox::RowType>(input->type()), leafPool_.get());
    ASSERT_EQ(actual->size(), page.row_count);
    for (facebook::velox::vector_size_t row = 0; row < actual->size(); ++row) {
      EXPECT_TRUE(actual->equalValueAt(input.get(), row, offset++));
    }
  }
  EXPECT_EQ(offset, input->size());
}

TEST_F(ShuffleBatchSerializerTest, RejectsExactSchemaDriftRatherThanRenamingWireColumns) {
  ShuffleBatchSerializer writer(declaredConfig({"none", 10}, {
      schema::computed("source", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))}));
  ShuffleBatchSerializer reader(declaredConfig({"none", 10}, {
      schema::computed("different", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))}));
  facebook::velox::test::VectorMaker maker(leafPool_.get());
  auto input = maker.rowVector({"source"}, {maker.flatVector<int64_t>({42})});
  auto wire = writer.serialize(input, leafPool_.get());
  EXPECT_THROW(reader.deserialize(std::move(*wire), facebook::velox::ROW({"different"},
      {facebook::velox::BIGINT()}), leafPool_.get()), facebook::velox::VeloxException);
}

}  // namespace
}  // namespace pangrova::shuffle
