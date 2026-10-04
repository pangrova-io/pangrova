// Canonical serde type rejection and value preservation; no execution rerun.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/plan/CanonicalPlanSchema.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "columnar/CanonicalBatchStream.h"
#include <arrow/array.h>

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;
namespace schema = pangrova::velox::schema;

ArrowBatchVectorSerde::StreamOptions declaredOptions(const std::vector<schema::Field>& fields) {
  ArrowBatchVectorSerde::StreamOptions options;
  options.canonical_schema = std::make_shared<const substrait::NamedStruct>(schema::named(fields));
  return options;
}

class ArrowBatchTypeContractTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("ArrowBatchTypeContractTest");
  }
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(ArrowBatchTypeContractTest, ThrowsOnOpaqueType) {
  auto opaqueType = OpaqueType::create<int>();
  auto rowType = ROW({"normal_col", "opaque_col"}, {BIGINT(), opaqueType});
  auto normalVector = BaseVector::createNullConstant(BIGINT(), 5, pool_.get());
  auto opaqueVector = BaseVector::createNullConstant(opaqueType, 5, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 5,
      std::vector<VectorPtr>{normalVector, opaqueVector});
  pangrova::velox::ArrowBatchVectorSerde serde;
  const auto options = declaredOptions({
      schema::computed("normal_col", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE)),
      schema::computed("opaque_col", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE))});
  auto batchSerializer = serde.createBatchSerializer(pool_.get(), &options);
  ASSERT_NE(batchSerializer, nullptr);
  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange fullRange{0, 5};
  std::vector<IndexRange> ranges = {fullRange};
  EXPECT_THROW(
      batchSerializer->serialize(
          rowVector,
          folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
          outputStream.get()),
      pangrova::velox::ArrowBatchUnsupportedTypeException);
}

TEST_F(ArrowBatchTypeContractTest, ExceptionMessagePreservesSpecificTypeReason) {
  const std::string test_type_name = "OPAQUE<int>";
  pangrova::velox::ArrowBatchUnsupportedTypeException ex(test_type_name);
  static constexpr const char* kExpectedPrefix =
      "ArrowBatch serde does not support type: ";
  std::string msg = ex.what();
  EXPECT_NE(msg.find(kExpectedPrefix), std::string::npos)
      << "Exception message does not contain expected prefix.\n"
      << "  Expected prefix: " << kExpectedPrefix << "\n"
      << "  Actual message:  " << msg;
  EXPECT_NE(msg.find(test_type_name), std::string::npos)
      << "Exception message does not contain type name.\n"
      << "  Expected type: " << test_type_name << "\n"
      << "  Actual message: " << msg;
}

TEST_F(ArrowBatchTypeContractTest, RejectsOpaqueInsideEverySupportedNestedShapeBeforeWriting) {
  auto opaque = OpaqueType::create<int>();
  const std::vector<TypePtr> shapes{ARRAY(opaque), MAP(VARCHAR(), opaque), ROW({"nested"}, {opaque})};
  for (const auto& type : shapes) {
    auto rowType = ROW({"value"}, {type});
    auto vector = std::make_shared<RowVector>(pool_.get(), rowType, nullptr, 2,
        std::vector<VectorPtr>{BaseVector::createNullConstant(type, 2, pool_.get())});
    pangrova::velox::ArrowBatchVectorSerde serde;
    const auto options = declaredOptions({
        schema::computed("value", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE))});
    auto serializer = serde.createBatchSerializer(pool_.get(), &options);
    IOBufOutputStream stream(*pool_);
    IndexRange range{0, 2};
    EXPECT_THROW(serializer->serialize(vector, folly::Range<const IndexRange*>(&range, 1), &stream),
        pangrova::velox::ArrowBatchUnsupportedTypeException);
    EXPECT_EQ(stream.tellp(), 0);
  }
}

TEST_F(ArrowBatchTypeContractTest, ExceptionPreservesTypeName) {
  const std::string type_name = "OPAQUE<CustomVeloxType>";
  pangrova::velox::ArrowBatchUnsupportedTypeException ex(type_name);
  EXPECT_EQ(ex.typeName(), type_name);
}

TEST_F(ArrowBatchTypeContractTest, StandardTypesDoNotThrow) {
  auto rowType = ROW({"int_col", "bigint_col", "varchar_col"},
                     {INTEGER(), BIGINT(), VARCHAR()});
  auto intVector = BaseVector::createNullConstant(INTEGER(), 3, pool_.get());
  auto bigintVector = BaseVector::createNullConstant(BIGINT(), 3, pool_.get());
  auto varcharVector = BaseVector::createNullConstant(VARCHAR(), 3, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 3,
      std::vector<VectorPtr>{intVector, bigintVector, varcharVector});
  pangrova::velox::ArrowBatchVectorSerde serde;
  const auto options = declaredOptions({
      schema::computed("int_col", schema::primitive("i32", substrait::Type::NULLABILITY_NULLABLE)),
      schema::computed("bigint_col", schema::primitive("i64", substrait::Type::NULLABILITY_NULLABLE)),
      schema::computed("varchar_col", schema::primitive("string", substrait::Type::NULLABILITY_NULLABLE))});
  auto batchSerializer = serde.createBatchSerializer(pool_.get(), &options);
  ASSERT_NE(batchSerializer, nullptr);
  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange fullRange{0, 3};
  std::vector<IndexRange> ranges = {fullRange};
  EXPECT_NO_THROW(
      batchSerializer->serialize(
          rowVector,
          folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
          outputStream.get()));
}

TEST_F(ArrowBatchTypeContractTest, Utf8ExportPreservesOriginalInlineAndOutOfLineStringValues) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  const std::vector<std::string> values{"a", std::string(200, 'x'), "多字节字符串保持原值", ""};
  auto strings = maker.flatVector<std::string>(values);
  auto input = maker.rowVector({"text"}, {strings});
  pangrova::velox::ArrowBatchVectorSerde serde;
  const auto options = declaredOptions({
      schema::computed("text", schema::primitive("string", substrait::Type::NULLABILITY_REQUIRED))});
  auto serializer = serde.createBatchSerializer(pool_.get(), &options);
  IOBufOutputStream output(*pool_);
  IndexRange range{0, input->size()};
  serializer->serialize(input, folly::Range<const IndexRange*>(&range, 1), &output);
  for (vector_size_t i = 0; i < input->size(); ++i) EXPECT_EQ(strings->valueAt(i).str(), values[i]);
  auto wire = output.getIOBuf();
  auto source = std::make_unique<BufferInputStream>(byteRangesFromIOBuf(wire.get()));
  RowVectorPtr decoded;
  serde.deserialize(source.get(), pool_.get(), std::dynamic_pointer_cast<const RowType>(input->type()), &decoded, &options);
  ASSERT_EQ(decoded->size(), input->size());
  for (vector_size_t i = 0; i < input->size(); ++i) {
    EXPECT_EQ(decoded->childAt(0)->as<SimpleVector<StringView>>()->valueAt(i).str(), values[i]);
    EXPECT_EQ(strings->valueAt(i).str(), values[i]);
  }
}

TEST_F(ArrowBatchTypeContractTest, MissingOrUntypedOptionsCannotCreateAWriter) {
  ArrowBatchVectorSerde serde;
  StreamArena arena(pool_.get());
  const auto type = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(serde.createBatchSerializer(pool_.get()), VeloxUserError);
  EXPECT_THROW(serde.createIterativeSerializer(type, 0, &arena), VeloxUserError);
  VectorSerde::Options untyped;
  EXPECT_THROW(serde.createBatchSerializer(pool_.get(), &untyped), VeloxUserError);
  ArrowBatchVectorSerde::StreamOptions missing;
  EXPECT_THROW(serde.createBatchSerializer(pool_.get(), &missing), VeloxUserError);
}

TEST_F(ArrowBatchTypeContractTest, MissingReadContractFailsBeforeConsumingOrPublishing) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  const auto input = maker.rowVector({"id"}, {maker.flatVector<int64_t>({42})});
  const auto options = declaredOptions({
      schema::computed("id", schema::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))});
  ArrowBatchVectorSerde serde;
  const auto writer = serde.createBatchSerializer(pool_.get(), &options);
  IOBufOutputStream output(*pool_);
  const IndexRange range{0, input->size()};
  writer->serialize(input, folly::Range<const IndexRange*>(&range, 1), &output);
  auto wire = output.getIOBuf();
  BufferInputStream source(byteRangesFromIOBuf(wire.get()));
  const auto remaining = source.remainingSize();
  RowVectorPtr result = input;
  EXPECT_THROW(serde.deserialize(&source, pool_.get(), input->rowType(), &result), VeloxUserError);
  EXPECT_EQ(source.remainingSize(), remaining);
  EXPECT_EQ(result, input);
  EXPECT_EQ(result->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 42);
}

}  // namespace
}  // namespace pangrova::velox::test
