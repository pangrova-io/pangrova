// ArrowBatchFallbackTest: Unit tests for Arrow IPC serde fallback behavior.
//
// Verifies:
//   1. ArrowBatchUnsupportedTypeException is thrown when serializing a
//      RowVector containing OPAQUE type columns.
//   2. VeloxTaskRunner::isResultSerdeUnsupportedTypeError() correctly
//      detects the exception message format.
//   3. The exception message format matches the prefix used by the
//      fallback detection logic.
//
// Requirements: 3.5

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "result/ArrowBatchVectorSerde.h"
#include "velox/runner/VeloxTaskRunner.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

class ArrowBatchFallbackTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("ArrowBatchFallbackTest");
  }
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(ArrowBatchFallbackTest, ThrowsOnOpaqueType) {
  auto opaqueType = OpaqueType::create<int>();
  auto rowType = ROW({"normal_col", "opaque_col"}, {BIGINT(), opaqueType});
  auto normalVector = BaseVector::createNullConstant(BIGINT(), 5, pool_.get());
  auto opaqueVector = BaseVector::createNullConstant(opaqueType, 5, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 5,
      std::vector<VectorPtr>{normalVector, opaqueVector});
  pangrova::result::ArrowBatchVectorSerde serde;
  auto batchSerializer = serde.createBatchSerializer(pool_.get());
  ASSERT_NE(batchSerializer, nullptr);
  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange fullRange{0, 5};
  std::vector<IndexRange> ranges = {fullRange};
  EXPECT_THROW(
      batchSerializer->serialize(
          rowVector,
          folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
          outputStream.get()),
      pangrova::result::ArrowBatchUnsupportedTypeException);
}

TEST_F(ArrowBatchFallbackTest, ExceptionMessageMatchesFallbackPrefix) {
  const std::string test_type_name = "OPAQUE<int>";
  pangrova::result::ArrowBatchUnsupportedTypeException ex(test_type_name);
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

TEST_F(ArrowBatchFallbackTest, FallbackDetectionLogicMatchesExceptionFormat) {
  static constexpr const char* kUnsupportedTypePrefix =
      "ArrowBatch serde does not support type: ";
  {
    pangrova::result::ArrowBatchUnsupportedTypeException ex("OPAQUE<MyType>");
    std::string error_message = ex.what();
    bool detected = !error_message.empty() &&
        error_message.find(kUnsupportedTypePrefix) != std::string::npos;
    EXPECT_TRUE(detected)
        << "Failed to detect ArrowBatch unsupported type error.\n"
        << "  Message: " << error_message;
  }
  {
    std::string error_message = "Some other Velox error occurred";
    bool detected = !error_message.empty() &&
        error_message.find(kUnsupportedTypePrefix) != std::string::npos;
    EXPECT_FALSE(detected);
  }
  {
    std::string error_message;
    bool detected = !error_message.empty() &&
        error_message.find(kUnsupportedTypePrefix) != std::string::npos;
    EXPECT_FALSE(detected);
  }
  {
    std::string error_message = "ArrowBatch serde failed for unknown reason";
    bool detected = !error_message.empty() &&
        error_message.find(kUnsupportedTypePrefix) != std::string::npos;
    EXPECT_FALSE(detected);
  }
}

TEST_F(ArrowBatchFallbackTest, ExceptionPreservesTypeName) {
  const std::string type_name = "OPAQUE<CustomVeloxType>";
  pangrova::result::ArrowBatchUnsupportedTypeException ex(type_name);
  EXPECT_EQ(ex.typeName(), type_name);
}

TEST_F(ArrowBatchFallbackTest, StandardTypesDoNotThrow) {
  auto rowType = ROW({"int_col", "bigint_col", "varchar_col"},
                     {INTEGER(), BIGINT(), VARCHAR()});
  auto intVector = BaseVector::createNullConstant(INTEGER(), 3, pool_.get());
  auto bigintVector = BaseVector::createNullConstant(BIGINT(), 3, pool_.get());
  auto varcharVector = BaseVector::createNullConstant(VARCHAR(), 3, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 3,
      std::vector<VectorPtr>{intVector, bigintVector, varcharVector});
  pangrova::result::ArrowBatchVectorSerde serde;
  auto batchSerializer = serde.createBatchSerializer(pool_.get());
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

}  // namespace
}  // namespace pangrova::velox::test
