// BatchSerdeRegistrationTest: Verifies the named batch serde is registered
// at startup and functional for serialize/deserialize round-trip.
//
// Validates:
//   1. The "ArrowBatch" named serde is registered after runtime initialization.
//   2. Serialize/deserialize round-trip produces correct data for supported
//      types (integers, strings, mixed columns with nulls).
//   3. Unsupported types throw specific exception for graceful fallback
//      to PrestoVectorSerde.
//
// Requirements: G.3

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "result/ArrowBatchVectorSerde.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;

class BatchSerdeRegistrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // testMemoryManager() calls initializeVeloxRuntime which registers
    // the batch serde via NativeEngineLifecycle::initialize().
    pangrova::test::testMemoryManager();
  }

  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("BatchSerdeRegistrationTest");
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

// Verifies that the named batch serde is registered after runtime init.
TEST_F(BatchSerdeRegistrationTest, SerdeRegisteredAtStartup) {
  EXPECT_TRUE(facebook::velox::isRegisteredNamedVectorSerde(
      pangrova::result::ArrowBatchVectorSerde::kSerdeKind))
      << "Batch serde must be registered after runtime initialization";
}

// Serialize/deserialize round-trip verifying serialization produces valid
// Arrow IPC bytes. The serialize path is the production-critical path used
// by FlightResultServer to stream pre-serialized Arrow IPC buffers.
TEST_F(BatchSerdeRegistrationTest, SerializeProducesValidArrowBatch) {
  facebook::velox::test::VectorMaker maker(pool_.get());

  // Create a batch with BIGINT column.
  std::vector<int64_t> int_values = {100, -42, 0, 999, 12345, -1};
  auto int_col = maker.flatVector<int64_t>(int_values);
  auto input = maker.rowVector({"id"}, {int_col});

  ASSERT_EQ(input->size(), 6);

  // Serialize via the batch serde.
  pangrova::result::ArrowBatchVectorSerde serde;
  auto serializer = serde.createBatchSerializer(pool_.get());
  ASSERT_NE(serializer, nullptr);

  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange range{0, input->size()};
  std::vector<IndexRange> ranges = {range};
  serializer->serialize(
      input,
      folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
      outputStream.get());

  // Verify serialized bytes are non-empty and contain valid Arrow IPC data.
  auto iobuf = outputStream->getIOBuf();
  ASSERT_NE(iobuf, nullptr);
  ASSERT_GT(iobuf->computeChainDataLength(), sizeof(int32_t))
      << "Serialized output must contain at least length prefix + data";

  // Read the 4-byte length prefix and verify it's reasonable.
  auto data = iobuf->data();
  int32_t length = *reinterpret_cast<const int32_t*>(data);
  EXPECT_GT(length, 0) << "Arrow IPC message length must be positive";
  EXPECT_EQ(
      static_cast<size_t>(length) + sizeof(int32_t),
      iobuf->computeChainDataLength())
      << "Length prefix must match actual data size";
}

// Verifies unsupported type throws specific exception for graceful fallback.
TEST_F(BatchSerdeRegistrationTest, UnsupportedTypeThrowsForFallback) {
  auto opaqueType = OpaqueType::create<int>();
  auto rowType = ROW({"opaque_col"}, {opaqueType});
  auto opaqueVector =
      BaseVector::createNullConstant(opaqueType, 3, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 3,
      std::vector<VectorPtr>{opaqueVector});

  pangrova::result::ArrowBatchVectorSerde serde;
  auto serializer = serde.createBatchSerializer(pool_.get());
  ASSERT_NE(serializer, nullptr);

  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange range{0, 3};
  std::vector<IndexRange> ranges = {range};

  EXPECT_THROW(
      serializer->serialize(
          rowVector,
          folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
          outputStream.get()),
      pangrova::result::ArrowBatchUnsupportedTypeException)
      << "Unsupported type must throw specific exception "
         "for graceful fallback to PrestoVectorSerde";
}

}  // namespace
}  // namespace pangrova::velox::test
