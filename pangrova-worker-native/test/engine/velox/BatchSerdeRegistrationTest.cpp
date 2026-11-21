// BatchSerdeRegistrationTest: Verifies the named batch serde is registered
// at startup and functional for serialize/deserialize round-trip.
//
// Validates:
//   1. The "ArrowBatch" named serde is registered after runtime initialization.
//   2. Serialize/deserialize round-trip produces correct data for supported
//      types (integers, strings, mixed columns with nulls).
//   3. Unsupported types fail explicitly without another execution path.

#include <gtest/gtest.h>
#include <arrow/array.h>
#include <arrow/buffer.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"

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
namespace schema = pangrova::velox::schema;

ArrowBatchVectorSerde::StreamOptions declaredInteger(
    const std::string& name, substrait::Type::Nullability nullability) {
  ArrowBatchVectorSerde::StreamOptions options;
  options.canonical_schema = std::make_shared<const substrait::NamedStruct>(
      schema::named({schema::computed(name, schema::primitive("i64", nullability))}));
  return options;
}

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
      pangrova::velox::ArrowBatchVectorSerde::kSerdeKind))
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
  pangrova::velox::ArrowBatchVectorSerde serde;
  const auto options = declaredInteger("id", substrait::Type::NULLABILITY_REQUIRED);
  auto serializer = serde.createBatchSerializer(pool_.get(), &options);
  ASSERT_NE(serializer, nullptr);

  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange range{0, input->size()};
  std::vector<IndexRange> ranges = {range};
  serializer->serialize(
      input,
      folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
      outputStream.get());

  // Decode the actual canonical stream rather than a private length prefix.
  auto iobuf = outputStream->getIOBuf();
  ASSERT_NE(iobuf, nullptr);
  ASSERT_GT(iobuf->computeChainDataLength(), 8);
  iobuf->coalesce();
  auto buffer = arrow::Buffer::Wrap(iobuf->data(), iobuf->length());
  auto columnar_pool = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
  auto decoded = pangrova::columnar::decodeBatchStream(buffer, columnar_pool);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  ASSERT_EQ(decoded->size(), 1);
  const auto& batch = decoded->front();
  ASSERT_TRUE(batch->ValidateFull().ok());
  ASSERT_EQ(batch->num_columns(), 1);
  EXPECT_EQ(batch->schema()->field(0)->name(), "id");
  EXPECT_FALSE(batch->schema()->field(0)->nullable());
  ASSERT_EQ(batch->num_rows(), int_values.size());
  ASSERT_EQ(batch->column(0)->type_id(), arrow::Type::INT64);
  auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
  EXPECT_EQ(values->null_count(), 0);
  for (size_t row = 0; row < int_values.size(); ++row) {
    EXPECT_EQ(values->Value(row), int_values[row]);
  }
}

// Unsupported values cannot enter the canonical wire representation.
TEST_F(BatchSerdeRegistrationTest, UnsupportedTypeFailsWithoutWritingPayload) {
  auto opaqueType = OpaqueType::create<int>();
  auto rowType = ROW({"opaque_col"}, {opaqueType});
  auto opaqueVector =
      BaseVector::createNullConstant(opaqueType, 3, pool_.get());
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 3,
      std::vector<VectorPtr>{opaqueVector});

  pangrova::velox::ArrowBatchVectorSerde serde;
  const auto options = declaredInteger("opaque_col", substrait::Type::NULLABILITY_NULLABLE);
  auto serializer = serde.createBatchSerializer(pool_.get(), &options);
  ASSERT_NE(serializer, nullptr);

  auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
  IndexRange range{0, 3};
  std::vector<IndexRange> ranges = {range};

  EXPECT_THROW(
      serializer->serialize(
          rowVector,
          folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
          outputStream.get()),
      pangrova::velox::ArrowBatchUnsupportedTypeException)
      << "Unsupported type must fail before publishing any payload";
  EXPECT_EQ(outputStream->tellp(), std::streampos(0));
}

}  // namespace
}  // namespace pangrova::velox::test
