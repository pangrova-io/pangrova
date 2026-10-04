// DirectBatchStreamReaderLifetimeTest: Regression test for F-NATIVE-SHUF-01.
//
// The canonical stream reader parses every batch in a standard Arrow stream
// produced by ArrowBatchVectorSerde. Each page comes from an owned IOBuf
// that the fetch loop destroys at the end of every iteration. Earlier code
// wrapped bytes with a NON-owning arrow::Buffer::Wrap and pushed the
// zero-copy RecordBatch into a pending queue that outlives the page buffer,
// producing a use-after-free when the batch was later consumed via ReadNext.
//
// These tests exercise the extracted parser (detail::parseArrowBatchFrames)
// directly and assert that the produced RecordBatches OWN their memory: after
// the source page buffer is poisoned (or freed entirely), the batch values must
// remain correct. Against the buggy non-owning implementation the poisoned
// bytes corrupt the batch values (and trip AddressSanitizer when built with the
// dev-asan preset); against the owning-buffer fix the values survive.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "../TestMemoryHelper.h"
#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/plan/CanonicalPlanSchema.h"
#include "result/FlightResultServer.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::result::test {
namespace {

using namespace facebook::velox;
using pangrova::velox::ArrowBatchVectorSerde;

class DirectBatchStreamReaderLifetimeTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pangrova::test::testMemoryManager();
  }
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("DirectBatchStreamReaderLifetimeTest");
    columnar_pool_ = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_);
  }

  // Builds a single-column INTEGER RowVector from explicit values.
  RowVectorPtr makeIntVector(const std::vector<int32_t>& values) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto col = maker.flatVector<int32_t>(values);
    return maker.rowVector({"col_int"}, {col});
  }

  // Serializes RowVectors into the page wire format used by the direct path:
  // one complete standard Arrow stream containing every appended batch.
  std::vector<uint8_t> serializeCanonicalPage(
      const std::vector<RowVectorPtr>& vectors) {
    ArrowBatchVectorSerde serde;
    ArrowBatchVectorSerde::StreamOptions options;
    options.canonical_schema = std::make_shared<const substrait::NamedStruct>(
        pangrova::velox::schema::named({pangrova::velox::schema::computed("col_int",
            pangrova::velox::schema::primitive("i32", substrait::Type::NULLABILITY_NULLABLE))}));
    StreamArena arena(pool_.get());
    auto serializer = serde.createIterativeSerializer(
        std::dynamic_pointer_cast<const RowType>(vectors.front()->type()), 0, &arena, &options);
    auto outputStream = std::make_unique<IOBufOutputStream>(*pool_);
    for (const auto& vec : vectors) {
      serializer->append(vec);
    }
    serializer->flush(outputStream.get());
    auto iobuf = outputStream->getIOBuf();
    iobuf->coalesce();
    const uint8_t* p = iobuf->data();
    return std::vector<uint8_t>(p, p + iobuf->length());
  }

  // Derives the Arrow schema the way DirectBatchStreamReader::schema() does.
  std::shared_ptr<arrow::Schema> deriveSchema(const RowTypePtr& rowType) {
    ArrowSchema arrow_schema;
    facebook::velox::exportToArrow(
        facebook::velox::BaseVector::create(rowType, 0, pool_.get()),
        arrow_schema);
    auto result = arrow::ImportSchema(&arrow_schema);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return *result;
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<pangrova::memory::ColumnarMemoryPool> columnar_pool_;
};

// Core regression: parse a multi-batch stream, then POISON the source buffer
// before reading the batch values. Owning buffers keep the values intact;
// non-owning Buffer::Wrap would read the poisoned bytes.
TEST_F(DirectBatchStreamReaderLifetimeTest, BatchesSurviveSourcePoisoning) {
  const std::vector<int32_t> v0 = {10, 20, 30, 40, 50};
  const std::vector<int32_t> v1 = {-1, -2, -3, 4242};
  auto rowType = ROW({"col_int"}, {INTEGER()});

  auto page = serializeCanonicalPage({makeIntVector(v0), makeIntVector(v1)});
  auto schema = deriveSchema(rowType);

  auto batches_result = detail::parseArrowBatchFrames(
      schema, page.data(), static_cast<int64_t>(page.size()), columnar_pool_);
  ASSERT_TRUE(batches_result.ok()) << batches_result.status().ToString();
  auto batches = *batches_result;
  ASSERT_EQ(batches.size(), 2u) << "Expected both batches in the stream";

  // Simulate the coalesced IOBuf being freed/reused after the fetch loop
  // iteration by overwriting the entire source buffer with a poison pattern.
  std::memset(page.data(), 0xAB, page.size());

  auto check = [](const std::shared_ptr<arrow::RecordBatch>& batch,
                  const std::vector<int32_t>& expected) {
    ASSERT_EQ(batch->num_columns(), 1);
    ASSERT_EQ(batch->num_rows(), static_cast<int64_t>(expected.size()));
    auto arr = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    ASSERT_NE(arr, nullptr);
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_FALSE(arr->IsNull(static_cast<int64_t>(i)));
      EXPECT_EQ(arr->Value(static_cast<int64_t>(i)), expected[i])
          << "Corrupted value at row " << i
          << " — batch did not own its buffer (use-after-free)";
    }
  };

  check(batches[0], v0);
  check(batches[1], v1);
}

// Stronger variant: the source bytes live in a heap allocation that is FREED
// before the batches are consumed. Under AddressSanitizer (dev-asan preset)
// this deterministically reports a heap-use-after-free against the buggy
// non-owning implementation; with the owning fix it is clean.
TEST_F(DirectBatchStreamReaderLifetimeTest, BatchesSurviveSourceFree) {
  const std::vector<int32_t> values = {7, 11, 13, 17, 19, 23};
  auto rowType = ROW({"col_int"}, {INTEGER()});
  auto page = serializeCanonicalPage({makeIntVector(values)});
  auto schema = deriveSchema(rowType);

  auto* heap = static_cast<uint8_t*>(std::malloc(page.size()));
  ASSERT_NE(heap, nullptr);
  std::memcpy(heap, page.data(), page.size());

  auto batches_result = detail::parseArrowBatchFrames(
      schema, heap, static_cast<int64_t>(page.size()), columnar_pool_);
  ASSERT_TRUE(batches_result.ok()) << batches_result.status().ToString();
  auto batches = *batches_result;
  ASSERT_EQ(batches.size(), 1u);

  // Free the source — owning batches must not reference it anymore.
  std::free(heap);

  auto arr = std::static_pointer_cast<arrow::Int32Array>(batches[0]->column(0));
  ASSERT_NE(arr, nullptr);
  ASSERT_EQ(arr->length(), static_cast<int64_t>(values.size()));
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(arr->Value(static_cast<int64_t>(i)), values[i])
        << "Batch referenced freed source memory at row " << i;
  }
}

// Malformed standard streams are rejected, not parsed into bad batches.
TEST_F(DirectBatchStreamReaderLifetimeTest, RejectsInvalidFrameLength) {
  // 4-byte length prefix claiming more bytes than the page holds.
  std::vector<uint8_t> page(8, 0);
  int32_t bogus = 1000;
  std::memcpy(page.data(), &bogus, sizeof(bogus));
  auto schema = deriveSchema(ROW({"col_int"}, {INTEGER()}));

  auto result = detail::parseArrowBatchFrames(
      schema, page.data(), static_cast<int64_t>(page.size()), columnar_pool_);
  EXPECT_FALSE(result.ok());
}

// ---------------------------------------------------------------------------
// decideFetchWait: result-fetch wait policy (regression for the hardcoded 30s
// deadline that killed long-running / sparse-output queries). The fix waits
// indefinitely while the producing task is alive (timeout<=0 == unbounded),
// enforces a bounded deadline only when one is configured, and fails fast the
// moment the producer dies.
// ---------------------------------------------------------------------------
using detail::FetchWaitDecision;
using detail::decideFetchWait;

// Long-running / sparse query: producer alive, unbounded timeout, huge elapsed.
// Must NOT be killed — this is the core misfire the fix removes.
TEST(DecideFetchWaitTest, AliveUnboundedNeverTimesOut) {
  EXPECT_EQ(decideFetchWait(/*alive=*/true, /*elapsed=*/30, /*timeout=*/0),
            FetchWaitDecision::kKeepWaiting);
  EXPECT_EQ(decideFetchWait(true, 100000, 0),
            FetchWaitDecision::kKeepWaiting);
  EXPECT_EQ(decideFetchWait(true, 100000, -1),
            FetchWaitDecision::kKeepWaiting);
}

// A dead producer is reported promptly regardless of elapsed/timeout.
TEST(DecideFetchWaitTest, DeadProducerFailsFast) {
  EXPECT_EQ(decideFetchWait(/*alive=*/false, 0, 0),
            FetchWaitDecision::kProducerDead);
  EXPECT_EQ(decideFetchWait(false, 5, 3600),
            FetchWaitDecision::kProducerDead);
  // Death takes precedence even past a configured deadline.
  EXPECT_EQ(decideFetchWait(false, 9999, 30),
            FetchWaitDecision::kProducerDead);
}

// A bounded deadline still applies when explicitly configured (timeout>0).
TEST(DecideFetchWaitTest, BoundedDeadlineHonoredWhenConfigured) {
  EXPECT_EQ(decideFetchWait(/*alive=*/true, /*elapsed=*/29, /*timeout=*/30),
            FetchWaitDecision::kKeepWaiting);
  EXPECT_EQ(decideFetchWait(true, 30, 30),
            FetchWaitDecision::kTimedOut);
  EXPECT_EQ(decideFetchWait(true, 31, 30),
            FetchWaitDecision::kTimedOut);
}

}  // namespace
}  // namespace pangrova::result::test
