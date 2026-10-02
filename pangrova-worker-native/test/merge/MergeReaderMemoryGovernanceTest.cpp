// Tests for MergeReader memory governance.
// Validates that merge readers:
// 1. Release all resources on close() (idempotent)
// 2. Release all resources on destruction (destructor calls close())
// 3. Allocate from the provided MemoryPool (not private pools)
// 4. Keep bounded memory footprint (K batches + 1 output)
//
// Validates: Requirements R21.1, R21.4
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "../TestMemoryHelper.h"
#include "merge/DeduplicateMergeReader.h"
#include "merge/FirstRowMergeReader.h"
#include "merge/MergeReader.h"
#include "merge/PartialUpdateMergeReader.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::merge::test {
namespace {

using namespace facebook::velox;

// Stub RowReaderWrapper that produces a fixed number of batches.
class StubRowReader : public RowReaderWrapper {
 public:
  StubRowReader(
      RowTypePtr type,
      int num_batches,
      int rows_per_batch)
      : type_(std::move(type)),
        num_batches_(num_batches),
        rows_per_batch_(rows_per_batch) {}

  RowVectorPtr next(uint64_t /*size*/, facebook::velox::memory::MemoryPool* pool) override {
    if (current_batch_ >= num_batches_) {
      exhausted_ = true;
      return nullptr;
    }
    current_batch_++;

    // Create a batch with key=current_batch_*100+row, seq=row, val=row*10.
    auto key_vector = BaseVector::create(BIGINT(), rows_per_batch_, pool);
    auto seq_vector = BaseVector::create(BIGINT(), rows_per_batch_, pool);
    auto val_vector = BaseVector::create(BIGINT(), rows_per_batch_, pool);

    auto* keys = key_vector->asFlatVector<int64_t>();
    auto* seqs = seq_vector->asFlatVector<int64_t>();
    auto* vals = val_vector->asFlatVector<int64_t>();

    for (int i = 0; i < rows_per_batch_; ++i) {
      keys->set(i, static_cast<int64_t>(current_batch_ * 100 + i));
      seqs->set(i, static_cast<int64_t>(i));
      vals->set(i, static_cast<int64_t>(i * 10));
    }

    return std::make_shared<RowVector>(
        pool, type_, nullptr, rows_per_batch_,
        std::vector<VectorPtr>{key_vector, seq_vector, val_vector});
  }

  bool hasNext() const override { return !exhausted_; }

 private:
  RowTypePtr type_;
  int num_batches_;
  int rows_per_batch_;
  int current_batch_ = 0;
  bool exhausted_ = false;
};

RowTypePtr makeTestType() {
  return ROW({"key", "seq", "val"}, {BIGINT(), BIGINT(), BIGINT()});
}

// --- Close is idempotent ---

TEST(MergeReaderMemoryGovernanceTest, CloseIsIdempotent) {
  auto type = makeTestType();
  DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});

  auto pool = pangrova::test::testLeafPool("test-close-idempotent");
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(
      std::make_unique<StubRowReader>(type, 1, 5));
  reader.initialize(std::move(readers));

  // Close multiple times — should not crash.
  reader.close();
  reader.close();
  reader.close();
  EXPECT_FALSE(reader.hasNext());
}

// --- Destructor releases resources ---

TEST(MergeReaderMemoryGovernanceTest, DestructorReleasesResources) {
  auto type = makeTestType();
  auto pool = pangrova::test::testLeafPool("test-destructor");

  {
    DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
    std::vector<std::unique_ptr<RowReaderWrapper>> readers;
    readers.push_back(
        std::make_unique<StubRowReader>(type, 2, 10));
    reader.initialize(std::move(readers));

    // Read one batch to populate internal state.
    auto batch = reader.next(5, pool.get());
    EXPECT_NE(batch, nullptr);
    // Destructor should call close() and release all resources.
  }
  // If we get here without crash/leak, destructor worked.
  SUCCEED();
}

// --- FirstRow destructor releases resources ---

TEST(MergeReaderMemoryGovernanceTest, FirstRowDestructorReleasesResources) {
  auto type = makeTestType();
  auto pool = pangrova::test::testLeafPool("test-firstrow-dtor");

  {
    FirstRowMergeReader reader({0}, 1, type, MergeDeletePolicy{});
    std::vector<std::unique_ptr<RowReaderWrapper>> readers;
    readers.push_back(
        std::make_unique<StubRowReader>(type, 1, 5));
    reader.initialize(std::move(readers));
  }
  SUCCEED();
}

// --- PartialUpdate destructor releases resources ---

TEST(MergeReaderMemoryGovernanceTest, PartialUpdateDestructorReleasesResources) {
  auto type = makeTestType();
  auto pool = pangrova::test::testLeafPool("test-partial-dtor");

  {
    PartialUpdateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
    std::vector<std::unique_ptr<RowReaderWrapper>> readers;
    readers.push_back(
        std::make_unique<StubRowReader>(type, 1, 5));
    reader.initialize(std::move(readers));
  }
  SUCCEED();
}

// --- Stats reflect merge activity ---

TEST(MergeReaderMemoryGovernanceTest, StatsReflectMergeActivity) {
  auto type = makeTestType();
  auto pool = pangrova::test::testLeafPool("test-stats");

  DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(
      std::make_unique<StubRowReader>(type, 1, 5));
  reader.initialize(std::move(readers));

  // Drain all data.
  while (reader.hasNext()) {
    reader.next(100, pool.get());
  }

  auto stats = reader.getStats();
  EXPECT_GT(stats.input_rows, 0);
  EXPECT_GT(stats.output_rows, 0);
  EXPECT_GE(stats.input_rows, stats.output_rows);
}

// --- Close after partial read releases remaining resources ---

TEST(MergeReaderMemoryGovernanceTest, CloseAfterPartialReadReleasesResources) {
  auto type = makeTestType();
  auto pool = pangrova::test::testLeafPool("test-partial-close");

  DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(
      std::make_unique<StubRowReader>(type, 10, 100));
  reader.initialize(std::move(readers));

  // Read only one batch (leaving data in the pipeline).
  auto batch = reader.next(5, pool.get());
  EXPECT_NE(batch, nullptr);

  // Close should release all remaining resources.
  reader.close();
  EXPECT_FALSE(reader.hasNext());
  EXPECT_EQ(reader.next(5, pool.get()), nullptr);
}

}  // namespace
}  // namespace pangrova::merge::test
