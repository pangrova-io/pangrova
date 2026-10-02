// Tests for DeduplicateMergeReader.
// Validates: Requirements R8.4, R8.8

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "../TestMemoryHelper.h"
#include "merge/DeduplicateMergeReader.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::merge::test {
namespace {

using namespace facebook::velox;

// =============================================================================
// Test RowReaderWrapper: produces pre-built RowVectors in sequence.
// =============================================================================

class TestRowReaderWrapper : public RowReaderWrapper {
 public:
  explicit TestRowReaderWrapper(std::vector<RowVectorPtr> batches)
      : batches_(std::move(batches)), index_(0) {}

  RowVectorPtr next(uint64_t /*size*/, facebook::velox::memory::MemoryPool* /*pool*/) override {
    if (index_ >= batches_.size()) {
      return nullptr;
    }
    return batches_[index_++];
  }

  bool hasNext() const override {
    return index_ < batches_.size();
  }

 private:
  std::vector<RowVectorPtr> batches_;
  size_t index_;
};

// =============================================================================
// Test fixture with shared MemoryPool and schema helpers.
// =============================================================================

class DeduplicateMergeReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("merge_test");
  }

  // Schema: (key: BIGINT, seq: BIGINT, value: BIGINT)
  RowTypePtr makeSchema() {
    return ROW({"key", "seq", "value"}, {BIGINT(), BIGINT(), BIGINT()});
  }

  // Schema: (pk1: BIGINT, pk2: BIGINT, seq: BIGINT, value: BIGINT)
  RowTypePtr makeCompositeKeySchema() {
    return ROW(
        {"pk1", "pk2", "seq", "value"},
        {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  }

  // Build a RowVector from parallel arrays for (key, seq, value).
  RowVectorPtr makeBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& values) {
    auto size = static_cast<vector_size_t>(keys.size());
    auto key_vec = makeFlatVector<int64_t>(keys);
    auto seq_vec = makeFlatVector<int64_t>(seqs);
    auto val_vec = makeFlatVector<int64_t>(values);
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{key_vec, seq_vec, val_vec});
  }

  // Build a RowVector for composite key schema (pk1, pk2, seq, value).
  RowVectorPtr makeCompositeKeyBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& pk1,
      const std::vector<int64_t>& pk2,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& values) {
    auto size = static_cast<vector_size_t>(pk1.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(pk1),
            makeFlatVector<int64_t>(pk2),
            makeFlatVector<int64_t>(seqs),
            makeFlatVector<int64_t>(values)});
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(const std::vector<T>& data) {
    auto vec = BaseVector::create(
        CppToType<T>::create(), data.size(), pool_.get());
    auto flat = vec->template asFlatVector<T>();
    for (vector_size_t i = 0; i < static_cast<vector_size_t>(data.size());
         ++i) {
      flat->set(i, data[i]);
    }
    return std::dynamic_pointer_cast<FlatVector<T>>(vec);
  }

  // Drain all output from a merge reader into a single RowVector.
  RowVectorPtr drainAll(DeduplicateMergeReader& reader) {
    std::vector<RowVectorPtr> results;
    while (reader.hasNext()) {
      auto batch = reader.next(4096, pool_.get());
      if (batch) {
        results.push_back(batch);
      }
    }
    if (results.empty()) {
      return nullptr;
    }
    if (results.size() == 1) {
      return results[0];
    }
    // Concatenate all result batches.
    vector_size_t total = 0;
    for (auto& r : results) {
      total += r->size();
    }
    const auto& type = results[0]->type()->asRow();
    auto num_children = type.size();
    std::vector<VectorPtr> children(num_children);
    for (size_t col = 0; col < num_children; ++col) {
      children[col] = BaseVector::create(
          type.childAt(col), total, pool_.get());
      vector_size_t offset = 0;
      for (auto& r : results) {
        children[col]->copy(r->childAt(col).get(), offset, 0, r->size());
        offset += r->size();
      }
    }
    return std::make_shared<RowVector>(
        pool_.get(),
        std::dynamic_pointer_cast<const RowType>(results[0]->type()),
        nullptr, total, std::move(children));
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

// =============================================================================
// DEDUPLICATE: basic — keeps latest version per key
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, KeepsLatestVersionPerKey) {
  auto type = makeSchema();
  // key_indices={0}, sequence_index=1

  // Reader 0 (older level): key=1 seq=1 val=100, key=2 seq=1 val=200
  auto b0 = makeBatch(type, {1, 2}, {1, 1}, {100, 200});
  // Reader 1 (newer level): key=1 seq=5 val=999, key=3 seq=2 val=300
  auto b1 = makeBatch(type, {1, 3}, {5, 2}, {999, 300});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);  // keys: 1, 2, 3

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto vals = result->childAt(2)->asFlatVector<int64_t>();

  // Output should be sorted by key.
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(vals->valueAt(0), 999);  // key=1 latest (seq=5)
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(vals->valueAt(1), 200);  // key=2 only version
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(vals->valueAt(2), 300);  // key=3 only version
}

// =============================================================================
// DEDUPLICATE: all readers have same key — only one row emitted
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, AllSameKeyKeepsHighestSequence) {
  auto type = makeSchema();

  auto b0 = makeBatch(type, {1}, {1}, {100});
  auto b1 = makeBatch(type, {1}, {3}, {300});
  auto b2 = makeBatch(type, {1}, {2}, {200});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto vals = result->childAt(2)->asFlatVector<int64_t>();
  EXPECT_EQ(vals->valueAt(0), 300);  // seq=3 is highest
}

// =============================================================================
// DEDUPLICATE: empty readers
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, EmptyReaders) {
  auto type = makeSchema();
  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));
  EXPECT_FALSE(merger.hasNext());
  auto result = merger.next(100, pool_.get());
  EXPECT_EQ(result, nullptr);
}

// =============================================================================
// DEDUPLICATE: output preserves primary key sort order
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, OutputIsSortedByPrimaryKey) {
  auto type = makeSchema();

  // Reader 0: keys 2, 4 (sorted)
  auto b0 = makeBatch(type, {2, 4}, {1, 1}, {20, 40});
  // Reader 1: keys 1, 3 (sorted)
  auto b1 = makeBatch(type, {1, 3}, {1, 1}, {10, 30});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 4);

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(keys->valueAt(3), 4);
}

// =============================================================================
// DEDUPLICATE: stats tracking
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, StatsTracking) {
  auto type = makeSchema();

  // Reader 0: key=1 seq=1, key=2 seq=1
  auto b0 = makeBatch(type, {1, 2}, {1, 1}, {100, 200});
  // Reader 1: key=1 seq=2 (duplicate of key=1)
  auto b1 = makeBatch(type, {1}, {2}, {999});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));
  drainAll(merger);

  auto stats = merger.getStats();
  EXPECT_EQ(stats.input_rows, 3);   // 2 from reader0 + 1 from reader1
  EXPECT_EQ(stats.output_rows, 2);  // key=1 (deduped), key=2
  EXPECT_EQ(stats.merged_rows, 1);  // one duplicate eliminated
}

// =============================================================================
// DEDUPLICATE: composite primary key (pk1, pk2)
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, CompositeKeyDeduplication) {
  auto type = makeCompositeKeySchema();
  // key_indices={0,1}, sequence_index=2

  // Reader 0: (pk1=1, pk2=1, seq=1, val=100), (pk1=1, pk2=2, seq=1, val=200)
  auto b0 = makeCompositeKeyBatch(type, {1, 1}, {1, 2}, {1, 1}, {100, 200});
  // Reader 1: (pk1=1, pk2=1, seq=3, val=999)
  auto b1 = makeCompositeKeyBatch(type, {1}, {1}, {3}, {999});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  DeduplicateMergeReader merger({0, 1}, 2, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 2);  // (1,1) and (1,2)

  auto vals = result->childAt(3)->asFlatVector<int64_t>();
  EXPECT_EQ(vals->valueAt(0), 999);  // (1,1) latest seq=3
  EXPECT_EQ(vals->valueAt(1), 200);  // (1,2) only version
}

// =============================================================================
// DEDUPLICATE: single reader (no merge needed)
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, SingleReaderPassthrough) {
  auto type = makeSchema();

  auto b0 = makeBatch(type, {1, 2, 3}, {1, 1, 1}, {10, 20, 30});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);

  auto vals = result->childAt(2)->asFlatVector<int64_t>();
  EXPECT_EQ(vals->valueAt(0), 10);
  EXPECT_EQ(vals->valueAt(1), 20);
  EXPECT_EQ(vals->valueAt(2), 30);
}

// =============================================================================
// DEDUPLICATE: multiple batches per reader
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, MultipleBatchesPerReader) {
  auto type = makeSchema();

  // Reader 0: two batches
  auto b0a = makeBatch(type, {1, 2}, {1, 1}, {10, 20});
  auto b0b = makeBatch(type, {5, 6}, {1, 1}, {50, 60});
  // Reader 1: one batch overlapping key=2
  auto b1 = makeBatch(type, {2, 3}, {5, 1}, {999, 30});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0a, b0b}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 5);  // keys: 1, 2, 3, 5, 6

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto vals = result->childAt(2)->asFlatVector<int64_t>();

  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(vals->valueAt(0), 10);
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(vals->valueAt(1), 999);  // key=2 latest (seq=5)
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(vals->valueAt(2), 30);
  EXPECT_EQ(keys->valueAt(3), 5);
  EXPECT_EQ(vals->valueAt(3), 50);
  EXPECT_EQ(keys->valueAt(4), 6);
  EXPECT_EQ(vals->valueAt(4), 60);
}

// =============================================================================
// DEDUPLICATE: close is idempotent
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, CloseIsIdempotent) {
  auto type = makeSchema();
  DeduplicateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));

  // Multiple close calls should not crash.
  merger.close();
  merger.close();
  merger.close();
}

// =============================================================================
// DEDUPLICATE: constructor rejects empty key indices
// =============================================================================

TEST_F(DeduplicateMergeReaderTest, RejectsEmptyKeyIndices) {
  auto type = makeSchema();
  EXPECT_THROW(
      DeduplicateMergeReader({}, 1, type, MergeDeletePolicy{}),
      std::invalid_argument);
}

TEST_F(DeduplicateMergeReaderTest, PersistedPaimonDeleteWinnerDoesNotResurrectAnOlderValue) {
  const auto type = ROW({"key", "_SEQUENCE_NUMBER", "value", "_VALUE_KIND"},
      {BIGINT(), BIGINT(), BIGINT(), TINYINT()});
  const auto batch = [&](int64_t sequence, int8_t kind) {
    return std::make_shared<RowVector>(pool_.get(), type, nullptr, 1,
        std::vector<VectorPtr>{makeFlatVector<int64_t>({1}), makeFlatVector<int64_t>({sequence}),
            makeFlatVector<int64_t>({sequence * 10}), makeFlatVector<int8_t>({kind})});
  };
  DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> sources;
  sources.push_back(std::make_unique<TestRowReaderWrapper>(std::vector<RowVectorPtr>{batch(1, 0)}));
  sources.push_back(std::make_unique<TestRowReaderWrapper>(std::vector<RowVectorPtr>{batch(2, 3)}));
  reader.initialize(std::move(sources));
  EXPECT_EQ(reader.next(10, pool_.get()), nullptr);
  EXPECT_FALSE(reader.hasNext());
}

TEST_F(DeduplicateMergeReaderTest, LaterVersionWithinOnePhysicalFileStillWins) {
  const auto type = makeSchema();
  DeduplicateMergeReader reader({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> sources;
  sources.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{makeBatch(type, {1, 1, 1}, {1, 3, 2}, {10, 30, 20})}));
  reader.initialize(std::move(sources));
  const auto result = reader.next(10, pool_.get());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ(result->childAt(2)->asFlatVector<int64_t>()->valueAt(0), 30);
}

}  // namespace
}  // namespace pangrova::merge::test
