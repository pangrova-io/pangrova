// Tests for PartialUpdateMergeReader.
// Validates: Requirements R8.5

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "../TestMemoryHelper.h"
#include "merge/PartialUpdateMergeReader.h"
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

class PartialUpdateMergeReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("partial_update_test");
  }

  // Schema: (key: BIGINT, seq: BIGINT, col_a: BIGINT, col_b: BIGINT)
  RowTypePtr makeSchema() {
    return ROW(
        {"key", "seq", "col_a", "col_b"},
        {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  }

  // Build a RowVector with nullable value columns.
  // null_flags: per-column per-row null indicators.
  // null_flags[col][row] == true means that cell is null.
  RowVectorPtr makeBatchWithNulls(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& col_a_vals,
      const std::vector<bool>& col_a_nulls,
      const std::vector<int64_t>& col_b_vals,
      const std::vector<bool>& col_b_nulls) {
    auto size = static_cast<vector_size_t>(keys.size());
    auto key_vec = makeFlatVector<int64_t>(keys);
    auto seq_vec = makeFlatVector<int64_t>(seqs);
    auto a_vec = makeFlatVectorNullable<int64_t>(col_a_vals, col_a_nulls);
    auto b_vec = makeFlatVectorNullable<int64_t>(col_b_vals, col_b_nulls);
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{key_vec, seq_vec, a_vec, b_vec});
  }

  // Build a non-nullable batch (convenience for simple cases).
  RowVectorPtr makeBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& col_a,
      const std::vector<int64_t>& col_b) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeFlatVector<int64_t>(col_a),
            makeFlatVector<int64_t>(col_b)});
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

  template <typename T>
  FlatVectorPtr<T> makeFlatVectorNullable(
      const std::vector<T>& data,
      const std::vector<bool>& nulls) {
    auto vec = BaseVector::create(
        CppToType<T>::create(), data.size(), pool_.get());
    auto flat = vec->template asFlatVector<T>();
    for (vector_size_t i = 0; i < static_cast<vector_size_t>(data.size());
         ++i) {
      if (nulls[i]) {
        flat->setNull(i, true);
      } else {
        flat->set(i, data[i]);
      }
    }
    return std::dynamic_pointer_cast<FlatVector<T>>(vec);
  }

  // Drain all output from a merge reader into a single RowVector.
  RowVectorPtr drainAll(PartialUpdateMergeReader& reader) {
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
// PARTIAL_UPDATE: non-null fields from latest version fill nulls
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, MergesNonNullFieldsFromVersions) {
  auto type = makeSchema();
  // key_indices={0}, sequence_index=1
  // col_a at index 2, col_b at index 3

  // Reader 0 (older): key=1 seq=1, col_a=100, col_b=null
  auto b0 = makeBatchWithNulls(
      type, {1}, {1}, {100}, {false}, {0}, {true});
  // Reader 1 (newer): key=1 seq=5, col_a=null, col_b=200
  auto b1 = makeBatchWithNulls(
      type, {1}, {5}, {0}, {true}, {200}, {false});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto col_a = result->childAt(2)->asFlatVector<int64_t>();
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();

  EXPECT_EQ(keys->valueAt(0), 1);
  // col_a: latest (seq=5) is null, so filled from older (seq=1) → 100
  EXPECT_FALSE(col_a->isNullAt(0));
  EXPECT_EQ(col_a->valueAt(0), 100);
  // col_b: latest (seq=5) is 200 (non-null), keeps 200
  EXPECT_FALSE(col_b->isNullAt(0));
  EXPECT_EQ(col_b->valueAt(0), 200);
}

// =============================================================================
// PARTIAL_UPDATE: latest non-null takes priority over older non-null
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, LatestNonNullTakesPriority) {
  auto type = makeSchema();

  // Reader 0 (older): key=1 seq=1, col_a=100, col_b=200
  auto b0 = makeBatch(type, {1}, {1}, {100}, {200});
  // Reader 1 (newer): key=1 seq=5, col_a=999, col_b=888
  auto b1 = makeBatch(type, {1}, {5}, {999}, {888});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto col_a = result->childAt(2)->asFlatVector<int64_t>();
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();

  // Both columns non-null in latest version → latest values kept.
  EXPECT_EQ(col_a->valueAt(0), 999);
  EXPECT_EQ(col_b->valueAt(0), 888);
}

// =============================================================================
// PARTIAL_UPDATE: three versions merge progressively
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, ThreeVersionsMergeProgressively) {
  auto type = makeSchema();

  // Reader 0 (oldest): key=1 seq=1, col_a=100, col_b=null
  auto b0 = makeBatchWithNulls(
      type, {1}, {1}, {100}, {false}, {0}, {true});
  // Reader 1 (middle): key=1 seq=3, col_a=null, col_b=null
  auto b1 = makeBatchWithNulls(
      type, {1}, {3}, {0}, {true}, {0}, {true});
  // Reader 2 (newest): key=1 seq=5, col_a=null, col_b=500
  auto b2 = makeBatchWithNulls(
      type, {1}, {5}, {0}, {true}, {500}, {false});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto col_a = result->childAt(2)->asFlatVector<int64_t>();
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();

  // col_a: newest=null, middle=null, oldest=100 → filled from oldest → 100
  EXPECT_FALSE(col_a->isNullAt(0));
  EXPECT_EQ(col_a->valueAt(0), 100);
  // col_b: newest=500 (non-null) → keeps 500
  EXPECT_FALSE(col_b->isNullAt(0));
  EXPECT_EQ(col_b->valueAt(0), 500);
}

// =============================================================================
// PARTIAL_UPDATE: all versions null for a column stays null
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, AllNullStaysNull) {
  auto type = makeSchema();

  // Reader 0: key=1 seq=1, col_a=null, col_b=100
  auto b0 = makeBatchWithNulls(
      type, {1}, {1}, {0}, {true}, {100}, {false});
  // Reader 1: key=1 seq=5, col_a=null, col_b=null
  auto b1 = makeBatchWithNulls(
      type, {1}, {5}, {0}, {true}, {0}, {true});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto col_a = result->childAt(2);
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();

  // col_a: both versions null → stays null
  EXPECT_TRUE(col_a->isNullAt(0));
  // col_b: latest=null, older=100 → filled from older → 100
  EXPECT_FALSE(col_b->isNullAt(0));
  EXPECT_EQ(col_b->valueAt(0), 100);
}

// =============================================================================
// PARTIAL_UPDATE: multiple keys each with partial updates
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, MultipleKeysWithPartialUpdates) {
  auto type = makeSchema();

  // Reader 0: key=1 seq=1 (a=10, b=null), key=2 seq=1 (a=null, b=20)
  auto b0 = makeBatchWithNulls(
      type, {1, 2}, {1, 1}, {10, 0}, {false, true}, {0, 20}, {true, false});
  // Reader 1: key=1 seq=3 (a=null, b=30), key=3 seq=1 (a=50, b=60)
  auto b1 = makeBatchWithNulls(
      type, {1, 3}, {3, 1}, {0, 50}, {true, false}, {30, 60}, {false, false});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);  // keys: 1, 2, 3

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto col_a = result->childAt(2)->asFlatVector<int64_t>();
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();

  // key=1: latest(seq=3) a=null,b=30 + older(seq=1) a=10,b=null → a=10,b=30
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(col_a->valueAt(0), 10);
  EXPECT_EQ(col_b->valueAt(0), 30);

  // key=2: only one version → a=null, b=20
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_TRUE(col_a->isNullAt(1));
  EXPECT_EQ(col_b->valueAt(1), 20);

  // key=3: only one version → a=50, b=60
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(col_a->valueAt(2), 50);
  EXPECT_EQ(col_b->valueAt(2), 60);
}

// =============================================================================
// PARTIAL_UPDATE: empty readers
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, EmptyReaders) {
  auto type = makeSchema();
  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));
  EXPECT_FALSE(merger.hasNext());
  auto result = merger.next(100, pool_.get());
  EXPECT_EQ(result, nullptr);
}

// =============================================================================
// PARTIAL_UPDATE: single version per key (no merge needed)
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, SingleVersionPassthrough) {
  auto type = makeSchema();

  auto b0 = makeBatch(type, {1, 2, 3}, {1, 1, 1}, {10, 20, 30}, {40, 50, 60});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);

  auto col_a = result->childAt(2)->asFlatVector<int64_t>();
  auto col_b = result->childAt(3)->asFlatVector<int64_t>();
  EXPECT_EQ(col_a->valueAt(0), 10);
  EXPECT_EQ(col_b->valueAt(0), 40);
  EXPECT_EQ(col_a->valueAt(1), 20);
  EXPECT_EQ(col_b->valueAt(1), 50);
  EXPECT_EQ(col_a->valueAt(2), 30);
  EXPECT_EQ(col_b->valueAt(2), 60);
}

// =============================================================================
// PARTIAL_UPDATE: stats tracking
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, StatsTracking) {
  auto type = makeSchema();

  // Reader 0: key=1 seq=1
  auto b0 = makeBatch(type, {1, 2}, {1, 1}, {10, 20}, {40, 50});
  // Reader 1: key=1 seq=2 (duplicate)
  auto b1 = makeBatch(type, {1}, {2}, {99}, {88});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  merger.initialize(std::move(readers));
  drainAll(merger);

  auto stats = merger.getStats();
  EXPECT_EQ(stats.input_rows, 3);   // 2 from reader0 + 1 from reader1
  EXPECT_EQ(stats.output_rows, 2);  // key=1 (merged), key=2
  EXPECT_EQ(stats.merged_rows, 1);  // one older version merged
}

// =============================================================================
// PARTIAL_UPDATE: close is idempotent
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, CloseIsIdempotent) {
  auto type = makeSchema();
  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));

  merger.close();
  merger.close();
  merger.close();
}

// =============================================================================
// PARTIAL_UPDATE: constructor rejects empty key indices
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, RejectsEmptyKeyIndices) {
  auto type = makeSchema();
  EXPECT_THROW(
      PartialUpdateMergeReader({}, 1, type, MergeDeletePolicy{}),
      std::invalid_argument);
}

// =============================================================================
// PARTIAL_UPDATE: output preserves primary key sort order
// =============================================================================

TEST_F(PartialUpdateMergeReaderTest, OutputIsSortedByPrimaryKey) {
  auto type = makeSchema();

  auto b0 = makeBatch(type, {2, 4}, {1, 1}, {20, 40}, {200, 400});
  auto b1 = makeBatch(type, {1, 3}, {1, 1}, {10, 30}, {100, 300});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  PartialUpdateMergeReader merger({0}, 1, type, MergeDeletePolicy{});
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

TEST_F(PartialUpdateMergeReaderTest, DeleteResetPreventsOlderFieldsFromCrossingAReinsert) {
  auto type = ROW({"key", "_SEQUENCE_NUMBER", "a", "b", "_VALUE_KIND"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT(), TINYINT()});
  auto batch = std::make_shared<RowVector>(pool_.get(), type, nullptr, 3,
      std::vector<VectorPtr>{makeFlatVector<int64_t>({1, 1, 1}), makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVectorNullable<int64_t>({100, 0, 0}, {false, true, true}),
          makeFlatVectorNullable<int64_t>({200, 0, 300}, {false, true, false}),
          makeFlatVector<int8_t>({0, 3, 0})});
  PartialUpdateMergeReader reader({0}, 1, type, MergeDeletePolicy{false, true});
  std::vector<std::unique_ptr<RowReaderWrapper>> sources;
  sources.push_back(std::make_unique<TestRowReaderWrapper>(std::vector<RowVectorPtr>{batch}));
  reader.initialize(std::move(sources));
  auto result = reader.next(10, pool_.get());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->size(), 1);
  EXPECT_TRUE(result->childAt(2)->isNullAt(0));
  EXPECT_EQ(result->childAt(3)->asFlatVector<int64_t>()->valueAt(0), 300);
}

}  // namespace
}  // namespace pangrova::merge::test
