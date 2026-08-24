// Tests for AggregationMergeReader.
// Validates: Requirements R8.6

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../TestMemoryHelper.h"
#include "merge/AggregationMergeReader.h"
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

class AggregationMergeReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("aggregation_test");
  }

  // Schema: (key: BIGINT, seq: BIGINT, amount: BIGINT, count: BIGINT)
  RowTypePtr makeSumCountSchema() {
    return ROW(
        {"key", "seq", "amount", "count"},
        {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  }

  // Schema: (key: BIGINT, seq: BIGINT, value: BIGINT)
  RowTypePtr makeSimpleSchema() {
    return ROW({"key", "seq", "value"}, {BIGINT(), BIGINT(), BIGINT()});
  }

  // Schema: (key: BIGINT, seq: BIGINT, flag: BOOLEAN)
  RowTypePtr makeBoolSchema() {
    return ROW({"key", "seq", "flag"}, {BIGINT(), BIGINT(), BOOLEAN()});
  }

  // Schema: (key: BIGINT, seq: BIGINT, name: VARCHAR)
  RowTypePtr makeStringSchema() {
    return ROW({"key", "seq", "name"}, {BIGINT(), BIGINT(), VARCHAR()});
  }

  // Schema: (key: BIGINT, seq: BIGINT, val: DOUBLE)
  RowTypePtr makeDoubleSchema() {
    return ROW({"key", "seq", "val"}, {BIGINT(), BIGINT(), DOUBLE()});
  }

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

  RowVectorPtr makeSimpleBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& values) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeFlatVector<int64_t>(values)});
  }

  RowVectorPtr makeBoolBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<bool>& flags) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeBoolFlatVector(flags)});
  }

  RowVectorPtr makeStringBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<std::string>& names) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeStringFlatVector(names)});
  }

  RowVectorPtr makeDoubleBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<double>& values) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeFlatVector<double>(values)});
  }

  RowVectorPtr makeNullableSimpleBatch(
      const RowTypePtr& type,
      const std::vector<int64_t>& keys,
      const std::vector<int64_t>& seqs,
      const std::vector<int64_t>& values,
      const std::vector<bool>& nulls) {
    auto size = static_cast<vector_size_t>(keys.size());
    return std::make_shared<RowVector>(
        pool_.get(), type, nullptr, size,
        std::vector<VectorPtr>{
            makeFlatVector<int64_t>(keys),
            makeFlatVector<int64_t>(seqs),
            makeFlatVectorNullable<int64_t>(values, nulls)});
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

  VectorPtr makeBoolFlatVector(const std::vector<bool>& data) {
    auto vec = BaseVector::create(BOOLEAN(), data.size(), pool_.get());
    auto flat = vec->asFlatVector<bool>();
    for (vector_size_t i = 0; i < static_cast<vector_size_t>(data.size());
         ++i) {
      flat->set(i, data[i]);
    }
    return vec;
  }

  VectorPtr makeStringFlatVector(const std::vector<std::string>& data) {
    auto vec = BaseVector::create(VARCHAR(), data.size(), pool_.get());
    auto flat = vec->asFlatVector<StringView>();
    for (vector_size_t i = 0; i < static_cast<vector_size_t>(data.size());
         ++i) {
      flat->set(i, StringView(data[i]));
    }
    return vec;
  }

  // Drain all output from a merge reader into a single RowVector.
  RowVectorPtr drainAll(AggregationMergeReader& reader) {
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
// parseAggFunction tests
// =============================================================================

TEST_F(AggregationMergeReaderTest, ParseAggFunctionValid) {
  EXPECT_EQ(parseAggFunction("SUM"), AggFunction::SUM);
  EXPECT_EQ(parseAggFunction("sum"), AggFunction::SUM);
  EXPECT_EQ(parseAggFunction("Count"), AggFunction::COUNT);
  EXPECT_EQ(parseAggFunction("MAX"), AggFunction::MAX);
  EXPECT_EQ(parseAggFunction("min"), AggFunction::MIN);
  EXPECT_EQ(parseAggFunction("LAST_VALUE"), AggFunction::LAST_VALUE);
  EXPECT_EQ(parseAggFunction("last_non_null_value"),
            AggFunction::LAST_NON_NULL_VALUE);
  EXPECT_EQ(parseAggFunction("LISTAGG"), AggFunction::LISTAGG);
  EXPECT_EQ(parseAggFunction("BOOL_AND"), AggFunction::BOOL_AND);
  EXPECT_EQ(parseAggFunction("BOOL_OR"), AggFunction::BOOL_OR);
}

TEST_F(AggregationMergeReaderTest, ParseAggFunctionInvalid) {
  EXPECT_THROW(parseAggFunction("UNKNOWN"), std::invalid_argument);
  EXPECT_THROW(parseAggFunction(""), std::invalid_argument);
}

// =============================================================================
// AGGREGATION: SUM — sums values across versions of the same key
// =============================================================================

TEST_F(AggregationMergeReaderTest, SumAggregation) {
  auto type = makeSumCountSchema();
  // key_indices={0}, sequence_index=1
  // amount at index 2 → SUM, count at index 3 → SUM

  // Reader 0: key=1 seq=1 amount=100 count=1
  auto b0 = makeBatch(type, {1}, {1}, {100}, {1});
  // Reader 1: key=1 seq=2 amount=200 count=3
  auto b1 = makeBatch(type, {1}, {2}, {200}, {3});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;  // amount → SUM
  agg_specs[3] = AggFunction::SUM;  // count → SUM

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto amounts = result->childAt(2)->asFlatVector<int64_t>();
  auto counts = result->childAt(3)->asFlatVector<int64_t>();

  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(amounts->valueAt(0), 300);  // 200 + 100
  EXPECT_EQ(counts->valueAt(0), 4);     // 3 + 1
}

// =============================================================================
// AGGREGATION: MAX and MIN
// =============================================================================

TEST_F(AggregationMergeReaderTest, MaxMinAggregation) {
  auto type = makeSumCountSchema();
  // amount at index 2 → MAX, count at index 3 → MIN

  auto b0 = makeBatch(type, {1, 2}, {1, 1}, {50, 10}, {100, 200});
  auto b1 = makeBatch(type, {1, 2}, {2, 2}, {80, 5}, {30, 300});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::MAX;
  agg_specs[3] = AggFunction::MIN;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 2);

  auto amounts = result->childAt(2)->asFlatVector<int64_t>();
  auto counts = result->childAt(3)->asFlatVector<int64_t>();

  // key=1: MAX(80, 50)=80, MIN(30, 100)=30
  EXPECT_EQ(amounts->valueAt(0), 80);
  EXPECT_EQ(counts->valueAt(0), 30);
  // key=2: MAX(5, 10)=10, MIN(300, 200)=200
  EXPECT_EQ(amounts->valueAt(1), 10);
  EXPECT_EQ(counts->valueAt(1), 200);
}

// =============================================================================
// AGGREGATION: COUNT — increments for each non-null value
// =============================================================================

TEST_F(AggregationMergeReaderTest, CountAggregation) {
  auto type = makeSimpleSchema();
  // value at index 2 → COUNT

  auto b0 = makeSimpleBatch(type, {1}, {1}, {100});
  auto b1 = makeSimpleBatch(type, {1}, {2}, {200});
  auto b2 = makeSimpleBatch(type, {1}, {3}, {300});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::COUNT;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  // The winner (seq=3, val=300) is the initial value.
  // Then two older rows are aggregated: COUNT increments by 1 each.
  // Initial value from winner is kept as-is (300), then +1, +1 = 302?
  // No — COUNT semantics: the winner's value is the starting accumulator,
  // then each older row increments by 1. So: 300 + 1 + 1 = 302.
  auto vals = result->childAt(2)->asFlatVector<int64_t>();
  EXPECT_EQ(vals->valueAt(0), 302);
}

// =============================================================================
// AGGREGATION: BOOL_AND and BOOL_OR
// =============================================================================

TEST_F(AggregationMergeReaderTest, BoolAndAggregation) {
  auto type = makeBoolSchema();
  // flag at index 2 → BOOL_AND

  auto b0 = makeBoolBatch(type, {1}, {1}, {true});
  auto b1 = makeBoolBatch(type, {1}, {2}, {true});
  auto b2 = makeBoolBatch(type, {1}, {3}, {false});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::BOOL_AND;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto flags = result->childAt(2)->asFlatVector<bool>();
  // BOOL_AND(false, true, true) = false
  EXPECT_EQ(flags->valueAt(0), false);
}

TEST_F(AggregationMergeReaderTest, BoolOrAggregation) {
  auto type = makeBoolSchema();

  auto b0 = makeBoolBatch(type, {1}, {1}, {false});
  auto b1 = makeBoolBatch(type, {1}, {2}, {false});
  auto b2 = makeBoolBatch(type, {1}, {3}, {true});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::BOOL_OR;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto flags = result->childAt(2)->asFlatVector<bool>();
  // BOOL_OR(true, false, false) = true
  EXPECT_EQ(flags->valueAt(0), true);
}

// =============================================================================
// AGGREGATION: LISTAGG — concatenates strings with comma separator
// =============================================================================

TEST_F(AggregationMergeReaderTest, ListaggAggregation) {
  auto type = makeStringSchema();

  auto b0 = makeStringBatch(type, {1}, {1}, {"alice"});
  auto b1 = makeStringBatch(type, {1}, {2}, {"bob"});
  auto b2 = makeStringBatch(type, {1}, {3}, {"charlie"});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::LISTAGG;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto names = result->childAt(2)->asFlatVector<StringView>();
  // Winner is seq=3 ("charlie"), then seq=2 ("bob"), then seq=1 ("alice")
  // LISTAGG: "charlie,bob,alice"
  EXPECT_EQ(names->valueAt(0).str(), "charlie,bob,alice");
}

// =============================================================================
// AGGREGATION: LAST_VALUE — keeps the latest value (from winner)
// =============================================================================

TEST_F(AggregationMergeReaderTest, LastValueDefaultForNonAggColumn) {
  auto type = makeSumCountSchema();
  // amount at index 2 → SUM, count at index 3 → no spec (defaults to LAST_VALUE)

  auto b0 = makeBatch(type, {1}, {1}, {100}, {10});
  auto b1 = makeBatch(type, {1}, {5}, {200}, {50});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;
  // index 3 has no spec → defaults to LAST_VALUE

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto amounts = result->childAt(2)->asFlatVector<int64_t>();
  auto counts = result->childAt(3)->asFlatVector<int64_t>();

  EXPECT_EQ(amounts->valueAt(0), 300);  // SUM: 200 + 100
  // LAST_VALUE: winner is seq=5 (val=50), older rows do NOT overwrite.
  EXPECT_EQ(counts->valueAt(0), 50);
}

// =============================================================================
// AGGREGATION: LAST_NON_NULL_VALUE — keeps latest non-null
// =============================================================================

TEST_F(AggregationMergeReaderTest, LastNonNullValueAggregation) {
  auto type = makeSimpleSchema();

  // Reader 0 (oldest): key=1 seq=1 value=100
  auto b0 = makeSimpleBatch(type, {1}, {1}, {100});
  // Reader 1 (middle): key=1 seq=3 value=null
  auto b1 = makeNullableSimpleBatch(type, {1}, {3}, {0}, {true});
  // Reader 2 (newest): key=1 seq=5 value=null
  auto b2 = makeNullableSimpleBatch(type, {1}, {5}, {0}, {true});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b2}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::LAST_NON_NULL_VALUE;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto vals = result->childAt(2)->asFlatVector<int64_t>();
  // Winner seq=5 is null, seq=3 is null, seq=1 is 100 → keeps 100
  EXPECT_FALSE(vals->isNullAt(0));
  EXPECT_EQ(vals->valueAt(0), 100);
}

// =============================================================================
// AGGREGATION: SUM with DOUBLE type
// =============================================================================

TEST_F(AggregationMergeReaderTest, SumDoubleAggregation) {
  auto type = makeDoubleSchema();

  auto b0 = makeDoubleBatch(type, {1}, {1}, {1.5});
  auto b1 = makeDoubleBatch(type, {1}, {2}, {2.5});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto vals = result->childAt(2)->asFlatVector<double>();
  EXPECT_DOUBLE_EQ(vals->valueAt(0), 4.0);  // 2.5 + 1.5
}

// =============================================================================
// AGGREGATION: multiple keys with different agg functions
// =============================================================================

TEST_F(AggregationMergeReaderTest, MultipleKeysWithAggregation) {
  auto type = makeSumCountSchema();

  // Reader 0: key=1 amount=10 count=1, key=2 amount=20 count=2
  auto b0 = makeBatch(type, {1, 2}, {1, 1}, {10, 20}, {1, 2});
  // Reader 1: key=1 amount=30 count=3, key=3 amount=50 count=5
  auto b1 = makeBatch(type, {1, 3}, {2, 1}, {30, 50}, {3, 5});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;
  agg_specs[3] = AggFunction::MAX;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 3);

  auto keys = result->childAt(0)->asFlatVector<int64_t>();
  auto amounts = result->childAt(2)->asFlatVector<int64_t>();
  auto counts = result->childAt(3)->asFlatVector<int64_t>();

  // key=1: SUM(30, 10)=40, MAX(3, 1)=3
  EXPECT_EQ(keys->valueAt(0), 1);
  EXPECT_EQ(amounts->valueAt(0), 40);
  EXPECT_EQ(counts->valueAt(0), 3);

  // key=2: only one version → amount=20, count=2
  EXPECT_EQ(keys->valueAt(1), 2);
  EXPECT_EQ(amounts->valueAt(1), 20);
  EXPECT_EQ(counts->valueAt(1), 2);

  // key=3: only one version → amount=50, count=5
  EXPECT_EQ(keys->valueAt(2), 3);
  EXPECT_EQ(amounts->valueAt(2), 50);
  EXPECT_EQ(counts->valueAt(2), 5);
}

// =============================================================================
// AGGREGATION: SUM with null values — null contributes nothing
// =============================================================================

TEST_F(AggregationMergeReaderTest, SumWithNullValues) {
  auto type = makeSimpleSchema();

  auto b0 = makeNullableSimpleBatch(type, {1}, {1}, {0}, {true});  // null
  auto b1 = makeSimpleBatch(type, {1}, {2}, {100});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));

  auto result = drainAll(merger);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 1);

  auto vals = result->childAt(2)->asFlatVector<int64_t>();
  // SUM(100, null) = 100
  EXPECT_FALSE(vals->isNullAt(0));
  EXPECT_EQ(vals->valueAt(0), 100);
}

// =============================================================================
// AGGREGATION: empty readers
// =============================================================================

TEST_F(AggregationMergeReaderTest, EmptyReaders) {
  auto type = makeSimpleSchema();
  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));
  EXPECT_FALSE(merger.hasNext());
  auto result = merger.next(100, pool_.get());
  EXPECT_EQ(result, nullptr);
}

// =============================================================================
// AGGREGATION: stats tracking
// =============================================================================

TEST_F(AggregationMergeReaderTest, StatsTracking) {
  auto type = makeSimpleSchema();

  auto b0 = makeSimpleBatch(type, {1, 2}, {1, 1}, {10, 20});
  auto b1 = makeSimpleBatch(type, {1}, {2}, {30});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
  merger.initialize(std::move(readers));
  drainAll(merger);

  auto stats = merger.getStats();
  EXPECT_EQ(stats.input_rows, 3);
  EXPECT_EQ(stats.output_rows, 2);
  EXPECT_EQ(stats.merged_rows, 1);
}

// =============================================================================
// AGGREGATION: close is idempotent
// =============================================================================

TEST_F(AggregationMergeReaderTest, CloseIsIdempotent) {
  auto type = makeSimpleSchema();
  std::unordered_map<column_index_t, AggFunction> agg_specs;
  AggregationMergeReader merger({0}, 1, type, agg_specs);
  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  merger.initialize(std::move(readers));

  merger.close();
  merger.close();
  merger.close();
}

// =============================================================================
// AGGREGATION: constructor rejects empty key indices
// =============================================================================

TEST_F(AggregationMergeReaderTest, RejectsEmptyKeyIndices) {
  auto type = makeSimpleSchema();
  std::unordered_map<column_index_t, AggFunction> agg_specs;
  EXPECT_THROW(
      AggregationMergeReader({}, 1, type, agg_specs),
      std::invalid_argument);
}

// =============================================================================
// AGGREGATION: output preserves primary key sort order
// =============================================================================

TEST_F(AggregationMergeReaderTest, OutputIsSortedByPrimaryKey) {
  auto type = makeSimpleSchema();

  auto b0 = makeSimpleBatch(type, {2, 4}, {1, 1}, {20, 40});
  auto b1 = makeSimpleBatch(type, {1, 3}, {1, 1}, {10, 30});

  std::vector<std::unique_ptr<RowReaderWrapper>> readers;
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b0}));
  readers.push_back(std::make_unique<TestRowReaderWrapper>(
      std::vector<RowVectorPtr>{b1}));

  std::unordered_map<column_index_t, AggFunction> agg_specs;
  agg_specs[2] = AggFunction::SUM;

  AggregationMergeReader merger({0}, 1, type, agg_specs);
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

}  // namespace
}  // namespace pangrova::merge::test
