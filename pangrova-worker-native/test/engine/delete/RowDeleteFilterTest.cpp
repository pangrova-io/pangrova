// RowDeleteFilterTest: Unit tests for PositionDeleteFilter and
// EqualityDeleteFilter operating on Velox RowVector + SelectivityVector.
// Validates: Requirements R5.1, R5.4, R5.5
//
// Tests cover position-based deletion with offsets, equality-based deletion
// with composite keys, edge cases (empty filters, missing columns, nulls).

#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/delete/RowDeleteFilter.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::delete_filter::test {
namespace {

using namespace facebook::velox;

class RowDeleteFilterTest : public ::testing::Test {
 protected:
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("velox_delete_filter_test");
  }

  facebook::velox::memory::MemoryPool* pool() { return pool_.get(); }

  // Helper: create RowVector with (id: INT, name: VARCHAR).
  RowVectorPtr makeIdNameBatch(
      const std::vector<int32_t>& ids,
      const std::vector<std::string>& names) {
    auto idVec = BaseVector::create(INTEGER(), ids.size(), pool());
    auto idFlat = idVec->asFlatVector<int32_t>();
    for (size_t i = 0; i < ids.size(); ++i) {
      idFlat->set(i, ids[i]);
    }

    auto nameVec = BaseVector::create(VARCHAR(), names.size(), pool());
    auto nameFlat = nameVec->asFlatVector<StringView>();
    for (size_t i = 0; i < names.size(); ++i) {
      nameFlat->set(i, StringView(names[i]));
    }

    auto rowType = ROW({"id", "name"}, {INTEGER(), VARCHAR()});
    return std::make_shared<RowVector>(
        pool(), rowType, nullptr, ids.size(),
        std::vector<VectorPtr>{idVec, nameVec});
  }

  // Helper: create simple int-only RowVector.
  RowVectorPtr makeIntBatch(int32_t numRows) {
    auto vec = BaseVector::create(INTEGER(), numRows, pool());
    auto flat = vec->asFlatVector<int32_t>();
    for (int32_t i = 0; i < numRows; ++i) {
      flat->set(i, i * 10);
    }
    auto rowType = ROW({"val"}, {INTEGER()});
    return std::make_shared<RowVector>(
        pool(), rowType, nullptr, numRows,
        std::vector<VectorPtr>{vec});
  }
};

std::shared_ptr<facebook::velox::memory::MemoryPool> RowDeleteFilterTest::pool_;

// =====================================================================
// PositionDeleteFilter
// =====================================================================

TEST_F(RowDeleteFilterTest, PositionDeleteBasic) {
  PositionDeleteFilter filter({2, 4, 7});
  auto batch = makeIntBatch(10);
  SelectivityVector rows(10, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_TRUE(rows.isValid(0));
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_FALSE(rows.isValid(2));
  EXPECT_TRUE(rows.isValid(3));
  EXPECT_FALSE(rows.isValid(4));
  EXPECT_TRUE(rows.isValid(5));
  EXPECT_TRUE(rows.isValid(6));
  EXPECT_FALSE(rows.isValid(7));
  EXPECT_TRUE(rows.isValid(8));
  EXPECT_TRUE(rows.isValid(9));
}

TEST_F(RowDeleteFilterTest, PositionDeleteWithOffset) {
  // Global positions 102, 105 → local indices 2, 5 with offset=100.
  PositionDeleteFilter filter({102, 105});
  auto batch = makeIntBatch(8);
  SelectivityVector rows(8, true);

  filter.apply(*batch, rows, /*row_offset=*/100);

  EXPECT_TRUE(rows.isValid(0));
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_FALSE(rows.isValid(2));
  EXPECT_TRUE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
  EXPECT_FALSE(rows.isValid(5));
  EXPECT_TRUE(rows.isValid(6));
  EXPECT_TRUE(rows.isValid(7));
}

TEST_F(RowDeleteFilterTest, PositionDeleteEmpty) {
  PositionDeleteFilter filter({});
  auto batch = makeIntBatch(5);
  SelectivityVector rows(5, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(rows.isValid(i));
  }
}

TEST_F(RowDeleteFilterTest, PositionDeleteAllRows) {
  PositionDeleteFilter filter({0, 1, 2, 3, 4});
  auto batch = makeIntBatch(5);
  SelectivityVector rows(5, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_FALSE(rows.isValid(i));
  }
}

TEST_F(RowDeleteFilterTest, PositionDeleteOutOfRange) {
  // Positions 100, 200 are outside batch range [0, 5).
  PositionDeleteFilter filter({100, 200});
  auto batch = makeIntBatch(5);
  SelectivityVector rows(5, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(rows.isValid(i));
  }
}

TEST_F(RowDeleteFilterTest, PositionDeleteUnsortedInput) {
  // Constructor should sort positions internally.
  PositionDeleteFilter filter({7, 2, 4});
  auto batch = makeIntBatch(10);
  SelectivityVector rows(10, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(2));
  EXPECT_FALSE(rows.isValid(4));
  EXPECT_FALSE(rows.isValid(7));
  EXPECT_EQ(rows.countSelected(), 7);
}

TEST_F(RowDeleteFilterTest, PositionDeletePreservesPriorInvalid) {
  PositionDeleteFilter filter({3});
  auto batch = makeIntBatch(5);
  SelectivityVector rows(5, true);
  rows.setValid(0, false);
  rows.updateBounds();

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));  // still invalid
  EXPECT_FALSE(rows.isValid(3));  // newly deleted
  EXPECT_EQ(rows.countSelected(), 3);
}

// =====================================================================
// EqualityDeleteFilter
// =====================================================================

TEST_F(RowDeleteFilterTest, EqualityDeleteSingleColumn) {
  // Delete rows where name = "Alice" or name = "Charlie".
  // Key format: just the column value (single column, no separator).
  std::unordered_set<std::string> deletedKeys = {"Alice", "Charlie"};
  EqualityDeleteFilter filter({"name"}, std::move(deletedKeys));

  auto batch = makeIdNameBatch(
      {1, 2, 3, 4, 5},
      {"Alice", "Bob", "Charlie", "Diana", "Eve"});
  SelectivityVector rows(5, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));  // Alice
  EXPECT_TRUE(rows.isValid(1));   // Bob
  EXPECT_FALSE(rows.isValid(2));  // Charlie
  EXPECT_TRUE(rows.isValid(3));   // Diana
  EXPECT_TRUE(rows.isValid(4));   // Eve
}

TEST_F(RowDeleteFilterTest, EqualityDeleteCompositeKey) {
  // Delete rows where (id, name) = (1, "Alice") or (3, "Charlie").
  // Composite key uses null separator: "1\0Alice", "3\0Charlie".
  std::unordered_set<std::string> deletedKeys;
  deletedKeys.insert(std::string("1") + '\0' + "Alice");
  deletedKeys.insert(std::string("3") + '\0' + "Charlie");
  EqualityDeleteFilter filter({"id", "name"}, std::move(deletedKeys));

  auto batch = makeIdNameBatch(
      {1, 2, 3, 1, 3},
      {"Alice", "Bob", "Charlie", "Bob", "Alice"});
  SelectivityVector rows(5, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));  // (1, Alice) → deleted
  EXPECT_TRUE(rows.isValid(1));   // (2, Bob) → kept
  EXPECT_FALSE(rows.isValid(2));  // (3, Charlie) → deleted
  EXPECT_TRUE(rows.isValid(3));   // (1, Bob) → kept
  EXPECT_TRUE(rows.isValid(4));   // (3, Alice) → kept
}

TEST_F(RowDeleteFilterTest, EqualityDeleteEmptyKeys) {
  std::unordered_set<std::string> deletedKeys;
  EqualityDeleteFilter filter({"name"}, std::move(deletedKeys));

  auto batch = makeIdNameBatch({1, 2}, {"Alice", "Bob"});
  SelectivityVector rows(2, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_TRUE(rows.isValid(0));
  EXPECT_TRUE(rows.isValid(1));
}

TEST_F(RowDeleteFilterTest, EqualityDeleteEmptyColumns) {
  std::unordered_set<std::string> deletedKeys = {"anything"};
  EqualityDeleteFilter filter({}, std::move(deletedKeys));

  auto batch = makeIdNameBatch({1}, {"Alice"});
  SelectivityVector rows(1, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  // Empty columns → no filtering.
  EXPECT_TRUE(rows.isValid(0));
}

TEST_F(RowDeleteFilterTest, EqualityDeleteMissingColumn) {
  std::unordered_set<std::string> deletedKeys = {"val"};
  EqualityDeleteFilter filter({"nonexistent_col"}, std::move(deletedKeys));

  auto batch = makeIdNameBatch({1, 2}, {"Alice", "Bob"});
  SelectivityVector rows(2, true);

  EXPECT_THROW(filter.apply(*batch, rows, /*row_offset=*/0), std::runtime_error);
}

TEST_F(RowDeleteFilterTest, EqualityDeleteAcceptsConstantEncodedColumns) {
  auto batch = makeIdNameBatch({1, 2}, {"Alice", "Bob"});
  batch->childAt(0) = BaseVector::wrapInConstant(2, 0, batch->childAt(0));
  EqualityDeleteFilter filter({"id"}, {"1"});
  SelectivityVector rows(2, true);
  filter.apply(*batch, rows, 0);
  EXPECT_FALSE(rows.isValid(0));
  EXPECT_FALSE(rows.isValid(1));
}

TEST_F(RowDeleteFilterTest, EqualityDeleteAllRowsMatch) {
  std::unordered_set<std::string> deletedKeys = {"Alice", "Bob"};
  EqualityDeleteFilter filter({"name"}, std::move(deletedKeys));

  auto batch = makeIdNameBatch({1, 2}, {"Alice", "Bob"});
  SelectivityVector rows(2, true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));
  EXPECT_FALSE(rows.isValid(1));
}

}  // namespace
}  // namespace pangrova::delete_filter::test
