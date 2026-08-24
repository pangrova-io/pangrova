// DeletionVectorRowFilterTest: Unit tests for Velox-native deletion vector
// filter using Roaring Bitmap on RowVector + SelectivityVector.
// Validates: Requirements R5.1, R5.2, R5.3

#include <gtest/gtest.h>

#include <roaring.hh>

#include "../TestMemoryHelper.h"
#include "delete/DeletionVectorRowFilter.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

namespace pangrova::delete_filter {
namespace {

using namespace facebook::velox;

// Helper: create a FlatVector<int32_t> with given values.
template <typename T>
VectorPtr makeFlatVector(
    facebook::velox::memory::MemoryPool* pool,
    const std::vector<T>& values) {
  auto vector = BaseVector::create(CppToType<T>::create(), values.size(), pool);
  auto flat = vector->template asFlatVector<T>();
  for (size_t i = 0; i < values.size(); ++i) {
    flat->set(i, values[i]);
  }
  return vector;
}

// Helper: create a simple RowVector with a single int32 column.
RowVectorPtr makeRowVector(facebook::velox::memory::MemoryPool* pool, int32_t num_rows) {
  std::vector<int32_t> values(num_rows);
  for (int32_t i = 0; i < num_rows; ++i) {
    values[i] = i;
  }
  auto col = makeFlatVector<int32_t>(pool, values);
  auto rowType = ROW({"id"}, {INTEGER()});
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, num_rows, std::vector<VectorPtr>{col});
}

class DeletionVectorRowFilterTest : public ::testing::Test {
 protected:
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("velox_dv_filter_test");
  }

  facebook::velox::memory::MemoryPool* pool() { return pool_.get(); }
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    DeletionVectorRowFilterTest::pool_;

// Basic: delete specific rows at positions 1, 3, 5 in a 10-row batch.
TEST_F(DeletionVectorRowFilterTest, BasicFiltering) {
  roaring::Roaring bitmap;
  bitmap.add(1);
  bitmap.add(3);
  bitmap.add(5);

  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 10);
  SelectivityVector rows(batch->size(), true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  // Rows 1, 3, 5 should be invalid.
  EXPECT_TRUE(rows.isValid(0));
  EXPECT_FALSE(rows.isValid(1));
  EXPECT_TRUE(rows.isValid(2));
  EXPECT_FALSE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
  EXPECT_FALSE(rows.isValid(5));
  EXPECT_TRUE(rows.isValid(6));
}

// Empty bitmap should leave all rows valid.
TEST_F(DeletionVectorRowFilterTest, EmptyBitmapNoDeletes) {
  roaring::Roaring bitmap;
  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(rows.isValid(i));
  }
}

// All rows deleted.
TEST_F(DeletionVectorRowFilterTest, AllRowsDeleted) {
  roaring::Roaring bitmap;
  for (uint32_t i = 0; i < 5; ++i) {
    bitmap.add(i);
  }

  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_FALSE(rows.isValid(i));
  }
}

// Batch starts at row_offset=100. Delete global positions 101 and 103.
TEST_F(DeletionVectorRowFilterTest, OffsetRowIndex) {
  roaring::Roaring bitmap;
  bitmap.add(101);
  bitmap.add(103);

  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);

  filter.apply(*batch, rows, /*row_offset=*/100);

  // Local indices: 0→100(valid), 1→101(deleted), 2→102(valid),
  //                3→103(deleted), 4→104(valid)
  EXPECT_TRUE(rows.isValid(0));
  EXPECT_FALSE(rows.isValid(1));
  EXPECT_TRUE(rows.isValid(2));
  EXPECT_FALSE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
}

// Bitmap positions outside the batch range should not affect any rows.
TEST_F(DeletionVectorRowFilterTest, BitmapOutsideBatchRange) {
  roaring::Roaring bitmap;
  bitmap.add(100);
  bitmap.add(200);

  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);

  filter.apply(*batch, rows, /*row_offset=*/0);

  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(rows.isValid(i));
  }
}

// Already-invalid rows should remain invalid (no resurrection).
TEST_F(DeletionVectorRowFilterTest, PreInvalidRowsPreserved) {
  roaring::Roaring bitmap;
  bitmap.add(2);

  DeletionVectorRowFilter filter(std::move(bitmap));
  auto batch = makeRowVector(pool(), 5);
  SelectivityVector rows(batch->size(), true);
  // Pre-invalidate row 0.
  rows.setValid(0, false);
  rows.updateBounds();

  filter.apply(*batch, rows, /*row_offset=*/0);

  EXPECT_FALSE(rows.isValid(0));  // still invalid
  EXPECT_TRUE(rows.isValid(1));
  EXPECT_FALSE(rows.isValid(2));  // newly invalidated by DV
  EXPECT_TRUE(rows.isValid(3));
  EXPECT_TRUE(rows.isValid(4));
}

}  // namespace
}  // namespace pangrova::delete_filter
