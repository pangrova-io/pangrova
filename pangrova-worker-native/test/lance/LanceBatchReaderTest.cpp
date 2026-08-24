// LanceBatchReaderTest: GoogleTest unit tests for LanceBatchReader.
//
// Tests cover:
//   - Construction validation (null pool, null type, empty path)
//   - Reading pre-created .lance files with correct row counts
//   - Column projection (only requested columns returned)
//   - Predicate pushdown (filter expression reduces output rows)
//   - VECTOR(dim) type mapping to FixedSizeList<Float32>
//   - Exhaustion semantics (hasNext/next after end-of-dataset)
//
// Requirements: 1.3, 1.6, 1.8, 1.9

#include <gtest/gtest.h>

#if PANGROVA_LANCE_AVAILABLE

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "lance/LanceBatchReader.h"
#include "lance/LanceBatchWriter.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::lance::test {
namespace {

using namespace facebook::velox;

// ---------------------------------------------------------------------------
// Test fixture: creates temporary Lance datasets for reader tests.
// ---------------------------------------------------------------------------

class LanceBatchReaderTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("LanceBatchReaderTest");
  }

  void SetUp() override {
    tmpDir_ = std::filesystem::temp_directory_path() /
        ("lance_reader_test_" + std::to_string(std::rand()));
    std::filesystem::create_directories(tmpDir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(tmpDir_);
  }

  // Write a dataset with id (BIGINT) and name (VARCHAR) columns.
  std::string writeSimpleDataset(int numRows) {
    auto path = (tmpDir_ / "simple.lance").string();
    auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

    LanceWriterConfig config;
    config.outputPath = path;
    config.append = false;

    LanceBatchWriter writer(pool_.get(), outputType, config);

    auto idVector = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto nameVector = BaseVector::create(VARCHAR(), numRows, pool_.get());
    auto idFlat = idVector->asFlatVector<int64_t>();
    auto nameFlat = nameVector->asFlatVector<StringView>();

    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
      std::string name = "row_" + std::to_string(i + 1);
      nameFlat->set(i, StringView(name));
    }

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVector, nameVector});

    writer.writeBatch(batch);
    writer.finish();
    return path;
  }

  // Write a dataset with id, category (VARCHAR), and score (DOUBLE).
  std::string writeFilterableDataset() {
    auto path = (tmpDir_ / "filterable.lance").string();
    auto outputType =
        ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()});

    LanceWriterConfig config;
    config.outputPath = path;
    config.append = false;

    LanceBatchWriter writer(pool_.get(), outputType, config);

    constexpr int numRows = 10;
    auto idVec = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto catVec = BaseVector::create(VARCHAR(), numRows, pool_.get());
    auto scoreVec = BaseVector::create(DOUBLE(), numRows, pool_.get());

    auto idFlat = idVec->asFlatVector<int64_t>();
    auto catFlat = catVec->asFlatVector<StringView>();
    auto scoreFlat = scoreVec->asFlatVector<double>();

    const char* categories[] = {"tech", "science", "art"};
    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
      catFlat->set(i, StringView(categories[i % 3]));
      scoreFlat->set(i, 50.0 + i * 10.0);
    }

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVec, catVec, scoreVec});

    writer.writeBatch(batch);
    writer.finish();
    return path;
  }

  // Write a dataset with id (BIGINT) and embedding (ARRAY<REAL> dim=4).
  // VECTOR(dim) maps to Lance FixedSizeList<Float32>, represented in Velox
  // as ARRAY(REAL()) with fixed-size semantics enforced by the reader.
  std::string writeVectorDataset() {
    auto path = (tmpDir_ / "vector.lance").string();
    auto vecType = ARRAY(REAL());
    auto outputType = ROW({"id", "embedding"}, {BIGINT(), vecType});

    LanceWriterConfig config;
    config.outputPath = path;
    config.append = false;

    LanceBatchWriter writer(pool_.get(), outputType, config);

    constexpr int numRows = 3;
    auto idVec = BaseVector::create(BIGINT(), numRows, pool_.get());
    auto idFlat = idVec->asFlatVector<int64_t>();
    for (int i = 0; i < numRows; ++i) {
      idFlat->set(i, static_cast<int64_t>(i + 1));
    }

    // Build FixedSizeArray vector for embeddings.
    auto elementsVec =
        BaseVector::create(REAL(), numRows * 4, pool_.get());
    auto elemFlat = elementsVec->asFlatVector<float>();
    for (int i = 0; i < numRows * 4; ++i) {
      elemFlat->set(i, static_cast<float>(i) * 0.1f);
    }

    auto offsets =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto sizes =
        AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      rawOffsets[i] = i * 4;
      rawSizes[i] = 4;
    }

    auto arrayVec = std::make_shared<ArrayVector>(
        pool_.get(), vecType, nullptr, numRows,
        offsets, sizes, elementsVec);

    auto batch = std::make_shared<RowVector>(
        pool_.get(), outputType, nullptr, numRows,
        std::vector<VectorPtr>{idVec, arrayVec});

    writer.writeBatch(batch);
    writer.finish();
    return path;
  }

  // Fully qualified: the project's pangrova::memory namespace shadows the
  // `memory` alias pulled in by `using namespace facebook::velox`.
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::filesystem::path tmpDir_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool> LanceBatchReaderTest::pool_;

// ---------------------------------------------------------------------------
// Construction validation
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, RejectsNullMemoryPool) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      LanceBatchReader(nullptr, outputType, "/tmp/fake.lance", {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsNullOutputType) {
  EXPECT_THROW(
      LanceBatchReader(pool_.get(), nullptr, "/tmp/fake.lance", {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsEmptyFilePath) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      LanceBatchReader(pool_.get(), outputType, "", {}),
      std::invalid_argument);
}

TEST_F(LanceBatchReaderTest, RejectsNonexistentFile) {
  auto outputType = ROW({"id"}, {BIGINT()});
  EXPECT_THROW(
      LanceBatchReader(
          pool_.get(), outputType, "/nonexistent/path/data.lance", {}),
      std::runtime_error);
}

// ---------------------------------------------------------------------------
// Basic read: correct row count and values
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ReadsAllRowsFromSimpleDataset) {
  auto path = writeSimpleDataset(5);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});

  EXPECT_TRUE(reader.hasNext());

  int64_t totalRows = 0;
  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
    totalRows += batch->size();
  }

  EXPECT_EQ(totalRows, 5);
  EXPECT_FALSE(reader.hasNext());
}

TEST_F(LanceBatchReaderTest, ReadsCorrectValuesFromSimpleDataset) {
  auto path = writeSimpleDataset(3);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);
  EXPECT_EQ(idCol->valueAt(1), 2);
  EXPECT_EQ(idCol->valueAt(2), 3);

  auto nameCol = batch->childAt(1)->asFlatVector<StringView>();
  EXPECT_EQ(nameCol->valueAt(0).str(), "row_1");
  EXPECT_EQ(nameCol->valueAt(1).str(), "row_2");
  EXPECT_EQ(nameCol->valueAt(2).str(), "row_3");
}

// ---------------------------------------------------------------------------
// Column projection
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ColumnProjectionReadsOnlyRequestedColumns) {
  auto path = writeFilterableDataset();
  auto outputType = ROW({"id", "score"}, {BIGINT(), DOUBLE()});

  LanceBatchReader reader(
      pool_.get(), outputType, path, {"id", "score"});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->childrenSize(), 2);
  EXPECT_EQ(batch->size(), 10);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);

  auto scoreCol = batch->childAt(1)->asFlatVector<double>();
  EXPECT_EQ(scoreCol->valueAt(0), 50.0);
}

// ---------------------------------------------------------------------------
// Predicate pushdown
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, FilterReducesOutputRows) {
  auto path = writeFilterableDataset();
  auto outputType =
      ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});
  reader.setFilter("score > 100.0");

  int64_t totalRows = 0;
  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
    totalRows += batch->size();
  }

  // score values: 50,60,70,80,90,100,110,120,130,140
  // score > 100: 110,120,130,140 = 4 rows
  EXPECT_EQ(totalRows, 4);
}

TEST_F(LanceBatchReaderTest, FilterByStringEquality) {
  auto path = writeFilterableDataset();
  auto outputType =
      ROW({"id", "category", "score"}, {BIGINT(), VARCHAR(), DOUBLE()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});
  reader.setFilter("category = 'tech'");

  int64_t totalRows = 0;
  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
    totalRows += batch->size();

    auto catCol = batch->childAt(1)->asFlatVector<StringView>();
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      EXPECT_EQ(catCol->valueAt(i).str(), "tech");
    }
  }

  // Pattern: tech, science, art repeating. Indices 0,3,6,9 = 4 rows.
  EXPECT_EQ(totalRows, 4);
}

// ---------------------------------------------------------------------------
// VECTOR type (FixedSizeList<Float32>)
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, ReadsVectorTypeAsFixedSizeArray) {
  auto path = writeVectorDataset();
  auto vecType = ARRAY(REAL());
  auto outputType = ROW({"id", "embedding"}, {BIGINT(), vecType});

  LanceBatchReader reader(pool_.get(), outputType, path, {});

  auto batch = reader.next(1024);
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 3);

  auto idCol = batch->childAt(0)->asFlatVector<int64_t>();
  EXPECT_EQ(idCol->valueAt(0), 1);
  EXPECT_EQ(idCol->valueAt(1), 2);
  EXPECT_EQ(idCol->valueAt(2), 3);

  auto embCol = batch->childAt(1)->as<ArrayVector>();
  ASSERT_NE(embCol, nullptr);
  EXPECT_EQ(embCol->sizeAt(0), 4);
  EXPECT_EQ(embCol->sizeAt(1), 4);
  EXPECT_EQ(embCol->sizeAt(2), 4);

  // First row embedding: [0.0, 0.1, 0.2, 0.3]
  auto elements = embCol->elements()->asFlatVector<float>();
  EXPECT_FLOAT_EQ(elements->valueAt(0), 0.0f);
  EXPECT_FLOAT_EQ(elements->valueAt(1), 0.1f);
  EXPECT_FLOAT_EQ(elements->valueAt(2), 0.2f);
  EXPECT_FLOAT_EQ(elements->valueAt(3), 0.3f);
}

// ---------------------------------------------------------------------------
// Exhaustion semantics
// ---------------------------------------------------------------------------

TEST_F(LanceBatchReaderTest, NextReturnsNullAfterExhaustion) {
  auto path = writeSimpleDataset(2);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});

  while (reader.hasNext()) {
    auto batch = reader.next(1024);
    if (!batch) {
      break;
    }
  }

  EXPECT_FALSE(reader.hasNext());
  EXPECT_EQ(reader.next(1024), nullptr);
}

TEST_F(LanceBatchReaderTest, BatchSizeLimitsRowsPerCall) {
  auto path = writeSimpleDataset(10);
  auto outputType = ROW({"id", "name"}, {BIGINT(), VARCHAR()});

  LanceBatchReader reader(pool_.get(), outputType, path, {});

  auto batch = reader.next(3);
  ASSERT_NE(batch, nullptr);
  EXPECT_LE(batch->size(), 3);
}

}  // namespace
}  // namespace pangrova::lance::test

#else  // !PANGROVA_LANCE_AVAILABLE

#include <gtest/gtest.h>

TEST(LanceBatchReaderTest, SkippedWhenLanceUnavailable) {
  GTEST_SKIP() << "Lance C FFI not available";
}

#endif  // PANGROVA_LANCE_AVAILABLE
