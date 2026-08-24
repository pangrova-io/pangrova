// BatchVectorSerdeEquivalenceTest: Parameterized tests for Arrow IPC serde
// logical equivalence verification.
//
// Property 7: Arrow IPC Serde Logical Equivalence
//
// TEST_P over column types (int, bigint, varchar, decimal, array, map, row)
// × null patterns (no_nulls, some_nulls, all_nulls) × batch sizes (1, 10,
// 100, 1000).
// Verifies:
//   - Data serialized via ArrowBatchVectorSerde and deserialized back produces
//     logically identical RowVector (same schema, same values, same nulls).
//   - Equivalence holds across all supported column types and null patterns.
//   - Batch size does not affect correctness.
//
// **Validates: Requirements 3.2, 3.4**

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "result/ArrowBatchVectorSerde.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

using namespace facebook::velox;
using pangrova::result::ArrowBatchVectorSerde;

// ===========================================================================
// Test parameters: column type × null pattern × batch size
// ===========================================================================

enum class ColumnType {
  kInt,
  kBigint,
  kVarchar,
  kDecimal,
  kArray,
  kMap,
  kRow
};

enum class NullPattern { kNoNulls, kSomeNulls, kAllNulls };

struct SerdeEquivalenceParam {
  std::string name;
  ColumnType column_type;
  NullPattern null_pattern;
  int32_t batch_size;
};

std::string columnTypeName(ColumnType ct) {
  switch (ct) {
    case ColumnType::kInt: return "Int";
    case ColumnType::kBigint: return "Bigint";
    case ColumnType::kVarchar: return "Varchar";
    case ColumnType::kDecimal: return "Decimal";
    case ColumnType::kArray: return "Array";
    case ColumnType::kMap: return "Map";
    case ColumnType::kRow: return "Row";
  }
  return "Unknown";
}

std::string nullPatternName(NullPattern np) {
  switch (np) {
    case NullPattern::kNoNulls: return "NoNulls";
    case NullPattern::kSomeNulls: return "SomeNulls";
    case NullPattern::kAllNulls: return "AllNulls";
  }
  return "Unknown";
}

// ===========================================================================
// Helpers: create RowVectors for each column type × null pattern × batch size
// ===========================================================================

namespace {

// Determines if a row at given index should be null based on the pattern.
bool isNullForPattern(NullPattern pattern, int32_t index) {
  switch (pattern) {
    case NullPattern::kNoNulls: return false;
    case NullPattern::kSomeNulls: return (index % 3 == 1);
    case NullPattern::kAllNulls: return true;
  }
  return false;
}

RowVectorPtr makeIntVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  facebook::velox::test::VectorMaker maker(pool);
  std::vector<std::optional<int32_t>> values(batch_size);
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      values[i] = std::nullopt;
    } else {
      values[i] = i * 7 - 100;
    }
  }
  auto col = maker.flatVectorNullable<int32_t>(values);
  return maker.rowVector({"col_int"}, {col});
}

RowVectorPtr makeBigintVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  facebook::velox::test::VectorMaker maker(pool);
  std::vector<std::optional<int64_t>> values(batch_size);
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      values[i] = std::nullopt;
    } else {
      values[i] = static_cast<int64_t>(i) * 1000000L - 500000L;
    }
  }
  auto col = maker.flatVectorNullable<int64_t>(values);
  return maker.rowVector({"col_bigint"}, {col});
}

RowVectorPtr makeVarcharVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  facebook::velox::test::VectorMaker maker(pool);
  std::vector<std::optional<StringView>> values(batch_size);
  // Pre-generate string data to keep StringViews valid.
  std::vector<std::string> string_storage(batch_size);
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      values[i] = std::nullopt;
    } else {
      string_storage[i] = "str_" + std::to_string(i * 13);
      values[i] = StringView(string_storage[i]);
    }
  }
  auto col = maker.flatVectorNullable<StringView>(values);
  return maker.rowVector({"col_varchar"}, {col});
}

RowVectorPtr makeDecimalVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  // Use BIGINT to represent DECIMAL(18,2) unscaled values.
  // ArrowBatchVectorSerde handles DECIMAL via Arrow's decimal128 type.
  auto decimalType = DECIMAL(18, 2);
  auto vec = BaseVector::create(decimalType, batch_size, pool);
  auto flat = vec->asFlatVector<int64_t>();
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      vec->setNull(i, true);
    } else {
      flat->set(i, static_cast<int64_t>(i) * 123 - 5000);
    }
  }
  auto rowType = ROW({"col_decimal"}, {decimalType});
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, batch_size, std::vector<VectorPtr>{vec});
}

RowVectorPtr makeArrayVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  // Create ARRAY<BIGINT> with varying element counts using BaseVector API.
  auto arrayType = ARRAY(BIGINT());

  // Compute total elements needed.
  vector_size_t total_elements = 0;
  for (int32_t i = 0; i < batch_size; ++i) {
    if (!isNullForPattern(null_pattern, i)) {
      total_elements += (i % 5) + 1;
    }
  }

  // Build offsets and sizes buffers.
  auto offsets = AlignedBuffer::allocate<vector_size_t>(batch_size, pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(batch_size, pool);
  auto* raw_offsets = offsets->asMutable<vector_size_t>();
  auto* raw_sizes = sizes->asMutable<vector_size_t>();

  // Build nulls buffer.
  auto nulls = AlignedBuffer::allocate<bool>(batch_size, pool, bits::kNotNull);
  auto* raw_nulls = nulls->asMutable<uint64_t>();

  // Build elements vector.
  auto elements = BaseVector::create(BIGINT(), total_elements, pool);
  auto* elements_flat = elements->asFlatVector<int64_t>();

  vector_size_t offset = 0;
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      bits::setNull(raw_nulls, i, true);
      raw_offsets[i] = offset;
      raw_sizes[i] = 0;
    } else {
      int32_t len = (i % 5) + 1;
      raw_offsets[i] = offset;
      raw_sizes[i] = len;
      for (int32_t j = 0; j < len; ++j) {
        elements_flat->set(offset + j, static_cast<int64_t>(i * 10 + j));
      }
      offset += len;
    }
  }

  auto arrayVector = std::make_shared<ArrayVector>(
      pool, arrayType, nulls, batch_size, offsets, sizes, elements);

  auto rowType = ROW({"col_array"}, {arrayType});
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, batch_size,
      std::vector<VectorPtr>{arrayVector});
}

RowVectorPtr makeMapVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  // Create MAP<BIGINT, BIGINT> with varying entry counts using BaseVector API.
  auto mapType = MAP(BIGINT(), BIGINT());

  // Compute total entries needed.
  vector_size_t total_entries = 0;
  for (int32_t i = 0; i < batch_size; ++i) {
    if (!isNullForPattern(null_pattern, i)) {
      total_entries += (i % 3) + 1;
    }
  }

  // Build offsets and sizes buffers.
  auto offsets = AlignedBuffer::allocate<vector_size_t>(batch_size, pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(batch_size, pool);
  auto* raw_offsets = offsets->asMutable<vector_size_t>();
  auto* raw_sizes = sizes->asMutable<vector_size_t>();

  // Build nulls buffer.
  auto nulls = AlignedBuffer::allocate<bool>(batch_size, pool, bits::kNotNull);
  auto* raw_nulls = nulls->asMutable<uint64_t>();

  // Build keys and values vectors.
  auto mapKeys = BaseVector::create(BIGINT(), total_entries, pool);
  auto mapValues = BaseVector::create(BIGINT(), total_entries, pool);
  auto* keys_flat = mapKeys->asFlatVector<int64_t>();
  auto* values_flat = mapValues->asFlatVector<int64_t>();

  vector_size_t offset = 0;
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      bits::setNull(raw_nulls, i, true);
      raw_offsets[i] = offset;
      raw_sizes[i] = 0;
    } else {
      int32_t len = (i % 3) + 1;
      raw_offsets[i] = offset;
      raw_sizes[i] = len;
      for (int32_t j = 0; j < len; ++j) {
        keys_flat->set(offset + j, static_cast<int64_t>(i * 10 + j));
        values_flat->set(offset + j, static_cast<int64_t>(i * 100 + j));
      }
      offset += len;
    }
  }

  auto mapVector = std::make_shared<MapVector>(
      pool, mapType, nulls, batch_size, offsets, sizes, mapKeys, mapValues);

  auto rowType = ROW({"col_map"}, {mapType});
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, batch_size,
      std::vector<VectorPtr>{mapVector});
}

RowVectorPtr makeRowTypeVector(
    facebook::velox::memory::MemoryPool* pool,
    NullPattern null_pattern,
    int32_t batch_size) {
  facebook::velox::test::VectorMaker maker(pool);
  // Create ROW(BIGINT, VARCHAR) — a nested struct column.
  std::vector<std::optional<int64_t>> int_values(batch_size);
  std::vector<std::optional<StringView>> str_values(batch_size);
  std::vector<std::string> str_storage(batch_size);
  for (int32_t i = 0; i < batch_size; ++i) {
    if (isNullForPattern(null_pattern, i)) {
      int_values[i] = std::nullopt;
      str_values[i] = std::nullopt;
    } else {
      int_values[i] = static_cast<int64_t>(i * 42);
      str_storage[i] = "nested_" + std::to_string(i);
      str_values[i] = StringView(str_storage[i]);
    }
  }
  auto intChild = maker.flatVectorNullable<int64_t>(int_values);
  auto strChild = maker.flatVectorNullable<StringView>(str_values);

  auto innerRowType = ROW({"f_int", "f_str"}, {BIGINT(), VARCHAR()});
  auto innerRow = std::make_shared<RowVector>(
      pool, innerRowType, nullptr, batch_size,
      std::vector<VectorPtr>{intChild, strChild});

  // Set null at row level for null pattern.
  if (null_pattern != NullPattern::kNoNulls) {
    for (int32_t i = 0; i < batch_size; ++i) {
      if (isNullForPattern(null_pattern, i)) {
        innerRow->setNull(i, true);
      }
    }
  }

  auto outerRowType = ROW({"col_row"}, {innerRowType});
  return std::make_shared<RowVector>(
      pool, outerRowType, nullptr, batch_size,
      std::vector<VectorPtr>{innerRow});
}

RowVectorPtr makeTestVector(
    facebook::velox::memory::MemoryPool* pool,
    ColumnType column_type,
    NullPattern null_pattern,
    int32_t batch_size) {
  switch (column_type) {
    case ColumnType::kInt: return makeIntVector(pool, null_pattern, batch_size);
    case ColumnType::kBigint: return makeBigintVector(pool, null_pattern, batch_size);
    case ColumnType::kVarchar: return makeVarcharVector(pool, null_pattern, batch_size);
    case ColumnType::kDecimal: return makeDecimalVector(pool, null_pattern, batch_size);
    case ColumnType::kArray: return makeArrayVector(pool, null_pattern, batch_size);
    case ColumnType::kMap: return makeMapVector(pool, null_pattern, batch_size);
    case ColumnType::kRow: return makeRowTypeVector(pool, null_pattern, batch_size);
  }
  return nullptr;
}

// Serializes a RowVector using ArrowBatchVectorSerde and deserializes it back.
RowVectorPtr serdeRoundTrip(
    const RowVectorPtr& input,
    facebook::velox::memory::MemoryPool* pool) {
  ArrowBatchVectorSerde serde;

  // Serialize using BatchVectorSerializer.
  auto serializer = serde.createBatchSerializer(pool);

  // Create output stream to capture serialized bytes.
  auto outputStream = std::make_unique<IOBufOutputStream>(*pool);

  // Build index ranges covering all rows.
  IndexRange range{0, input->size()};
  std::vector<IndexRange> ranges = {range};
  serializer->serialize(
      input,
      folly::Range<const IndexRange*>(ranges.data(), ranges.size()),
      outputStream.get());

  // Get the serialized bytes.
  auto iobuf = outputStream->getIOBuf();

  // Deserialize back to RowVector.
  auto dataCopy = iobuf->clone();
  auto byteStream = std::make_unique<facebook::velox::BufferInputStream>(
      facebook::velox::byteRangesFromIOBuf(dataCopy.get()));

  auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
  RowVectorPtr result;
  serde.deserialize(byteStream.get(), pool, rowType, &result);
  return result;
}

// Compares two vectors element-by-element for logical equivalence.
void assertVectorsEqual(
    const VectorPtr& expected,
    const VectorPtr& actual,
    vector_size_t size,
    const std::string& context) {
  ASSERT_EQ(expected->size(), actual->size())
      << context << ": size mismatch";
  for (vector_size_t i = 0; i < size; ++i) {
    ASSERT_EQ(expected->isNullAt(i), actual->isNullAt(i))
        << context << ": null mismatch at row " << i;
    if (!expected->isNullAt(i)) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
          << context << ": value mismatch at row " << i;
    }
  }
}

}  // namespace

// ===========================================================================
// Test fixture
// ===========================================================================

class BatchVectorSerdeEquivalenceTest
    : public ::testing::TestWithParam<SerdeEquivalenceParam> {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("BatchVectorSerdeEquivalenceTest");
    // Serde may already be registered by initializeVeloxRuntime (via
    // testMemoryManager). Only register if not already present.
    if (!facebook::velox::isRegisteredNamedVectorSerde(
            ArrowBatchVectorSerde::kSerdeKind)) {
      ArrowBatchVectorSerde::registerNamedVectorSerde();
      owns_serde_registration_ = true;
    }
  }

  static void TearDownTestSuite() {
    // Only deregister if this test suite was responsible for registration.
    if (owns_serde_registration_) {
      ArrowBatchVectorSerde::deregisterNamedVectorSerde();
    }
    pool_.reset();
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  static bool owns_serde_registration_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    BatchVectorSerdeEquivalenceTest::pool_;
bool BatchVectorSerdeEquivalenceTest::owns_serde_registration_ = false;

// ===========================================================================
// Property 7.1: Serde Round-Trip Preserves Schema
//
// For any supported column type, the deserialized RowVector must have the
// same output type (column names and types) as the original input.
//
// **Validates: Requirements 3.2, 3.4**
// ===========================================================================

TEST_P(BatchVectorSerdeEquivalenceTest, RoundTripPreservesSchema) {
  const auto& param = GetParam();
  auto input = makeTestVector(
      pool_.get(), param.column_type, param.null_pattern, param.batch_size);
  ASSERT_NE(input, nullptr) << "Failed to create test vector for " << param.name;
  ASSERT_EQ(input->size(), param.batch_size);

  auto result = serdeRoundTrip(input, pool_.get());
  ASSERT_NE(result, nullptr) << "Deserialization returned null for " << param.name;

  // Schema must be equivalent.
  auto input_type = input->type();
  auto result_type = result->type();
  ASSERT_EQ(input_type->size(), result_type->size())
      << "Column count mismatch for " << param.name;
  for (size_t i = 0; i < input_type->size(); ++i) {
    EXPECT_TRUE(input_type->childAt(i)->equivalent(*result_type->childAt(i)))
        << "Column type at index " << i << " differs for " << param.name;
  }
}

// ===========================================================================
// Property 7.2: Serde Round-Trip Preserves Row Count
//
// The deserialized RowVector must have the same number of rows as the input.
//
// **Validates: Requirements 3.2, 3.4**
// ===========================================================================

TEST_P(BatchVectorSerdeEquivalenceTest, RoundTripPreservesRowCount) {
  const auto& param = GetParam();
  auto input = makeTestVector(
      pool_.get(), param.column_type, param.null_pattern, param.batch_size);
  ASSERT_NE(input, nullptr);

  auto result = serdeRoundTrip(input, pool_.get());
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), input->size())
      << "Row count mismatch for " << param.name;
}

// ===========================================================================
// Property 7.3: Serde Round-Trip Preserves Values and Nulls
//
// For any valid RowVector containing supported types, the data deserialized
// from ArrowBatchVectorSerde must be logically identical to the original:
// same values at each position, same null positions.
//
// **Validates: Requirements 3.2, 3.4**
// ===========================================================================

TEST_P(BatchVectorSerdeEquivalenceTest, RoundTripPreservesValuesAndNulls) {
  const auto& param = GetParam();
  auto input = makeTestVector(
      pool_.get(), param.column_type, param.null_pattern, param.batch_size);
  ASSERT_NE(input, nullptr);

  auto result = serdeRoundTrip(input, pool_.get());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->size(), input->size());

  // Compare each column element-by-element.
  for (size_t col = 0; col < input->type()->size(); ++col) {
    auto expected_col = input->childAt(col);
    auto actual_col = result->childAt(col);
    assertVectorsEqual(
        expected_col, actual_col, input->size(),
        param.name + " col=" + std::to_string(col));
  }
}

// ===========================================================================
// Parameterization: column types × null patterns × batch sizes
// ===========================================================================

std::vector<SerdeEquivalenceParam> generateParams() {
  std::vector<SerdeEquivalenceParam> params;
  std::vector<ColumnType> types = {
      ColumnType::kInt, ColumnType::kBigint, ColumnType::kVarchar,
      ColumnType::kDecimal, ColumnType::kArray, ColumnType::kMap,
      ColumnType::kRow};
  std::vector<NullPattern> patterns = {
      NullPattern::kNoNulls, NullPattern::kSomeNulls, NullPattern::kAllNulls};
  std::vector<int32_t> sizes = {1, 10, 100, 1000};

  for (auto type : types) {
    for (auto pattern : patterns) {
      for (auto size : sizes) {
        std::string name = columnTypeName(type) + "_" +
            nullPatternName(pattern) + "_" + std::to_string(size);
        params.push_back({name, type, pattern, size});
      }
    }
  }
  return params;
}

INSTANTIATE_TEST_SUITE_P(
    BatchSerdeEquivalence,
    BatchVectorSerdeEquivalenceTest,
    ::testing::ValuesIn(generateParams()),
    [](const ::testing::TestParamInfo<SerdeEquivalenceParam>& info) {
      return info.param.name;
    });

}  // namespace
}  // namespace pangrova::velox::test
