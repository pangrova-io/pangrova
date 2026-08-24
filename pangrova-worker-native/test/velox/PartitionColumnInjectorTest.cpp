// VeloxPartitionColumnInjectorTest: Unit tests for partition column injection.
// Validates: Requirements R4.5, R12.4

#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "velox/connector/PartitionColumnInjector.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

using namespace facebook::velox;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

RowVectorPtr makeRowVector(
    facebook::velox::memory::MemoryPool* pool,
    const std::vector<std::string>& names,
    const std::vector<VectorPtr>& children) {
  std::vector<std::shared_ptr<const Type>> types;
  types.reserve(children.size());
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  auto rowType = ROW(std::vector<std::string>(names), std::move(types));
  auto size = children.empty() ? 0 : children[0]->size();
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, children);
}

// Extract a scalar value from a vector at a given index, handling both
// flat and constant encodings transparently.
template <typename T>
T extractValue(const VectorPtr& vec, vector_size_t idx) {
  auto flat = vec->as<SimpleVector<T>>();
  return flat->valueAt(idx);
}

class VeloxPartitionColumnInjectorTest : public ::testing::Test {
 protected:
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool(
        "partition_column_injector_test");
  }

  facebook::velox::memory::MemoryPool* pool() { return pool_.get(); }
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    VeloxPartitionColumnInjectorTest::pool_;

// ---------------------------------------------------------------------------
// Zero-overhead fast path
// ---------------------------------------------------------------------------

TEST_F(VeloxPartitionColumnInjectorTest, EmptyPartitionValuesReturnsInput) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col});
  auto output_type = ROW({"a"}, {INTEGER()});

  std::unordered_map<std::string, std::string> empty_pv;
  auto result = injectPartitionColumns(input, empty_pv, output_type, pool());

  // Should return the exact same pointer (zero overhead).
  EXPECT_EQ(result.get(), input.get());
}

TEST_F(VeloxPartitionColumnInjectorTest, AllColumnsExistReturnsInput) {
  // All output columns already present in input — no injection needed.
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2});
  auto col_b = makeFlatVector<int64_t>(pool(), {10, 20});
  auto input = makeRowVector(pool(), {"a", "b"}, {col_a, col_b});
  auto output_type = ROW({"a", "b"}, {INTEGER(), BIGINT()});

  std::unordered_map<std::string, std::string> pv = {{"c", "unused"}};
  auto result = injectPartitionColumns(input, pv, output_type, pool());

  EXPECT_EQ(result.get(), input.get());
}

// ---------------------------------------------------------------------------
// Partition column injection with type conversion
// ---------------------------------------------------------------------------

TEST_F(VeloxPartitionColumnInjectorTest, InjectIntegerPartitionColumn) {
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  // Output wants [a, part_col] where part_col is an INTEGER partition column.
  auto output_type = ROW({"a", "part_col"}, {INTEGER(), INTEGER()});
  std::unordered_map<std::string, std::string> pv = {{"part_col", "42"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(result->type()->size(), 2);
  EXPECT_EQ(result->type()->asRow().nameOf(0), "a");
  EXPECT_EQ(result->type()->asRow().nameOf(1), "part_col");

  // Data column preserved.
  EXPECT_EQ(extractValue<int32_t>(result->childAt(0), 0), 1);

  // Partition column: constant 42 for all rows.
  auto part_col = result->childAt(1);
  EXPECT_TRUE(part_col->isConstantEncoding());
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(part_col->isNullAt(i));
    EXPECT_EQ(extractValue<int32_t>(part_col, i), 42);
  }
}

TEST_F(VeloxPartitionColumnInjectorTest, InjectBigintPartitionColumn) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"x"}, {col});

  auto output_type = ROW({"x", "year"}, {INTEGER(), BIGINT()});
  std::unordered_map<std::string, std::string> pv = {{"year", "2024"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  ASSERT_EQ(result->type()->size(), 2);
  auto year_col = result->childAt(1);
  EXPECT_TRUE(year_col->isConstantEncoding());
  EXPECT_EQ(extractValue<int64_t>(year_col, 0), 2024);
}

TEST_F(VeloxPartitionColumnInjectorTest, InjectDoublePartitionColumn) {
  auto col = makeFlatVector<int32_t>(pool(), {1});
  auto input = makeRowVector(pool(), {"id"}, {col});

  auto output_type = ROW({"id", "ratio"}, {INTEGER(), DOUBLE()});
  std::unordered_map<std::string, std::string> pv = {{"ratio", "3.14"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  auto ratio_col = result->childAt(1);
  EXPECT_TRUE(ratio_col->isConstantEncoding());
  EXPECT_DOUBLE_EQ(extractValue<double>(ratio_col, 0), 3.14);
}

TEST_F(VeloxPartitionColumnInjectorTest, InjectVarcharPartitionColumn) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"id"}, {col});

  auto output_type = ROW({"id", "region"}, {INTEGER(), VARCHAR()});
  std::unordered_map<std::string, std::string> pv = {{"region", "us-east-1"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  auto region_col = result->childAt(1);
  EXPECT_TRUE(region_col->isConstantEncoding());
  EXPECT_EQ(extractValue<StringView>(region_col, 0).str(), "us-east-1");
}

TEST_F(VeloxPartitionColumnInjectorTest, InjectBooleanPartitionColumn) {
  auto col = makeFlatVector<int32_t>(pool(), {1});
  auto input = makeRowVector(pool(), {"id"}, {col});

  auto output_type = ROW({"id", "active"}, {INTEGER(), BOOLEAN()});
  std::unordered_map<std::string, std::string> pv = {{"active", "true"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  auto active_col = result->childAt(1);
  EXPECT_TRUE(active_col->isConstantEncoding());
  EXPECT_TRUE(extractValue<bool>(active_col, 0));
}

TEST_F(VeloxPartitionColumnInjectorTest, InjectRealPartitionColumn) {
  auto col = makeFlatVector<int32_t>(pool(), {1});
  auto input = makeRowVector(pool(), {"id"}, {col});

  auto output_type = ROW({"id", "score"}, {INTEGER(), REAL()});
  std::unordered_map<std::string, std::string> pv = {{"score", "1.5"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  auto score_col = result->childAt(1);
  EXPECT_TRUE(score_col->isConstantEncoding());
  EXPECT_FLOAT_EQ(extractValue<float>(score_col, 0), 1.5f);
}

// ---------------------------------------------------------------------------
// Missing column → null constant
// ---------------------------------------------------------------------------

TEST_F(VeloxPartitionColumnInjectorTest, MissingColumnFilledWithNull) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col});

  // Output wants [a, b, c]. "b" is in partition_values, "c" is missing.
  auto output_type = ROW({"a", "b", "c"}, {INTEGER(), VARCHAR(), BIGINT()});
  std::unordered_map<std::string, std::string> pv = {{"b", "hello"}};

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  ASSERT_EQ(result->type()->size(), 3);

  // "a" — data column.
  EXPECT_EQ(extractValue<int32_t>(result->childAt(0), 0), 1);

  // "b" — partition column.
  EXPECT_EQ(extractValue<StringView>(result->childAt(1), 0).str(), "hello");

  // "c" — not in input or partition_values → all nulls.
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(result->childAt(2)->isNullAt(i));
  }
}

// ---------------------------------------------------------------------------
// Multiple partition columns
// ---------------------------------------------------------------------------

TEST_F(VeloxPartitionColumnInjectorTest, MultiplePartitionColumns) {
  auto col_data = makeFlatVector<int32_t>(pool(), {10, 20});
  auto input = makeRowVector(pool(), {"data"}, {col_data});

  auto output_type = ROW(
      {"data", "year", "month", "region"},
      {INTEGER(), INTEGER(), SMALLINT(), VARCHAR()});

  std::unordered_map<std::string, std::string> pv = {
      {"year", "2024"},
      {"month", "6"},
      {"region", "eu-west"},
  };

  auto result = injectPartitionColumns(input, pv, output_type, pool());

  ASSERT_EQ(result->type()->size(), 4);
  ASSERT_EQ(result->size(), 2);

  // data column preserved.
  EXPECT_EQ(extractValue<int32_t>(result->childAt(0), 0), 10);

  // year = 2024 (INTEGER).
  EXPECT_EQ(extractValue<int32_t>(result->childAt(1), 0), 2024);

  // month = 6 (SMALLINT).
  EXPECT_EQ(extractValue<int16_t>(result->childAt(2), 0), 6);

  // region = "eu-west" (VARCHAR).
  EXPECT_EQ(extractValue<StringView>(result->childAt(3), 0).str(), "eu-west");
}

// ---------------------------------------------------------------------------
// Unsupported type
// ---------------------------------------------------------------------------

TEST_F(VeloxPartitionColumnInjectorTest, UnsupportedTypeThrows) {
  auto col = makeFlatVector<int32_t>(pool(), {1});
  auto input = makeRowVector(pool(), {"id"}, {col});

  // ARRAY type is not supported for partition columns.
  auto output_type = ROW({"id", "arr"}, {INTEGER(), ARRAY(INTEGER())});
  std::unordered_map<std::string, std::string> pv = {{"arr", "[1,2]"}};

  EXPECT_THROW(
      injectPartitionColumns(input, pv, output_type, pool()),
      std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox
