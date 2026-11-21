// VeloxSchemaEvolutionTest: Unit tests for column mapping and type promotion.
// Validates: Requirements R6.6, R13.4

#include <gtest/gtest.h>

#include "TestMemoryHelper.h"
#include "engine/velox/connector/SchemaEvolution.h"
#include "engine/velox/connector/PhysicalFileSchema.h"

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/DecodedVector.h"

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
  auto rowType = ROW(std::vector<std::string>(names),
                     std::move(types));
  auto size = children.empty() ? 0 : children[0]->size();
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, size, children);
}

class VeloxSchemaEvolutionTest : public ::testing::Test {
 protected:
  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;

  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool("schema_evolution_test");
  }

  facebook::velox::memory::MemoryPool* pool() { return pool_.get(); }
};

std::shared_ptr<facebook::velox::memory::MemoryPool> VeloxSchemaEvolutionTest::pool_;

// ---------------------------------------------------------------------------
// applyColumnMapping tests
// ---------------------------------------------------------------------------

TEST_F(VeloxSchemaEvolutionTest, EmptyMappingsReturnsInputUnchanged) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col});

  auto output_type = ROW({"a"}, {INTEGER()});
  std::vector<split::ColumnMappingConfig> mappings;

  auto result = applyColumnMapping(input, mappings, output_type, pool());
  // Should return the exact same pointer.
  EXPECT_EQ(result.get(), input.get());
}

TEST_F(VeloxSchemaEvolutionTest, ColumnReorderByMapping) {
  // Input has columns [b, a], output wants [a, b].
  auto col_b = makeFlatVector<int64_t>(pool(), {10, 20, 30});
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"b", "a"}, {col_b, col_a});

  auto output_type = ROW({"a", "b"}, {INTEGER(), BIGINT()});

  std::vector<split::ColumnMappingConfig> mappings = {
      {/*source_id=*/0, /*target_id=*/0, "a", "a"},
      {/*source_id=*/1, /*target_id=*/1, "b", "b"},
  };

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(result->type()->size(), 2);
  EXPECT_EQ(result->type()->asRow().nameOf(0), "a");
  EXPECT_EQ(result->type()->asRow().nameOf(1), "b");

  // Verify data: column "a" should have [1, 2, 3].
  auto result_a = result->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(result_a->valueAt(0), 1);
  EXPECT_EQ(result_a->valueAt(1), 2);
  EXPECT_EQ(result_a->valueAt(2), 3);
}

TEST_F(VeloxSchemaEvolutionTest, MissingColumnFilledWithNull) {
  // Input has [a], output wants [a, b]. Column "b" is missing → null.
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "b"}, {INTEGER(), BIGINT()});

  std::vector<split::ColumnMappingConfig> mappings = {
      {0, 0, "a", "a"},
  };

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(result->type()->size(), 2);

  // Column "a" should have data.
  auto result_a = result->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(result_a->valueAt(0), 1);

  // Column "b" should be all nulls.
  auto result_b = result->childAt(1);
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(result_b->isNullAt(i));
  }
}

TEST_F(VeloxSchemaEvolutionTest, ColumnRenameViaMapping) {
  // Input has column "old_name", output wants "new_name".
  auto col = makeFlatVector<int32_t>(pool(), {10, 20});
  auto input = makeRowVector(pool(), {"old_name"}, {col});

  auto output_type = ROW({"new_name"}, {INTEGER()});

  std::vector<split::ColumnMappingConfig> mappings = {
      {0, 0, "old_name", "new_name"},
  };

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ(result->type()->asRow().nameOf(0), "new_name");
  auto result_col = result->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(result_col->valueAt(0), 10);
  EXPECT_EQ(result_col->valueAt(1), 20);
}

TEST_F(VeloxSchemaEvolutionTest, DirectNameMatchWithoutMapping) {
  // Column "x" exists in both input and output with same name.
  // No explicit mapping entry for "x", but it should still be found.
  auto col_x = makeFlatVector<int32_t>(pool(), {5, 6});
  auto col_y = makeFlatVector<int64_t>(pool(), {50, 60});
  auto input = makeRowVector(pool(), {"x", "y"}, {col_x, col_y});

  auto output_type = ROW({"y", "x"}, {BIGINT(), INTEGER()});

  // Only map "y" explicitly; "x" should be found by direct name match.
  std::vector<split::ColumnMappingConfig> mappings = {
      {0, 0, "y", "y"},
  };

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ(result->type()->asRow().nameOf(0), "y");
  EXPECT_EQ(result->type()->asRow().nameOf(1), "x");
}

// ---------------------------------------------------------------------------
// Default value fill tests (schema evolution: missing column + default)
// ---------------------------------------------------------------------------

TEST_F(VeloxSchemaEvolutionTest, MissingColumnWithDefaultValueLong) {
  // Input has [a], output wants [a, b]. Column "b" is missing but has
  // a default value of "42" (BIGINT).
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "b"}, {INTEGER(), BIGINT()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_b{0, 1, "b_old", "b", "42"};
  std::vector<split::ColumnMappingConfig> mappings = {mapping_a, mapping_b};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 3);
  ASSERT_EQ(result->type()->size(), 2);

  // Column "a" should have original data.
  auto result_a = result->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(result_a->valueAt(0), 1);

  // Column "b" should be filled with default value 42 (ConstantVector).
  auto result_b = result->childAt(1);
  ASSERT_TRUE(result_b->isConstantEncoding());
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(result_b->isNullAt(i));
  }
  // Decode to flat to read values.
  auto flat_b = BaseVector::create(BIGINT(), 3, pool());
  flat_b->copy(result_b.get(), 0, 0, 3);
  auto typed_b = flat_b->asFlatVector<int64_t>();
  EXPECT_EQ(typed_b->valueAt(0), 42);
  EXPECT_EQ(typed_b->valueAt(1), 42);
  EXPECT_EQ(typed_b->valueAt(2), 42);
}

TEST_F(VeloxSchemaEvolutionTest, MissingColumnWithDefaultValueString) {
  // Missing VARCHAR column with default value "hello".
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "name"}, {INTEGER(), VARCHAR()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_name{0, 1, "name_old", "name", "hello"};
  std::vector<split::ColumnMappingConfig> mappings = {mapping_a, mapping_name};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  auto result_name = result->childAt(1);
  ASSERT_TRUE(result_name->isConstantEncoding());
  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_FALSE(result_name->isNullAt(i));
  }
  auto flat_name = BaseVector::create(VARCHAR(), 2, pool());
  flat_name->copy(result_name.get(), 0, 0, 2);
  auto typed_name = flat_name->asFlatVector<StringView>();
  EXPECT_EQ(typed_name->valueAt(0).getString(), "hello");
  EXPECT_EQ(typed_name->valueAt(1).getString(), "hello");
}

TEST_F(VeloxSchemaEvolutionTest, MissingColumnWithDefaultValueDouble) {
  // Missing DOUBLE column with default value "3.14".
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "score"}, {INTEGER(), DOUBLE()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_score{0, 1, "score_old", "score", "3.14"};
  std::vector<split::ColumnMappingConfig> mappings = {mapping_a, mapping_score};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  auto result_score = result->childAt(1);
  ASSERT_TRUE(result_score->isConstantEncoding());
  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_FALSE(result_score->isNullAt(i));
  }
  auto flat_score = BaseVector::create(DOUBLE(), 2, pool());
  flat_score->copy(result_score.get(), 0, 0, 2);
  auto typed_score = flat_score->asFlatVector<double>();
  EXPECT_DOUBLE_EQ(typed_score->valueAt(0), 3.14);
  EXPECT_DOUBLE_EQ(typed_score->valueAt(1), 3.14);
}

TEST_F(VeloxSchemaEvolutionTest, MissingColumnWithoutDefaultStillNull) {
  // Missing column with empty default_value should still be null.
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "b"}, {INTEGER(), BIGINT()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_b{0, 1, "b_old", "b", ""};
  std::vector<split::ColumnMappingConfig> mappings = {mapping_a, mapping_b};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 3);
  auto result_b = result->childAt(1);
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(result_b->isNullAt(i));
  }
}

TEST_F(VeloxSchemaEvolutionTest, MissingColumnWithDefaultValueBoolean) {
  // Missing BOOLEAN column with default value "true".
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW({"a", "active"}, {INTEGER(), BOOLEAN()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_active{
      0, 1, "active_old", "active", "true"};
  std::vector<split::ColumnMappingConfig> mappings = {
      mapping_a, mapping_active};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  auto result_active = result->childAt(1);
  ASSERT_TRUE(result_active->isConstantEncoding());
  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_FALSE(result_active->isNullAt(i));
  }
  auto flat_active = BaseVector::create(BOOLEAN(), 2, pool());
  flat_active->copy(result_active.get(), 0, 0, 2);
  auto typed_active = flat_active->asFlatVector<bool>();
  EXPECT_TRUE(typed_active->valueAt(0));
  EXPECT_TRUE(typed_active->valueAt(1));
}

TEST_F(VeloxSchemaEvolutionTest, MixedDefaultAndNullColumns) {
  // Output wants [a, b, c]. "a" exists, "b" has default "99", "c" has
  // no default. Verifies mixed behavior in a single mapping.
  auto col_a = makeFlatVector<int32_t>(pool(), {10, 20});
  auto input = makeRowVector(pool(), {"a"}, {col_a});

  auto output_type = ROW(
      {"a", "b", "c"}, {INTEGER(), BIGINT(), VARCHAR()});

  split::ColumnMappingConfig mapping_a{0, 0, "a", "a", ""};
  split::ColumnMappingConfig mapping_b{0, 1, "b_old", "b", "99"};
  split::ColumnMappingConfig mapping_c{0, 2, "c_old", "c", ""};
  std::vector<split::ColumnMappingConfig> mappings = {
      mapping_a, mapping_b, mapping_c};

  auto result = applyColumnMapping(input, mappings, output_type, pool());

  ASSERT_EQ(result->size(), 2);
  ASSERT_EQ(result->type()->size(), 3);

  // "a" has data.
  auto result_a = result->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(result_a->valueAt(0), 10);

  // "b" has default 99 (ConstantVector).
  auto result_b = result->childAt(1);
  ASSERT_TRUE(result_b->isConstantEncoding());
  auto flat_b = BaseVector::create(BIGINT(), 2, pool());
  flat_b->copy(result_b.get(), 0, 0, 2);
  auto typed_b = flat_b->asFlatVector<int64_t>();
  EXPECT_EQ(typed_b->valueAt(0), 99);
  EXPECT_EQ(typed_b->valueAt(1), 99);

  // "c" is null (no default).
  for (vector_size_t i = 0; i < 2; ++i) {
    EXPECT_TRUE(result->childAt(2)->isNullAt(i));
  }
}

// ---------------------------------------------------------------------------
// applyTypePromotion tests
// ---------------------------------------------------------------------------

TEST_F(VeloxSchemaEvolutionTest, EmptyPromotionsReturnsInputUnchanged) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col});

  std::vector<split::TypePromotionConfig> promotions;

  auto result = applyTypePromotion(input, promotions, pool());
  EXPECT_EQ(result.get(), input.get());
}

TEST_F(VeloxSchemaEvolutionTest, IntToLongPromotion) {
  auto col = makeFlatVector<int32_t>(pool(), {1, 2, 3});
  auto input = makeRowVector(pool(), {"a"}, {col});

  std::vector<split::TypePromotionConfig> promotions = {
      {/*field_id=*/0, "integer", "bigint"},
  };

  auto result = applyTypePromotion(input, promotions, pool());

  ASSERT_EQ(result->size(), 3);
  // The result column type should be BIGINT.
  EXPECT_TRUE(result->childAt(0)->type()->equivalent(*BIGINT()));
}

TEST_F(VeloxSchemaEvolutionTest, FloatToDoublePromotion) {
  auto col = makeFlatVector<float>(pool(), {1.5f, 2.5f});
  auto input = makeRowVector(pool(), {"val"}, {col});

  std::vector<split::TypePromotionConfig> promotions = {
      {/*field_id=*/0, "float", "double"},
  };

  auto result = applyTypePromotion(input, promotions, pool());

  ASSERT_EQ(result->size(), 2);
  EXPECT_TRUE(result->childAt(0)->type()->equivalent(*DOUBLE()));
}

TEST_F(VeloxSchemaEvolutionTest, NoActualTypeChangeReturnsInput) {
  // Promotion config says int→int (same type) — should be a no-op.
  auto col = makeFlatVector<int32_t>(pool(), {1, 2});
  auto input = makeRowVector(pool(), {"a"}, {col});

  std::vector<split::TypePromotionConfig> promotions = {
      {/*field_id=*/0, "integer", "integer"},
  };

  auto result = applyTypePromotion(input, promotions, pool());
  // Should return input unchanged since types are equivalent.
  EXPECT_EQ(result.get(), input.get());
}

TEST_F(VeloxSchemaEvolutionTest, MultipleColumnPromotions) {
  auto col_a = makeFlatVector<int32_t>(pool(), {1, 2});
  auto col_b = makeFlatVector<float>(pool(), {1.0f, 2.0f});
  auto col_c = makeFlatVector<int64_t>(pool(), {100, 200});
  auto input = makeRowVector(pool(), {"a", "b", "c"}, {col_a, col_b, col_c});

  std::vector<split::TypePromotionConfig> promotions = {
      {/*field_id=*/0, "integer", "bigint"},
      {/*field_id=*/1, "float", "double"},
      // col_c (field_id=2) not promoted.
  };

  auto result = applyTypePromotion(input, promotions, pool());

  ASSERT_EQ(result->size(), 2);
  EXPECT_TRUE(result->childAt(0)->type()->equivalent(*BIGINT()));
  EXPECT_TRUE(result->childAt(1)->type()->equivalent(*DOUBLE()));
  // col_c should remain BIGINT.
  EXPECT_TRUE(result->childAt(2)->type()->equivalent(*BIGINT()));
}

TEST_F(VeloxSchemaEvolutionTest, PhysicalFileIdentityPreventsReusedNameFromReadingAnOlderField) {
  auto target = ROW({"id", "renamed", "old"}, {BIGINT(), BIGINT(), BIGINT()});
  PhysicalFileSchema schema(target,
      {{0, 0, "id", "id"}, {1, 1, "old", "renamed"}, {-1, 2, "old", "old"}},
      {{0, "integer", "bigint", "id"}});
  EXPECT_EQ(schema.physicalType()->names(), (std::vector<std::string>{"id", "old"}));
  auto indices = allocateIndices(1, pool());
  indices->asMutable<vector_size_t>()[0] = 1;
  auto id = BaseVector::wrapInDictionary(nullptr, indices, 1, makeFlatVector<int32_t>(pool(), {4, 7}));
  auto old = BaseVector::wrapInDictionary(nullptr, indices, 1, makeFlatVector<int64_t>(pool(), {40, 70}));
  auto result = schema.apply(makeRowVector(pool(), {"id", "old"}, {id, old}), pool());
  EXPECT_EQ(result->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 7);
  DecodedVector renamed(*result->childAt(1));
  EXPECT_EQ(renamed.valueAt<int64_t>(0), 70);
  EXPECT_TRUE(result->childAt(2)->isNullAt(0));
}

TEST_F(VeloxSchemaEvolutionTest, ExactDecimalEvolutionCrossesShortAndLongStorageWithoutRounding) {
  auto input_type = DECIMAL(10, 2);
  auto values = BaseVector::create(input_type, 2, pool());
  values->asFlatVector<int64_t>()->set(0, 12345);
  values->setNull(1, true);
  auto row = makeRowVector(pool(), {"amount"}, {values});
  auto result = applyTypePromotion(row, {{0, "decimal(10,2)", "decimal(25,4)", "amount"}}, pool());
  EXPECT_TRUE(result->childAt(0)->type()->equivalent(*DECIMAL(25, 4)));
  EXPECT_EQ(result->childAt(0)->asFlatVector<int128_t>()->valueAt(0), 1234500);
  EXPECT_TRUE(result->childAt(0)->isNullAt(1));
  EXPECT_THROW(applyTypePromotion(row, {{0, "decimal(10,2)", "decimal(10,1)", "amount"}}, pool()),
      std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox
