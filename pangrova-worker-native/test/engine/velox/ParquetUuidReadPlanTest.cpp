#include <gtest/gtest.h>

#include "TestMemoryHelper.h"
#include "engine/velox/connector/ParquetUuidReadPlan.h"
#include "velox/core/QueryCtx.h"
#include "velox/core/Expressions.h"
#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

std::unique_ptr<dwio::common::TypeWithId> fileType(
    const TypePtr& logical, const std::string& name = "root", bool annotated = true, int width = 16) {
  std::vector<std::unique_ptr<dwio::common::TypeWithId>> children;
  std::vector<TypePtr> types;
  for (uint32_t index = 0; index < logical->size(); ++index) {
    children.push_back(fileType(logical->childAt(index),
        logical->isRow() ? logical->asRow().nameOf(index) : "element", annotated, width));
    types.push_back(children.back()->type());
  }
  TypePtr physical = logical;
  std::optional<parquet::thrift::Type::type> storage;
  std::optional<parquet::thrift::LogicalType> annotation;
  if (isUuidType(logical)) {
    physical = VARBINARY();
    storage = parquet::thrift::Type::FIXED_LEN_BYTE_ARRAY;
    if (annotated) {
      annotation.emplace();
      annotation->__set_UUID(parquet::thrift::UUIDType{});
    }
  } else if (logical->isRow()) physical = ROW(logical->asRow().names(), types);
  else if (logical->isArray()) physical = ARRAY(types.at(0));
  else if (logical->isMap()) physical = MAP(types.at(0), types.at(1));
  return std::make_unique<parquet::ParquetTypeWithId>(physical, std::move(children), 0, 0,
      logical->size() ? parquet::ParquetTypeWithId::kNonLeaf : 0,
      name, storage, annotation, std::nullopt, 0, 1, true, false, 0, 0, width);
}

class ParquetUuidReadPlanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("parquet-uuid-read-plan");
    query_ = core::QueryCtx::create();
    evaluator_ = std::make_unique<exec::SimpleExpressionEvaluator>(query_.get(), pool_.get());
  }

  VectorPtr ids() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    return maker.flatVectorNullable<StringView>(
        {StringView(wire_[0]), StringView(wire_[1]), StringView(wire_[2]), std::nullopt}, VARBINARY());
  }

  std::unique_ptr<ParquetUuidReadPlan> plan(const RowTypePtr& logical) {
    auto schema = fileType(logical);
    return ParquetUuidReadPlan::create(logical, *schema, evaluator_.get());
  }

  const std::vector<std::string> wire_{std::string(16, '\0'), std::string(16, '\xff'),
      std::string("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff", 16)};
  const int128_t mixed_ = (int128_t{0x0011223344556677LL} << 64) | int128_t{0x8899aabbccddeeffULL};
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<core::QueryCtx> query_;
  std::unique_ptr<exec::SimpleExpressionEvaluator> evaluator_;
};

TEST_F(ParquetUuidReadPlanTest, ConvertsAnnotatedBigEndianPayloadsAndNullsUsingTheNativeCast) {
  auto conversion = plan(ROW({"id"}, {UUID()}));
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto input = maker.rowVector({"id"}, {ids()});
  auto output = conversion->convert(input);
  input.reset();
  ASSERT_TRUE(isUuidType(output->childAt(0)->type()));
  DecodedVector values(*output->childAt(0));
  EXPECT_EQ(values.valueAt<int128_t>(0), 0);
  EXPECT_EQ(values.valueAt<int128_t>(1), -1);
  EXPECT_EQ(values.valueAt<int128_t>(2), mixed_);
  EXPECT_TRUE(values.isNullAt(3));
}

TEST_F(ParquetUuidReadPlanTest, NativeCastPreservesNestedDictionaryConstantAndParentNullSemantics) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto binary = ids();
  auto indices = allocateIndices(4, pool_.get());
  auto* raw = indices->asMutable<vector_size_t>();
  raw[0] = 3; raw[1] = 2; raw[2] = 0; raw[3] = 1;
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 4, binary);
  auto constant = BaseVector::wrapInConstant(4, 1, binary);
  auto nested = maker.rowVector({"id"}, {binary});
  nested->setNull(1, true);
  const std::vector<vector_size_t> offsets{0, 1, 2, 3};
  auto input = maker.rowVector({"dictionary", "constant", "nested", "items", "mapping"}, {
      dictionary, constant, nested, maker.arrayVector(offsets, binary),
      maker.mapVector(offsets, maker.flatVector<int32_t>({0, 1, 2, 3}), binary)});
  auto conversion = plan(ROW(input->type()->asRow().names(), {
      UUID(), UUID(), ROW({"id"}, {UUID()}), ARRAY(UUID()), MAP(INTEGER(), UUID())}));
  auto output = conversion->convert(input);
  DecodedVector values(*output->childAt(0));
  EXPECT_TRUE(values.isNullAt(0));
  EXPECT_EQ(values.valueAt<int128_t>(1), mixed_);
  EXPECT_EQ(values.valueAt<int128_t>(2), 0);
  EXPECT_EQ(values.valueAt<int128_t>(3), -1);
  DecodedVector constants(*output->childAt(1));
  for (int row = 0; row < 4; ++row) EXPECT_EQ(constants.valueAt<int128_t>(row), -1);
  EXPECT_TRUE(output->childAt(2)->isNullAt(1));
  const auto* items = output->childAt(3)->as<ArrayVector>();
  ASSERT_TRUE(isUuidType(items->elements()->type()));
  EXPECT_TRUE(items->elements()->isNullAt(3));
  const auto* mapping = output->childAt(4)->as<MapVector>();
  ASSERT_TRUE(isUuidType(mapping->mapValues()->type()));
  EXPECT_TRUE(mapping->mapValues()->isNullAt(3));
}

TEST_F(ParquetUuidReadPlanTest, UnannotatedBinaryCannotAcquireUuidIdentity) {
  auto logical = ROW({"id"}, {UUID()});
  auto schema = fileType(logical, "root", false);
  EXPECT_THROW(ParquetUuidReadPlan::create(logical, *schema, evaluator_.get()), std::invalid_argument);
}

class ParquetUuidWidthTest : public ParquetUuidReadPlanTest, public ::testing::WithParamInterface<int> {};
TEST_P(ParquetUuidWidthTest, RejectsNonCanonicalStorageWidthBeforeReadingPayloads) {
  auto logical = ROW({"id"}, {UUID()});
  auto schema = fileType(logical, "root", true, GetParam());
  EXPECT_THROW(ParquetUuidReadPlan::create(logical, *schema, evaluator_.get()), std::invalid_argument);
}
INSTANTIATE_TEST_SUITE_P(InvalidWidths, ParquetUuidWidthTest, ::testing::Values(15, 17));

TEST_F(ParquetUuidReadPlanTest, PlainBinaryRetainsItsIdentityWithoutAllocatingAConversionPlan) {
  auto logical = ROW({"id"}, {VARBINARY()});
  auto schema = fileType(logical);
  EXPECT_EQ(ParquetUuidReadPlan::create(logical, *schema, nullptr), nullptr);
}

TEST_F(ParquetUuidReadPlanTest, UuidConversionRequiresTheActualTaskEvaluator) {
  auto logical = ROW({"id"}, {UUID()});
  auto schema = fileType(logical);
  EXPECT_THROW(ParquetUuidReadPlan::create(logical, *schema, nullptr), std::invalid_argument);
}

TEST_F(ParquetUuidReadPlanTest, AReaderPlanCanBeReusedAcrossDistinctBatches) {
  auto conversion = plan(ROW({"id"}, {UUID()}));
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto first = conversion->convert(maker.rowVector({"id"}, {ids()}));
  auto second = conversion->convert(maker.rowVector({"id"}, {ids()}));
  DecodedVector old(*first->childAt(0)), fresh(*second->childAt(0));
  EXPECT_EQ(old.valueAt<int128_t>(2), mixed_);
  EXPECT_EQ(fresh.valueAt<int128_t>(2), mixed_);
}

class UuidPayloadWidthTest : public ParquetUuidReadPlanTest, public ::testing::WithParamInterface<int> {};
TEST_P(UuidPayloadWidthTest, ReusedNativeUuidCastRejectsNonCanonicalPayloadLengths) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  const std::string invalid(GetParam(), '\0');
  auto input = maker.rowVector({"id"}, {
      maker.flatVector<StringView>({StringView(invalid)}, VARBINARY())});
  auto cast = evaluator_->compile(std::make_shared<core::CastTypedExpr>(UUID(),
      std::make_shared<core::FieldAccessTypedExpr>(VARBINARY(), "id"), false));
  SelectivityVector rows(1);
  VectorPtr result;
  EXPECT_ANY_THROW(evaluator_->evaluate(cast.get(), rows, *input, result));
}
INSTANTIATE_TEST_SUITE_P(InvalidPayloads, UuidPayloadWidthTest, ::testing::Values(0, 15, 17));

TEST_F(ParquetUuidReadPlanTest, EmptyAndAllNullBatchesRetainTheirDeclaredUuidType) {
  auto conversion = plan(ROW({"id"}, {UUID()}));
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto empty = maker.rowVector({"id"}, {BaseVector::create(VARBINARY(), 0, pool_.get())});
  EXPECT_EQ(conversion->convert(empty)->size(), 0);
  auto nulls = maker.rowVector({"id"}, {BaseVector::createNullConstant(VARBINARY(), 4, pool_.get())});
  auto output = conversion->convert(nulls);
  ASSERT_TRUE(isUuidType(output->childAt(0)->type()));
  for (int row = 0; row < 4; ++row) EXPECT_TRUE(output->childAt(0)->isNullAt(row));
}

}  // namespace
}  // namespace pangrova::velox::test
