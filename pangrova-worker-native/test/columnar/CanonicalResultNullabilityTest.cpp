#include <gtest/gtest.h>

#include "../TestMemoryHelper.h"
#include "velox/serde/CanonicalResultNullability.h"
#include "velox/vector/FlatVector.h"

#include <stdexcept>

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

substrait::Type integer(bool nullable = false) {
  substrait::Type type;
  type.mutable_i64()->set_nullability(nullable ? substrait::Type::NULLABILITY_NULLABLE
                                             : substrait::Type::NULLABILITY_REQUIRED);
  return type;
}

substrait::NamedStruct schema(substrait::Type child) {
  substrait::NamedStruct result;
  result.add_names("value");
  result.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *result.mutable_struct_()->add_types() = std::move(child);
  return result;
}

VectorPtr integers(facebook::velox::memory::MemoryPool* pool) {
  auto values = BaseVector::create(BIGINT(), 3, pool);
  auto flat = values->asFlatVector<int64_t>();
  flat->setNull(0, true);
  flat->set(1, 11);
  flat->setNull(2, true);
  return values;
}

RowVectorPtr row(facebook::velox::memory::MemoryPool* pool, VectorPtr child) {
  const auto size = child->size();
  const auto type = ROW({"value"}, {child->type()});
  return std::make_shared<RowVector>(pool, type, nullptr, size,
      std::vector<VectorPtr>{std::move(child)});
}

TEST(CanonicalResultNullabilityTest, ActualNullCannotBeEncodedAsRequiredResult) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-required");
  auto values = row(pool.get(), integers(pool.get()));
  EXPECT_THROW(validateCanonicalResultNullability(schema(integer()), values), std::runtime_error);
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(integer(true)), values));
}

TEST(CanonicalResultNullabilityTest, NullableRowParentMasksRequiredChildOnlyForActuallyNullParents) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-parent");
  auto values = row(pool.get(), integers(pool.get()));
  auto contract = schema(integer());
  contract.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  values->setNull(0, true);
  values->setNull(2, true);
  EXPECT_NO_THROW(validateCanonicalResultNullability(contract, values));
  values->setNull(0, false);
  EXPECT_THROW(validateCanonicalResultNullability(contract, values), std::runtime_error);
}

TEST(CanonicalResultNullabilityTest, DictionaryAndConstantUseActualChildIndicesNotUnreferencedNulls) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-dictionary");
  const auto source = integers(pool.get());
  auto indices = AlignedBuffer::allocate<vector_size_t>(2, pool.get());
  auto raw = indices->asMutable<vector_size_t>();
  raw[0] = 1;
  raw[1] = 1;
  auto selected = row(pool.get(), BaseVector::wrapInDictionary(nullptr, indices, 2, source));
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(integer()), selected));
  raw[0] = 0;
  EXPECT_THROW(validateCanonicalResultNullability(schema(integer()), selected), std::runtime_error);
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(integer()),
      row(pool.get(), BaseVector::wrapInConstant(2, 1, source))));
  EXPECT_THROW(validateCanonicalResultNullability(schema(integer()),
      row(pool.get(), BaseVector::wrapInConstant(2, 0, source))), std::runtime_error);
}

TEST(CanonicalResultNullabilityTest, ArrayChecksOnlyActualOffsetsAndNullableParentMasksElements) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-array");
  auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
  offsets->asMutable<vector_size_t>()[0] = 1;
  sizes->asMutable<vector_size_t>()[0] = 1;
  auto array = std::make_shared<ArrayVector>(pool.get(), ARRAY(BIGINT()), nullptr, 1,
      offsets, sizes, integers(pool.get()));
  substrait::Type type;
  type.mutable_list()->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
  *type.mutable_list()->mutable_type() = integer();
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(type), row(pool.get(), array)));
  offsets->asMutable<vector_size_t>()[0] = 0;
  EXPECT_THROW(validateCanonicalResultNullability(schema(type), row(pool.get(), array)), std::runtime_error);
  array->setNull(0, true);
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(type), row(pool.get(), array)));
}

TEST(CanonicalResultNullabilityTest, MapKeysRemainRequiredWhileNullableValuesMayBeNull) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-map");
  auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
  offsets->asMutable<vector_size_t>()[0] = 1;
  sizes->asMutable<vector_size_t>()[0] = 1;
  auto map = std::make_shared<MapVector>(pool.get(), MAP(BIGINT(), BIGINT()), nullptr, 1,
      offsets, sizes, integers(pool.get()), BaseVector::wrapInConstant(3, 0, integers(pool.get())));
  substrait::Type type;
  type.mutable_map()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *type.mutable_map()->mutable_key() = integer(true);
  *type.mutable_map()->mutable_value() = integer(true);
  EXPECT_NO_THROW(validateCanonicalResultNullability(schema(type), row(pool.get(), map)));
  offsets->asMutable<vector_size_t>()[0] = 0;
  EXPECT_THROW(validateCanonicalResultNullability(schema(type), row(pool.get(), map)), std::runtime_error);
}

TEST(CanonicalResultNullabilityTest, EmptyBatchCannotHideUnspecifiedNullability) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-empty");
  auto type = integer();
  type.mutable_i64()->set_nullability(substrait::Type::NULLABILITY_UNSPECIFIED);
  EXPECT_THROW(validateCanonicalResultNullability(schema(type),
      row(pool.get(), BaseVector::create(BIGINT(), 0, pool.get()))), std::invalid_argument);
}

TEST(CanonicalResultNullabilityTest, NestedNamesCannotBeErasedByPhysicalEquivalence) {
  const auto pool = pangrova::test::testLeafPool("result-nullability-nested-names");
  auto nested = row(pool.get(), BaseVector::create(BIGINT(), 0, pool.get()));
  substrait::Type type;
  type.mutable_struct_()->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  *type.mutable_struct_()->add_types() = integer();
  auto contract = schema(type);
  contract.add_names("value");
  EXPECT_NO_THROW(validateCanonicalResultNullability(contract, row(pool.get(), nested)));
  contract.set_names(1, "different");
  EXPECT_THROW(validateCanonicalResultNullability(contract, row(pool.get(), nested)), std::invalid_argument);
}
}  // namespace
}  // namespace pangrova::velox::test
