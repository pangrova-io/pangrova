#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

#include "TestMemoryHelper.h"
#include "engine/velox/serde/CanonicalVectorNames.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {
using namespace facebook::velox;

TEST(CanonicalVectorNamesTest, NestedRowsBindNamesWithoutMutatingOrCopyingPayload) {
  auto pool = pangrova::test::testLeafPool("canonical-names-row");
  auto input = BaseVector::create(ROW({"old"}, {ROW({"id"}, {BIGINT()})}), 2, pool.get());
  auto* outer = input->as<RowVector>();
  auto inner = outer->childAt(0);
  inner->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->set(0, 41);
  inner->setNull(1, true);
  auto target = ROW({"wire"}, {ROW({"field_0"}, {BIGINT()})});
  auto output = bindCanonicalVectorNames(input, target);
  EXPECT_EQ(*output->type(), *target);
  EXPECT_EQ(input->type()->asRow().nameOf(0), "old");
  EXPECT_EQ(inner->type()->asRow().nameOf(0), "id");
  auto rebound = output->as<RowVector>()->childAt(0);
  EXPECT_EQ(rebound->nulls(), inner->nulls());
  EXPECT_EQ(rebound->as<RowVector>()->childAt(0), inner->as<RowVector>()->childAt(0));
  EXPECT_EQ(rebound->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 41);
  EXPECT_TRUE(rebound->isNullAt(1));
  EXPECT_EQ(bindCanonicalVectorNames(output, target), output);
}

TEST(CanonicalVectorNamesTest, DictionaryAndConstantRowsRetainIndicesNullsAndValues) {
  auto pool = pangrova::test::testLeafPool("canonical-names-encodings");
  auto input = BaseVector::create(ROW({"old"}, {BIGINT()}), 3, pool.get());
  input->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->set(1, 17);
  auto indices = AlignedBuffer::allocate<vector_size_t>(2, pool.get());
  indices->asMutable<vector_size_t>()[0] = 1;
  indices->asMutable<vector_size_t>()[1] = 1;
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 2, input);
  auto target = ROW({"new"}, {BIGINT()});
  auto output = bindCanonicalVectorNames(dictionary, target);
  EXPECT_EQ(output->wrapInfo(), dictionary->wrapInfo());
  EXPECT_EQ(output->valueVector()->as<RowVector>()->childAt(0), input->as<RowVector>()->childAt(0));
  EXPECT_EQ(output->wrappedIndex(0), 1);
  auto constant = BaseVector::wrapInConstant(5, 1, input);
  auto rebound = bindCanonicalVectorNames(constant, target);
  EXPECT_EQ(rebound->size(), 5);
  EXPECT_EQ(*rebound->type(), *target);
  EXPECT_EQ(rebound->wrappedIndex(0), constant->wrappedIndex(0));
  auto null = BaseVector::createNullConstant(input->type(), 5, pool.get());
  auto boundNull = bindCanonicalVectorNames(null, target);
  EXPECT_EQ(*boundNull->type(), *target);
  EXPECT_TRUE(boundNull->isNullAt(4));
}

TEST(CanonicalVectorNamesTest, ArrayAndMapWrappersKeepOffsetsAndSharedChildren) {
  auto pool = pangrova::test::testLeafPool("canonical-names-collections");
  const auto old = ROW({"old"}, {BIGINT()});
  const auto target = ROW({"new"}, {BIGINT()});
  for (bool map : {false, true}) {
    TypePtr sourceType = map ? TypePtr(MAP(BIGINT(), old)) : TypePtr(ARRAY(old));
    TypePtr targetType = map ? TypePtr(MAP(BIGINT(), target)) : TypePtr(ARRAY(target));
    auto nested = BaseVector::create(old, 3, pool.get());
    auto* values = nested->as<RowVector>()->childAt(0)->asFlatVector<int64_t>();
    values->set(1, 22);
    values->set(2, 33);
    nested->setNull(1, true);
    auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
    auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool.get());
    offsets->asMutable<vector_size_t>()[0] = 1;
    sizes->asMutable<vector_size_t>()[0] = 2;
    VectorPtr input;
    if (map) {
      auto keys = BaseVector::create(BIGINT(), 3, pool.get());
      keys->asFlatVector<int64_t>()->set(1, 10);
      keys->asFlatVector<int64_t>()->set(2, 20);
      input = std::make_shared<MapVector>(pool.get(), sourceType, nullptr, 1, offsets, sizes, keys, nested);
    } else {
      input = std::make_shared<ArrayVector>(pool.get(), sourceType, nullptr, 1, offsets, sizes, nested);
    }
    auto output = bindCanonicalVectorNames(input, targetType);
    if (map) {
      EXPECT_EQ(output->as<MapVector>()->offsets(), input->as<MapVector>()->offsets());
      EXPECT_EQ(output->as<MapVector>()->sizes(), input->as<MapVector>()->sizes());
      EXPECT_EQ(output->as<MapVector>()->mapKeys(), input->as<MapVector>()->mapKeys());
    } else {
      EXPECT_EQ(output->as<ArrayVector>()->offsets(), input->as<ArrayVector>()->offsets());
      EXPECT_EQ(output->as<ArrayVector>()->sizes(), input->as<ArrayVector>()->sizes());
    }
    auto rebound = map ? output->as<MapVector>()->mapValues() : output->as<ArrayVector>()->elements();
    EXPECT_EQ(rebound->as<RowVector>()->childAt(0), nested->as<RowVector>()->childAt(0));
    EXPECT_TRUE(rebound->isNullAt(1));
    EXPECT_EQ(rebound->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->valueAt(2), 33);
    EXPECT_EQ(*input->type(), *sourceType);
  }
}

class NamesLoader final : public VectorLoader {
 public:
  explicit NamesLoader(VectorPtr value) : value_(std::move(value)) {}
 protected:
  void loadInternal(RowSet, ValueHook* hook, vector_size_t size, VectorPtr* result) override {
    if (hook || size > value_->size()) throw std::runtime_error("Invalid lazy names fixture request");
    *result = value_;
  }
 private:
  VectorPtr value_;
};

TEST(CanonicalVectorNamesTest, LazyRowsLoadOnceAndKeepPayloadAliveAfterSourceOwnersExit) {
  auto pool = pangrova::test::testLeafPool("canonical-names-lazy");
  auto source = BaseVector::create(ROW({"old"}, {BIGINT()}), 1, pool.get());
  source->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->set(0, 71);
  auto lazy = std::make_shared<LazyVector>(pool.get(), source->type(), 1,
      std::make_unique<NamesLoader>(source));
  auto output = bindCanonicalVectorNames(lazy, ROW({"new"}, {BIGINT()}));
  EXPECT_EQ(source->type()->asRow().nameOf(0), "old");
  EXPECT_EQ(output->as<RowVector>()->childAt(0), source->as<RowVector>()->childAt(0));
  lazy.reset();
  source.reset();
  EXPECT_EQ(output->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 71);
  auto base = BaseVector::create(ROW({"old"}, {BIGINT()}), 3, pool.get());
  base->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->set(2, 93);
  auto indices = AlignedBuffer::allocate<vector_size_t>(2, pool.get());
  indices->asMutable<vector_size_t>()[0] = 2;
  indices->asMutable<vector_size_t>()[1] = 0;
  auto dictionary = BaseVector::wrapInDictionary(nullptr, indices, 2, base);
  auto deferred = std::make_shared<LazyVector>(pool.get(), base->type(), 2,
      std::make_unique<NamesLoader>(dictionary));
  auto constant = std::make_shared<ConstantVector<ComplexType>>(pool.get(), 4, 0, deferred);
  auto boundConstant = bindCanonicalVectorNames(constant, ROW({"new"}, {BIGINT()}));
  EXPECT_EQ(boundConstant->size(), 4);
  auto* boundBase = boundConstant->wrappedVector()->as<RowVector>();
  EXPECT_EQ(boundBase->childAt(0)->asFlatVector<int64_t>()->valueAt(boundConstant->wrappedIndex(0)), 93);
}

TEST(CanonicalVectorNamesTest, PhysicalTypeAndArityChangesRemainRejected) {
  auto pool = pangrova::test::testLeafPool("canonical-names-rejection");
  auto input = BaseVector::create(ROW({"old"}, {BIGINT()}), 1, pool.get());
  EXPECT_THROW(bindCanonicalVectorNames(input, ROW({"new"}, {INTEGER()})), std::invalid_argument);
  EXPECT_THROW(bindCanonicalVectorNames(input, ROW({}, {})), std::invalid_argument);
  EXPECT_THROW(bindCanonicalVectorNames(nullptr, input->type()), std::invalid_argument);
}

TEST(CanonicalVectorNamesTest, MatchingNamesStillMaterializeLazyArrayChildrenForSchemaExport) {
  auto pool = pangrova::test::testLeafPool("canonical-names-lazy-array");
  facebook::velox::test::VectorMaker maker(pool.get());
  auto elements = maker.arrayVector<float>({{1.0F, 0.0F}, {}, {0.5F}});
  auto deferred = std::make_shared<LazyVector>(pool.get(), elements->type(), elements->size(),
      std::make_unique<NamesLoader>(elements));
  auto source = maker.rowVector({"embedding"}, {deferred});
  auto output = bindCanonicalVectorNames(source, source->type());
  // Schema export uses the physical complex vector; retaining a LazyVector here
  // makes the SDK interpret its storage as ArrayVector and dereference garbage.
  ASSERT_EQ(output->as<RowVector>()->childAt(0), elements);
  EXPECT_EQ(source->childAt(0), deferred);
  EXPECT_EQ(output->as<RowVector>()->childAt(0)->as<ArrayVector>()->elements(), elements->elements());
  EXPECT_EQ(bindCanonicalVectorNames(output, output->type()), output);
}
}  // namespace
}  // namespace pangrova::velox::test
