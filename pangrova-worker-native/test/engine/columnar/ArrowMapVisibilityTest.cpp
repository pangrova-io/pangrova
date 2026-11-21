#include "TestMemoryHelper.h"

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::columnar::test {
namespace {
using namespace facebook::velox;

struct ExportedArray {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ExportedArray() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

class ArrowMapVisibilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = pangrova::test::testLeafPool("map_visibility");
  }

  BufferPtr positions(const std::vector<vector_size_t>& values) {
    auto output = AlignedBuffer::allocate<vector_size_t>(values.size(), pool_.get());
    for (size_t index = 0; index < values.size(); ++index) {
      output->asMutable<vector_size_t>()[index] = values[index];
    }
    return output;
  }

  std::shared_ptr<MapVector> maps(const std::vector<std::optional<int64_t>>& keys,
      const std::vector<vector_size_t>& offsets, const std::vector<vector_size_t>& sizes) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    std::vector<int64_t> items(keys.size(), 31);
    return std::make_shared<MapVector>(pool_.get(), MAP(BIGINT(), BIGINT()), nullptr,
        static_cast<vector_size_t>(offsets.size()), positions(offsets), positions(sizes),
        maker.flatVectorNullable<int64_t>(keys), maker.flatVector<int64_t>(items));
  }

  std::shared_ptr<arrow::Array> exportArray(const VectorPtr& source, bool flatten = true) {
    ArrowOptions options;
    options.flattenDictionary = flatten;
    options.flattenConstant = flatten;
    ExportedArray exported;
    exportToArrow(source, exported.schema, options);
    exportToArrow(source, exported.array, pool_.get(), options);
    auto imported = arrow::ImportArray(&exported.array, &exported.schema);
    if (!imported.ok()) throw std::runtime_error(imported.status().ToString());
    auto result = imported.ValueOrDie();
    EXPECT_TRUE(result->ValidateFull().ok()) << result->ValidateFull().ToString();
    return result;
  }

  void checkMap(const std::shared_ptr<arrow::Array>& source, int64_t count) {
    const auto mapping = std::static_pointer_cast<arrow::MapArray>(source);
    ASSERT_EQ(mapping->keys()->length(), count);
    ASSERT_EQ(mapping->keys()->null_count(), 0);
    const auto keys = std::static_pointer_cast<arrow::Int64Array>(mapping->keys());
    for (int64_t index = 0; index < count; ++index) EXPECT_EQ(keys->Value(index), 17);
  }

  void requireUserFailure(const VectorPtr& source, bool flatten = true) {
    try {
      exportArray(source, flatten);
      FAIL() << "Visible NULL MAP key must fail before Arrow import";
    } catch (const VeloxUserError& failure) {
      EXPECT_EQ(failure.errorSource(), "USER");
      EXPECT_EQ(failure.errorCode(), "INVALID_ARGUMENT");
      EXPECT_NE(std::string(failure.what()).find("Visible MAP key contains NULL"), std::string::npos);
      EXPECT_FALSE(failure.isRetriable());
    }
  }

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

TEST_F(ArrowMapVisibilityTest, OffsetSubsetExportsOnlyVisibleKeysAndKeepsSourceBuffers) {
  auto mapping = maps({std::nullopt, 17, std::nullopt}, {1}, {1});
  const auto keys = mapping->mapKeys();
  const auto originalOffsets = mapping->offsets();
  checkMap(exportArray(mapping), 1);
  EXPECT_EQ(mapping->mapKeys(), keys);
  EXPECT_EQ(mapping->offsets(), originalOffsets);
  EXPECT_TRUE(keys->isNullAt(0));
  EXPECT_TRUE(keys->isNullAt(2));
  EXPECT_EQ(mapping->offsetAt(0), 1);
}

TEST_F(ArrowMapVisibilityTest, NullMapParentRemovesItsPayloadWithoutDroppingRows) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  mapping->setNull(0, true);
  auto wire = std::static_pointer_cast<arrow::MapArray>(exportArray(mapping));
  ASSERT_EQ(wire->length(), 2);
  EXPECT_TRUE(wire->IsNull(0));
  EXPECT_EQ(wire->value_length(0), 0);
  EXPECT_EQ(wire->value_length(1), 1);
  checkMap(wire, 1);
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
  EXPECT_EQ(mapping->sizeAt(0), 1);
}

TEST_F(ArrowMapVisibilityTest, NullStructParentMasksMapWithoutChangingStructCardinality) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto row = std::make_shared<RowVector>(pool_.get(), ROW({"mapping"}, {mapping->type()}),
      nullptr, 2, std::vector<VectorPtr>{mapping});
  row->setNull(0, true);
  auto wire = std::static_pointer_cast<arrow::StructArray>(exportArray(row));
  ASSERT_EQ(wire->length(), 2);
  EXPECT_TRUE(wire->IsNull(0));
  auto child = std::static_pointer_cast<arrow::MapArray>(wire->field(0));
  EXPECT_EQ(child->value_length(0), 0);
  checkMap(child, 1);
  EXPECT_FALSE(mapping->isNullAt(0));
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, NullStructParentMasksArrayOfMapsRecursively) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto list = std::make_shared<ArrayVector>(pool_.get(), ARRAY(mapping->type()), nullptr,
      2, positions({0, 1}), positions({1, 1}), mapping);
  auto row = std::make_shared<RowVector>(pool_.get(), ROW({"nested"}, {list->type()}),
      nullptr, 2, std::vector<VectorPtr>{list});
  row->setNull(0, true);
  auto wire = std::static_pointer_cast<arrow::StructArray>(exportArray(row));
  ASSERT_EQ(wire->length(), 2);
  auto child = std::static_pointer_cast<arrow::ListArray>(wire->field(0));
  EXPECT_EQ(child->value_length(0), 0);
  EXPECT_EQ(child->values()->length(), 1);
  checkMap(child->values(), 1);
  EXPECT_EQ(list->sizeAt(0), 1);
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, DictionaryMapExportsReferencedValuesAndPreservesNullIndices) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto wrapped = BaseVector::wrapInDictionary(nullptr, positions({1, 1, 0}), 3, mapping);
  wrapped->setNull(2, true);
  auto encoded = std::static_pointer_cast<arrow::DictionaryArray>(exportArray(wrapped, false));
  ASSERT_EQ(encoded->length(), 3);
  EXPECT_TRUE(encoded->IsNull(2));
  EXPECT_EQ(encoded->GetValueIndex(0), encoded->GetValueIndex(1));
  checkMap(encoded->dictionary(), 1);
  auto flattened = std::static_pointer_cast<arrow::MapArray>(exportArray(wrapped, true));
  ASSERT_EQ(flattened->length(), 3);
  EXPECT_TRUE(flattened->IsNull(2));
  checkMap(flattened, 2);
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, ReferencedDictionaryNullKeyFailsInBothEncodingModes) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto wrapped = BaseVector::wrapInDictionary(nullptr, positions({0}), 1, mapping);
  requireUserFailure(wrapped, false);
  requireUserFailure(wrapped, true);
}

TEST_F(ArrowMapVisibilityTest, NullStructParentMasksDictionaryMapWithoutChangingSourceIndices) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto indices = positions({0, 1});
  auto wrapped = BaseVector::wrapInDictionary(nullptr, indices, 2, mapping);
  auto row = std::make_shared<RowVector>(pool_.get(), ROW({"mapping"}, {wrapped->type()}),
      nullptr, 2, std::vector<VectorPtr>{wrapped});
  row->setNull(0, true);
  auto wire = std::static_pointer_cast<arrow::StructArray>(exportArray(row, false));
  ASSERT_EQ(wire->length(), 2);
  auto child = std::static_pointer_cast<arrow::DictionaryArray>(wire->field(0));
  EXPECT_TRUE(child->IsNull(0));
  EXPECT_FALSE(child->IsNull(1));
  checkMap(child->dictionary(), 1);
  EXPECT_EQ(wrapped->wrapInfo(), indices);
  EXPECT_EQ(indices->as<vector_size_t>()[0], 0);
  EXPECT_FALSE(wrapped->isNullAt(0));
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, ConstantMapPreservesValuesAndRejectsVisibleNullKeys) {
  auto mapping = maps({std::nullopt, 17}, {0, 1}, {1, 1});
  auto constant = BaseVector::wrapInConstant(3, 1, mapping);
  auto encoded = std::static_pointer_cast<arrow::RunEndEncodedArray>(exportArray(constant, false));
  ASSERT_EQ(encoded->length(), 3);
  ASSERT_EQ(encoded->run_ends()->length(), 1);
  checkMap(encoded->values(), 1);
  checkMap(exportArray(constant, true), 3);
  auto invalid = BaseVector::wrapInConstant(3, 0, mapping);
  requireUserFailure(invalid, false);
  requireUserFailure(invalid, true);
}

TEST_F(ArrowMapVisibilityTest, ConstantMapUnderNullStructUsesVisibilityRunsWithoutMutatingSource) {
  auto mapping = maps({17}, {0}, {1});
  auto constant = BaseVector::wrapInConstant(3, 0, mapping);
  auto row = std::make_shared<RowVector>(pool_.get(), ROW({"mapping"}, {constant->type()}),
      nullptr, 3, std::vector<VectorPtr>{constant});
  row->setNull(0, true);
  row->setNull(1, true);
  auto wire = std::static_pointer_cast<arrow::StructArray>(exportArray(row, false));
  auto encoded = std::static_pointer_cast<arrow::RunEndEncodedArray>(wire->field(0));
  EXPECT_EQ(encoded->length(), 3);
  ASSERT_EQ(encoded->run_ends()->length(), 2);
  auto values = std::static_pointer_cast<arrow::MapArray>(encoded->values());
  EXPECT_TRUE(values->IsNull(0));
  EXPECT_FALSE(values->IsNull(1));
  checkMap(values, 1);
  auto flat = std::static_pointer_cast<arrow::StructArray>(exportArray(row, true));
  checkMap(flat->field(0), 1);
  EXPECT_FALSE(constant->isNullAt(0));
  EXPECT_FALSE(mapping->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, VisibleNullKeyIsATypedUserFailureBeforeCDataPublication) {
  auto mapping = maps({std::nullopt}, {0}, {1});
  requireUserFailure(mapping);
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, AllNullDictionaryDoesNotExportHiddenInvalidKeys) {
  auto mapping = maps({std::nullopt}, {0}, {1});
  auto wrapped = BaseVector::wrapInDictionary(nullptr, positions({0, 0}), 2, mapping);
  wrapped->setNull(0, true);
  wrapped->setNull(1, true);
  auto encoded = std::static_pointer_cast<arrow::DictionaryArray>(exportArray(wrapped, false));
  ASSERT_EQ(encoded->length(), 2);
  EXPECT_EQ(encoded->null_count(), 2);
  EXPECT_EQ(encoded->dictionary()->length(), 0);
  auto flat = std::static_pointer_cast<arrow::MapArray>(exportArray(wrapped, true));
  EXPECT_EQ(flat->length(), 2);
  EXPECT_EQ(flat->null_count(), 2);
  EXPECT_EQ(flat->keys()->length(), 0);
  EXPECT_TRUE(mapping->mapKeys()->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, NullConstantHasNoBackingPayloadInBothEncodingModes) {
  auto constant = BaseVector::createNullConstant(MAP(BIGINT(), BIGINT()), 3, pool_.get());
  auto encoded = std::static_pointer_cast<arrow::RunEndEncodedArray>(exportArray(constant, false));
  ASSERT_EQ(encoded->length(), 3);
  ASSERT_EQ(encoded->run_ends()->length(), 1);
  EXPECT_TRUE(encoded->values()->IsNull(0));
  auto flat = std::static_pointer_cast<arrow::MapArray>(exportArray(constant, true));
  EXPECT_EQ(flat->length(), 3);
  EXPECT_EQ(flat->null_count(), 3);
  EXPECT_EQ(flat->keys()->length(), 0);
  EXPECT_TRUE(constant->isNullAt(0));
}

TEST_F(ArrowMapVisibilityTest, EmptyMapAndDictionaryDoNotExportUnreferencedKeys) {
  auto mapping = maps({std::nullopt}, {}, {});
  auto plain = std::static_pointer_cast<arrow::MapArray>(exportArray(mapping));
  EXPECT_EQ(plain->length(), 0);
  EXPECT_EQ(plain->keys()->length(), 0);
  auto wrapped = BaseVector::wrapInDictionary(nullptr, positions({}), 0, mapping);
  auto encoded = std::static_pointer_cast<arrow::DictionaryArray>(exportArray(wrapped, false));
  EXPECT_EQ(encoded->length(), 0);
  EXPECT_EQ(encoded->dictionary()->length(), 0);
}
}  // namespace
}  // namespace pangrova::columnar::test
