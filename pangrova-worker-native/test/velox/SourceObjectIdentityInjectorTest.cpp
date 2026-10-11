// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>

#include "../TestMemoryHelper.h"
#include "velox/connector/PangrovaTableHandle.h"
#include "velox/connector/SourceObjectIdentityInjector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::velox::test {
namespace {

class SourceObjectIdentityInjectorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    pool_ = pangrova::test::testLeafPool(
        "SourceObjectIdentityInjectorTest");
  }

  static std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
};

std::shared_ptr<facebook::velox::memory::MemoryPool>
    SourceObjectIdentityInjectorTest::pool_;

TEST_F(SourceObjectIdentityInjectorTest, InjectsCanonicalSplitIdentity) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto input = maker.rowVector(
      {"payload", std::string(kPhysicalRowPositionColumnName)},
      {maker.flatVector<int64_t>({11, 12}),
       maker.flatVector<int64_t>({0, 1})});
  auto requested = facebook::velox::ROW(
      {"payload", std::string(kSourceObjectIdColumnName),
       std::string(kPhysicalRowPositionColumnName)},
      {facebook::velox::BIGINT(), facebook::velox::VARCHAR(),
       facebook::velox::BIGINT()});

  auto output = injectSourceObjectIdentity(
      input, "snapshot-object-17", requested, pool_.get());

  ASSERT_EQ(output->type()->toString(), requested->toString());
  facebook::velox::DecodedVector identities(*output->childAt(1));
  ASSERT_EQ(identities.size(), 2);
  for (facebook::velox::vector_size_t row = 0; row < 2; ++row) {
    auto value = identities.valueAt<facebook::velox::StringView>(row);
    EXPECT_EQ(
        std::string(value.data(), value.size()), "snapshot-object-17");
  }
}

TEST_F(SourceObjectIdentityInjectorTest, RejectsMissingOrDuplicateIdentity) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto input = maker.rowVector(
      {"payload"}, {maker.flatVector<int64_t>({11})});
  auto missing = facebook::velox::ROW(
      {"payload"}, {facebook::velox::BIGINT()});
  auto duplicate = facebook::velox::ROW(
      {std::string(kSourceObjectIdColumnName),
       std::string(kSourceObjectIdColumnName), "payload"},
      {facebook::velox::VARCHAR(), facebook::velox::VARCHAR(),
       facebook::velox::BIGINT()});

  EXPECT_THROW(
      injectSourceObjectIdentity(input, "object", missing, pool_.get()),
      std::invalid_argument);
  EXPECT_THROW(
      injectSourceObjectIdentity(input, "object", duplicate, pool_.get()),
      std::invalid_argument);
  EXPECT_THROW(
      injectSourceObjectIdentity(input, "", duplicate, pool_.get()),
      std::invalid_argument);
}

TEST_F(SourceObjectIdentityInjectorTest, CompleteIdentityPreservesDictionaryPositionsAndPhysicalEvolutionNames) {
  using namespace facebook::velox;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto indices = allocateIndices(2, pool_.get());
  indices->asMutable<vector_size_t>()[0] = 1;
  indices->asMutable<vector_size_t>()[1] = 0;
  auto input = maker.rowVector(
      {"old_payload", std::string(kPhysicalRowPositionColumnName)},
      {BaseVector::wrapInDictionary(nullptr, indices, 2, maker.flatVector<int32_t>({11, 12})),
       BaseVector::wrapInDictionary(nullptr, indices, 2, maker.flatVector<int64_t>({7, 19}))});
  auto requested = ROW({"new_payload", std::string(kSourceIdColumnName),
      std::string(kSourceSnapshotColumnName), std::string(kSourceObjectIdColumnName),
      std::string(kPhysicalRowPositionColumnName)},
      {BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), BIGINT()});
  auto output = injectPhysicalSourceIdentity(input,
      {"catalog.schema.table", "snapshot-7", "immutable-object"}, requested, pool_.get());
  ASSERT_EQ(output->childrenSize(), 5);
  EXPECT_EQ(output->type()->asRow().nameOf(0), "old_payload");
  EXPECT_EQ(output->type()->asRow().childAt(0)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(output->childAt(0), input->childAt(0));
  EXPECT_EQ(output->childAt(4), input->childAt(1));
  DecodedVector positions(*output->childAt(4));
  EXPECT_EQ(positions.valueAt<int64_t>(0), 19);
  EXPECT_EQ(positions.valueAt<int64_t>(1), 7);
  const std::array<std::string, 3> values{"catalog.schema.table", "snapshot-7", "immutable-object"};
  for (size_t channel = 0; channel < values.size(); ++channel) {
    DecodedVector identity(*output->childAt(channel + 1));
    EXPECT_EQ(identity.valueAt<StringView>(0).str(), values[channel]);
    EXPECT_EQ(identity.valueAt<StringView>(1).str(), values[channel]);
    EXPECT_FALSE(identity.isNullAt(0));
  }
}

TEST_F(SourceObjectIdentityInjectorTest, RejectsFileSuppliedIdentityAndIncompleteSignedDomain) {
  using namespace facebook::velox;
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto requested = ROW({std::string(kSourceIdColumnName), std::string(kSourceSnapshotColumnName),
      std::string(kSourceObjectIdColumnName), std::string(kPhysicalRowPositionColumnName)},
      {VARCHAR(), VARCHAR(), VARCHAR(), BIGINT()});
  auto positions = maker.rowVector({std::string(kPhysicalRowPositionColumnName)},
      {maker.flatVector<int64_t>({7})});
  EXPECT_THROW(injectPhysicalSourceIdentity(positions, {"", "s", "o"}, requested, pool_.get()),
      std::invalid_argument);
  auto spoof = maker.rowVector({std::string(kSourceSnapshotColumnName),
      std::string(kPhysicalRowPositionColumnName)},
      {maker.flatVector<std::string>({"forged"}), maker.flatVector<int64_t>({7})});
  EXPECT_THROW(injectPhysicalSourceIdentity(spoof, {"table", "s", "o"}, requested, pool_.get()),
      std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox::test
