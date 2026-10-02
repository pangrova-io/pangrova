// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

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

}  // namespace
}  // namespace pangrova::velox::test
