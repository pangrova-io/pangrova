// Tests for MergeReaderFactory.
// Validates: Requirements R8.1, R10.4, R22.2

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/split/SplitConfig.h"
#include "engine/executor/NativeUnsupportedFeatureException.h"
#include "engine/merge/AggregationMergeReader.h"
#include "engine/merge/DeduplicateMergeReader.h"
#include "engine/merge/FirstRowMergeReader.h"
#include "engine/merge/MergeReaderFactory.h"
#include "engine/merge/PartialUpdateMergeReader.h"
#include "velox/type/Type.h"

namespace pangrova::merge::test {
namespace {

using namespace facebook::velox;

// =============================================================================
// Test fixture with shared schema and config helpers.
// =============================================================================

class MergeReaderFactoryTest : public ::testing::Test {
 protected:
  // Schema: (key: BIGINT, seq: BIGINT, value: BIGINT)
  RowTypePtr makeSchema() {
    return ROW({"key", "seq", "value"}, {BIGINT(), BIGINT(), BIGINT()});
  }

  // Build a PaimonConfig with the given merge engine and needs_merge=true.
  split::PaimonConfig makeConfig(const std::string& engine) {
    split::PaimonConfig config;
    config.merge_engine = engine;
    config.needs_merge = true;
    config.deletion_vectors_enabled = false;
    config.primary_keys = {"key"};
    config.sequence_column = "seq";
    return config;
  }
};

// =============================================================================
// Returns nullptr when needs_merge is false (COW / read-optimized).
// =============================================================================

TEST_F(MergeReaderFactoryTest, ReturnsNullptrWhenNeedsMergeFalse) {
  split::PaimonConfig config;
  config.needs_merge = false;
  config.deletion_vectors_enabled = false;
  config.merge_engine = "deduplicate";
  config.primary_keys = {"key"};
  config.sequence_column = "seq";

  auto reader = MergeReaderFactory::create(config, makeSchema());
  EXPECT_EQ(reader, nullptr);
}

// =============================================================================
// Returns nullptr when deletion_vectors_enabled is true (MOW mode).
// =============================================================================

TEST_F(MergeReaderFactoryTest, ReturnsNullptrWhenDeletionVectorsEnabled) {
  split::PaimonConfig config;
  config.needs_merge = true;
  config.deletion_vectors_enabled = true;
  config.merge_engine = "deduplicate";
  config.primary_keys = {"key"};
  config.sequence_column = "seq";

  auto reader = MergeReaderFactory::create(config, makeSchema());
  EXPECT_EQ(reader, nullptr);
}

// =============================================================================
// Creates DeduplicateMergeReader for "deduplicate" engine.
// =============================================================================

TEST_F(MergeReaderFactoryTest, CreatesDeduplicateReader) {
  auto config = makeConfig("deduplicate");
  auto reader = MergeReaderFactory::create(config, makeSchema());

  ASSERT_NE(reader, nullptr);
  auto* typed = dynamic_cast<DeduplicateMergeReader*>(reader.get());
  EXPECT_NE(typed, nullptr);
}

// =============================================================================
// Creates PartialUpdateMergeReader for "partial_update" engine.
// =============================================================================

TEST_F(MergeReaderFactoryTest, CreatesPartialUpdateReader) {
  auto config = makeConfig("partial_update");
  auto reader = MergeReaderFactory::create(config, makeSchema());

  ASSERT_NE(reader, nullptr);
  auto* typed = dynamic_cast<PartialUpdateMergeReader*>(reader.get());
  EXPECT_NE(typed, nullptr);
}

// =============================================================================
// Creates FirstRowMergeReader for "first_row" engine.
// =============================================================================

TEST_F(MergeReaderFactoryTest, CreatesFirstRowReader) {
  auto config = makeConfig("first_row");
  auto reader = MergeReaderFactory::create(config, makeSchema());

  ASSERT_NE(reader, nullptr);
  auto* typed = dynamic_cast<FirstRowMergeReader*>(reader.get());
  EXPECT_NE(typed, nullptr);
}

// =============================================================================
// Creates AggregationMergeReader for "aggregation" engine.
// =============================================================================

TEST_F(MergeReaderFactoryTest, CreatesAggregationReader) {
  split::PaimonConfig config;
  config.merge_engine = "aggregation";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"key"};
  config.sequence_column = "seq";
  config.aggregation_specs = {{"value", "SUM"}};

  auto reader = MergeReaderFactory::create(config, makeSchema());

  ASSERT_NE(reader, nullptr);
  auto* typed = dynamic_cast<AggregationMergeReader*>(reader.get());
  EXPECT_NE(typed, nullptr);
}

// =============================================================================
// Throws NativeUnsupportedFeatureException for unknown merge engine.
// =============================================================================

TEST_F(MergeReaderFactoryTest, ThrowsForUnknownMergeEngine) {
  auto config = makeConfig("unknown_engine");

  EXPECT_THROW(
      MergeReaderFactory::create(config, makeSchema()),
      executor::NativeUnsupportedFeatureException);
}

// =============================================================================
// Throws for unknown merge engine with correct metadata.
// =============================================================================

TEST_F(MergeReaderFactoryTest, UnknownEngineExceptionCarriesMetadata) {
  auto config = makeConfig("bogus");

  try {
    MergeReaderFactory::create(config, makeSchema());
    FAIL() << "Expected NativeUnsupportedFeatureException";
  } catch (const executor::NativeUnsupportedFeatureException& e) {
    EXPECT_EQ(e.connectorType(), "paimon");
    EXPECT_NE(
        std::string(e.featureName()).find("bogus"), std::string::npos);
  }
}

// =============================================================================
// Throws when primary key column not found in output type.
// =============================================================================

TEST_F(MergeReaderFactoryTest, ThrowsForMissingPrimaryKeyColumn) {
  split::PaimonConfig config;
  config.merge_engine = "deduplicate";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"nonexistent_key"};
  config.sequence_column = "seq";

  EXPECT_THROW(
      MergeReaderFactory::create(config, makeSchema()),
      std::invalid_argument);
}

// =============================================================================
// Throws when sequence column not found in output type.
// =============================================================================

TEST_F(MergeReaderFactoryTest, ThrowsForMissingSequenceColumn) {
  split::PaimonConfig config;
  config.merge_engine = "deduplicate";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"key"};
  config.sequence_column = "nonexistent_seq";

  EXPECT_THROW(
      MergeReaderFactory::create(config, makeSchema()),
      std::invalid_argument);
}

// =============================================================================
// Composite primary key resolution works correctly.
// =============================================================================

TEST_F(MergeReaderFactoryTest, CompositeKeyResolution) {
  auto type = ROW(
      {"pk1", "pk2", "seq", "value"},
      {BIGINT(), BIGINT(), BIGINT(), BIGINT()});

  split::PaimonConfig config;
  config.merge_engine = "deduplicate";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"pk1", "pk2"};
  config.sequence_column = "seq";

  auto reader = MergeReaderFactory::create(config, type);
  ASSERT_NE(reader, nullptr);
  auto* typed = dynamic_cast<DeduplicateMergeReader*>(reader.get());
  EXPECT_NE(typed, nullptr);
}

// =============================================================================
// Aggregation spec with invalid column name throws.
// =============================================================================

TEST_F(MergeReaderFactoryTest, AggSpecInvalidColumnThrows) {
  split::PaimonConfig config;
  config.merge_engine = "aggregation";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"key"};
  config.sequence_column = "seq";
  config.aggregation_specs = {{"nonexistent_col", "SUM"}};

  EXPECT_THROW(
      MergeReaderFactory::create(config, makeSchema()),
      std::invalid_argument);
}

// =============================================================================
// Aggregation spec with invalid function name throws.
// =============================================================================

TEST_F(MergeReaderFactoryTest, AggSpecInvalidFunctionThrows) {
  split::PaimonConfig config;
  config.merge_engine = "aggregation";
  config.needs_merge = true;
  config.deletion_vectors_enabled = false;
  config.primary_keys = {"key"};
  config.sequence_column = "seq";
  config.aggregation_specs = {{"value", "INVALID_FUNC"}};

  EXPECT_THROW(
      MergeReaderFactory::create(config, makeSchema()),
      std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::merge::test
