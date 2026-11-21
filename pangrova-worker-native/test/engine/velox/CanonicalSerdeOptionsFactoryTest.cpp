#include <gtest/gtest.h>
#include <stdexcept>
#include <unordered_map>

#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "engine/velox/serde/CanonicalSerdeOptionsFactory.h"
#include "velox/core/QueryConfig.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/OperatorUtils.h"

namespace pangrova::velox::test {
namespace {
using Compression = facebook::velox::common::CompressionKind;

substrait::NamedStruct declared() {
  substrait::NamedStruct result;
  result.add_names("value");
  result.mutable_struct_()->add_types()->mutable_i64()->set_nullability(
      substrait::Type::NULLABILITY_REQUIRED);
  return result;
}

TEST(CanonicalSerdeOptionsFactoryTest, ActualSdkCallerReceivesIndependentTaskOptions) {
  auto factory = std::make_shared<const CanonicalSerdeOptionsFactory>(
      CanonicalSerdeOptionsFactory::SchemaBindings{{"output", declared()}}, 1024);
  const facebook::velox::core::QueryConfig query(
      std::unordered_map<std::string, std::string>{{"require_vector_serde_options_factory", "true"}},
      nullptr, factory);
  auto first = facebook::velox::exec::getVectorSerdeOptions(Compression::CompressionKind_LZ4, "ArrowBatch", query, "output");
  auto second = facebook::velox::exec::getVectorSerdeOptions(Compression::CompressionKind_ZSTD, "ArrowBatch", query, "output");
  const auto* bound = dynamic_cast<const ArrowBatchVectorSerde::StreamOptions*>(first.get());
  const auto* secondBound = dynamic_cast<const ArrowBatchVectorSerde::StreamOptions*>(second.get());
  ASSERT_NE(bound, nullptr);
  ASSERT_NE(secondBound, nullptr);
  EXPECT_EQ(bound->maximum_bytes, 1024);
  EXPECT_EQ(bound->canonical_schema->SerializeAsString(), declared().SerializeAsString());
  EXPECT_NE(first.get(), second.get());
  EXPECT_EQ(bound->canonical_schema.get(), secondBound->canonical_schema.get());
  EXPECT_EQ(first->compressionKind, facebook::velox::common::CompressionKind::CompressionKind_LZ4);
  EXPECT_EQ(second->compressionKind, facebook::velox::common::CompressionKind::CompressionKind_ZSTD);
}

TEST(CanonicalSerdeOptionsFactoryTest, ActualSdkCallerRejectsMissingTaskOwner) {
  const facebook::velox::core::QueryConfig query(
      std::unordered_map<std::string, std::string>{{"require_vector_serde_options_factory", "true"}});
  EXPECT_THROW(facebook::velox::exec::getVectorSerdeOptions(Compression::CompressionKind_NONE, "ArrowBatch", query, "output"),
      facebook::velox::VeloxUserError);
}

TEST(CanonicalSerdeOptionsFactoryTest, ActualSdkCallerRejectsMissingOrUnsupportedNodeBinding) {
  auto factory = std::make_shared<const CanonicalSerdeOptionsFactory>(
      CanonicalSerdeOptionsFactory::SchemaBindings{{"output", declared()}}, 1024);
  const facebook::velox::core::QueryConfig query(
      std::unordered_map<std::string, std::string>{{"require_vector_serde_options_factory", "true"}},
      nullptr, factory);
  EXPECT_THROW(facebook::velox::exec::getVectorSerdeOptions(Compression::CompressionKind_NONE, "ArrowBatch", query, "missing"),
      std::invalid_argument);
  EXPECT_THROW(facebook::velox::exec::getVectorSerdeOptions(Compression::CompressionKind_NONE, "unknown", query, "output"),
      std::invalid_argument);
}

TEST(CanonicalSerdeOptionsFactoryTest, MissingChildNullabilityCannotBecomeAnExecutableBinding) {
  auto malformed = declared();
  malformed.mutable_struct_()->mutable_types(0)->mutable_i64()->set_nullability(
      substrait::Type::NULLABILITY_UNSPECIFIED);
  EXPECT_THROW((CanonicalSerdeOptionsFactory({{"output", malformed}}, 1024)), std::invalid_argument);
  EXPECT_THROW((CanonicalSerdeOptionsFactory({{"", declared()}}, 1024)), std::invalid_argument);
  EXPECT_THROW((CanonicalSerdeOptionsFactory({{"output", declared()}}, 0)), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox::test
