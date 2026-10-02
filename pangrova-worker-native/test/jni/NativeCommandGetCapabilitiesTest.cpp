// Canonical v2 GET_CAPABILITIES dispatcher contract tests.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include <openssl/evp.h>

#include "artifact/ImmutableArtifactCache.h"
#include "capability/NativeCapabilityCatalog.h"
#include "google/protobuf/descriptor.pb.h"
#include "jni/command/NativeCommandDispatcher.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "vector/diskann/DiskAnnIndexCache.h"

namespace pangrova::jni::test {
namespace {

namespace proto = pangrova::protocol::v2;

std::vector<uint8_t> serialize(const google::protobuf::Message& message) {
  std::vector<uint8_t> bytes(message.ByteSizeLong());
  EXPECT_TRUE(message.SerializeToArray(bytes.data(), bytes.size()));
  return bytes;
}

std::vector<uint8_t> buildGetCapabilitiesCommand(
    uint32_t protocol_major = 2) {
  proto::RuntimeCommand command;
  command.mutable_protocol()->set_major(protocol_major);
  command.mutable_protocol()->set_minor(0);
  command.mutable_protocol()->set_rollout_epoch("test");
  command.set_command_type(proto::RUNTIME_COMMAND_TYPE_GET_CAPABILITIES);
  return serialize(command);
}

proto::RuntimeCommandResponse parseResponse(const std::vector<uint8_t>& bytes) {
  proto::RuntimeCommandResponse response;
  EXPECT_TRUE(response.ParseFromArray(bytes.data(), bytes.size()));
  return response;
}

proto::CapabilityInventory parseInventory(
    const proto::RuntimeCommandResponse& response) {
  proto::CapabilityResponse capabilities;
  EXPECT_TRUE(response.has_payload());
  EXPECT_TRUE(response.payload().UnpackTo(&capabilities));
  EXPECT_TRUE(capabilities.has_inventory());
  return capabilities.inventory();
}

void collectDescriptorFiles(
    const google::protobuf::FileDescriptor* descriptor,
    std::map<std::string, const google::protobuf::FileDescriptor*>& files) {
  if (!descriptor || !files.emplace(descriptor->name(), descriptor).second) {
    return;
  }
  for (int i = 0; i < descriptor->dependency_count(); ++i) {
    collectDescriptorFiles(descriptor->dependency(i), files);
  }
}

void clearDerivedFields(google::protobuf::DescriptorProto* message) {
  for (auto& field : *message->mutable_field()) {
    field.clear_json_name();
  }
  for (auto& extension : *message->mutable_extension()) {
    extension.clear_json_name();
  }
  for (auto& nested : *message->mutable_nested_type()) {
    clearDerivedFields(&nested);
  }
}

std::string canonicalDescriptorSet() {
  std::map<std::string, const google::protobuf::FileDescriptor*> files;
  collectDescriptorFiles(proto::RuntimeCommand::descriptor()->file(), files);
  google::protobuf::FileDescriptorSet result;
  for (const auto& [name, descriptor] : files) {
    (void)name;
    auto* output = result.add_file();
    descriptor->CopyTo(output);
    for (auto& message : *output->mutable_message_type()) {
      clearDerivedFields(&message);
    }
    for (auto& extension : *output->mutable_extension()) {
      extension.clear_json_name();
    }
  }
  return result.SerializeAsString();
}

std::string sha256(const std::string& input) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(
      input.data(), input.size(), digest.data(), &size,
      EVP_sha256(), nullptr), 1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

std::string toHex(std::string_view bytes) {
  constexpr char kDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (const unsigned char byte : bytes) {
    result.push_back(kDigits[byte >> 4]);
    result.push_back(kDigits[byte & 0x0f]);
  }
  return result;
}

TEST(NativeCommandGetCapabilitiesTest, ReturnsTypedV2SuccessResponse) {
  auto response = parseResponse(dispatchCommand(buildGetCapabilitiesCommand()));
  ASSERT_TRUE(response.success()) << response.error().message();
  ASSERT_TRUE(response.has_payload());
  EXPECT_EQ(
      response.payload().type_url(),
      "type.googleapis.com/pangrova.protocol.v2.CapabilityResponse");
}

TEST(NativeCommandGetCapabilitiesTest, AdvertisesSubstraitSupportInInventory) {
  auto inventory = parseInventory(parseResponse(
      dispatchCommand(buildGetCapabilitiesCommand())));
  std::unordered_set<std::string> rel_types(
      inventory.supported_substrait_rel_types().begin(),
      inventory.supported_substrait_rel_types().end());
  std::unordered_set<std::string> expression_types(
      inventory.supported_substrait_expression_types().begin(),
      inventory.supported_substrait_expression_types().end());
  EXPECT_TRUE(rel_types.contains("ReadRel"));
  EXPECT_TRUE(rel_types.contains("ExchangeRel"));
  EXPECT_TRUE(rel_types.contains("ExtensionSingleRel"));
  // The production converter routes the canonical ExtensionMultiRel to its
  // table-function implementation, so it must remain advertised.
  EXPECT_TRUE(rel_types.contains("ExtensionMultiRel"));
  EXPECT_TRUE(expression_types.contains("Literal"));
  EXPECT_TRUE(expression_types.contains("ScalarFunction"));
  EXPECT_EQ(rel_types.size(), 16u);
  EXPECT_EQ(expression_types.size(), 6u);
}

TEST(NativeCommandGetCapabilitiesTest, BindsInventoryToCanonicalDescriptor) {
  auto inventory = parseInventory(parseResponse(
      dispatchCommand(buildGetCapabilitiesCommand())));
  auto descriptor_set = canonicalDescriptorSet();
  EXPECT_EQ(inventory.protocol().major(), 2u);
  EXPECT_EQ(inventory.protocol().minor(), 0u);
  EXPECT_EQ(inventory.protocol().rollout_epoch(), "ai-native-function-resource-r3");
  EXPECT_EQ(inventory.protocol().descriptor_set_sha256(), sha256(descriptor_set));
  EXPECT_EQ(inventory.result_envelope_version(), 1u);
  EXPECT_FALSE(inventory.native_engine_version().empty());
}

TEST(NativeCommandGetCapabilitiesTest, AdvertisesTypedBoundedRetrievalContract) {
  const auto cache_path = std::filesystem::temp_directory_path() /
      ("pangrova-capability-cache-" + std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count()));
  pangrova::artifact::ImmutableArtifactCache::instance().configure(
      cache_path.string(), 1024 * 1024);
  pangrova::vector::diskann::DiskAnnIndexCache::instance().configure(
      1024 * 1024);
  auto inventory = parseInventory(parseResponse(
      dispatchCommand(buildGetCapabilitiesCommand())));
  ASSERT_EQ(inventory.capabilities_size(), 14);
  const auto& descriptor = inventory.capabilities(0);
  EXPECT_EQ(
      descriptor.ref().capability_id(),
      pangrova::capability::kExactTopKRetrievalId);
  EXPECT_TRUE(pangrova::capability::isExactTopKRetrieval(descriptor.ref()));
  EXPECT_GT(descriptor.limits().max_candidate_rows(), 0u);
  EXPECT_EQ(
      inventory.candidate_schema_version(),
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  ASSERT_EQ(inventory.supported_relation_type_urls_size(), 1);
  EXPECT_EQ(
      inventory.supported_relation_type_urls(0),
      pangrova::capability::kRelationDetailEnvelopeTypeUrl);

  const auto& rrf = inventory.capabilities(1);
  EXPECT_EQ(rrf.ref().capability_id(), pangrova::capability::kRrfRetrievalMergeId);
  EXPECT_TRUE(pangrova::capability::isRrfRetrievalMerge(rrf.ref()));
  EXPECT_EQ(rrf.kind(), proto::CAPABILITY_KIND_RETRIEVAL_MERGE);
  EXPECT_GT(rrf.limits().max_candidate_bytes(), 0u);

  const auto& linear = inventory.capabilities(2);
  EXPECT_EQ(
      linear.ref().capability_id(),
      pangrova::capability::kLinearRetrievalMergeId);
  EXPECT_TRUE(pangrova::capability::isLinearRetrievalMerge(linear.ref()));
  EXPECT_EQ(linear.arguments_size(), 2);

  const auto& artifact = inventory.capabilities(3);
  EXPECT_TRUE(pangrova::capability::isDiskAnnArtifactBuild(artifact.ref()));
  EXPECT_EQ(artifact.kind(), proto::CAPABILITY_KIND_BACKGROUND_SINK);
  ASSERT_EQ(artifact.supported_artifact_formats_size(), 1);
  EXPECT_EQ(
      artifact.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::diskAnnArtifactFormat().SerializeAsString());
  ASSERT_EQ(artifact.arguments_size(), 2);
  EXPECT_EQ(artifact.arguments(0).name(), "row_position");
  EXPECT_EQ(
      pangrova::capability::diskAnnArtifactFormat().major_version(), 2);

  const auto& hnsw_artifact = inventory.capabilities(4);
  EXPECT_TRUE(
      pangrova::capability::isHnswArtifactBuild(hnsw_artifact.ref()));
  EXPECT_EQ(
      hnsw_artifact.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::hnswArtifactFormat().SerializeAsString());

  const auto& ivf_pq_artifact = inventory.capabilities(5);
  EXPECT_TRUE(
      pangrova::capability::isIvfPqArtifactBuild(ivf_pq_artifact.ref()));
  EXPECT_EQ(
      ivf_pq_artifact.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::ivfPqArtifactFormat().SerializeAsString());

  const auto& bm25_artifact = inventory.capabilities(6);
  EXPECT_TRUE(
      pangrova::capability::isBm25ArtifactBuild(bm25_artifact.ref()));
  EXPECT_EQ(bm25_artifact.kind(), proto::CAPABILITY_KIND_BACKGROUND_SINK);
  ASSERT_EQ(bm25_artifact.supported_artifact_formats_size(), 1);
  EXPECT_EQ(
      bm25_artifact.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::bm25ArtifactFormat().SerializeAsString());

  const auto& generated_values = inventory.capabilities(7);
  EXPECT_TRUE(pangrova::capability::isGeneratedValuesArtifactBuild(
      generated_values.ref()));
  EXPECT_EQ(generated_values.ref().semantic_version(), "2.0.0");
  EXPECT_EQ(
      toHex(generated_values.ref().signature_sha256()),
      "8b22e62d0a4a228c69b8edc3c9e8920481bd6b83c54d168aeb6e62b0fabcbd5f");
  ASSERT_EQ(generated_values.supported_artifact_formats_size(), 1);
  EXPECT_EQ(generated_values.supported_artifact_formats(0).major_version(), 2);
  EXPECT_EQ(
      toHex(generated_values.supported_artifact_formats(0).schema_sha256()),
      "396997d51c3a824e49bd93b12cad2711a114b94ff8a52ab957829f4aab081698");

  const auto& tabular_export = inventory.capabilities(8);
  EXPECT_TRUE(pangrova::capability::isTabularExport(tabular_export.ref()));
  EXPECT_EQ(tabular_export.kind(), proto::CAPABILITY_KIND_BACKGROUND_SINK);
  EXPECT_EQ(
      tabular_export.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::tabularExportFormat().SerializeAsString());

  const auto& diskann = inventory.capabilities(9);
  EXPECT_TRUE(pangrova::capability::isDiskAnnRetrieval(diskann.ref()));
  EXPECT_EQ(diskann.kind(), proto::CAPABILITY_KIND_RETRIEVAL_SOURCE);
  ASSERT_EQ(diskann.supported_artifact_formats_size(), 1);
  EXPECT_EQ(
      diskann.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::diskAnnArtifactFormat().SerializeAsString());
  EXPECT_GT(diskann.limits().max_candidate_rows(), 0u);
  const auto& hnsw = inventory.capabilities(10);
  EXPECT_TRUE(pangrova::capability::isHnswRetrieval(hnsw.ref()));
  EXPECT_EQ(
      hnsw.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::hnswArtifactFormat().SerializeAsString());
  const auto& ivf_pq = inventory.capabilities(11);
  EXPECT_TRUE(pangrova::capability::isIvfPqRetrieval(ivf_pq.ref()));
  EXPECT_EQ(
      ivf_pq.supported_artifact_formats(0).SerializeAsString(),
      pangrova::capability::ivfPqArtifactFormat().SerializeAsString());
  const auto& bm25 = inventory.capabilities(12);
  EXPECT_TRUE(pangrova::capability::isBm25Retrieval(bm25.ref()));
  EXPECT_EQ(bm25.kind(), proto::CAPABILITY_KIND_RETRIEVAL_SOURCE);
  const auto& overlay = inventory.capabilities(13);
  EXPECT_TRUE(pangrova::capability::isGeneratedValuesOverlay(overlay.ref()));
  EXPECT_EQ(overlay.ref().semantic_version(), "2.0.0");
  EXPECT_EQ(overlay.kind(), proto::CAPABILITY_KIND_MATERIALIZER);
  EXPECT_EQ(
      toHex(overlay.ref().signature_sha256()),
      "0233b81f55d5f548c0d483b4da2bc8c7557ae742c16df19ef3ba73684a198ff5");
  pangrova::artifact::ImmutableArtifactCache::instance().shutdown();
  pangrova::vector::diskann::DiskAnnIndexCache::instance().shutdown();
  std::error_code ignored;
  std::filesystem::remove_all(cache_path, ignored);
}

TEST(NativeCommandGetCapabilitiesTest, OmitsDiskAnnWhenArtifactCacheIsNotReady) {
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled()) {
    pangrova::artifact::ImmutableArtifactCache::instance().shutdown();
  }
  if (pangrova::vector::diskann::DiskAnnIndexCache::instance().enabled()) {
    pangrova::vector::diskann::DiskAnnIndexCache::instance().shutdown();
  }
  auto inventory = parseInventory(parseResponse(
      dispatchCommand(buildGetCapabilitiesCommand())));
  EXPECT_EQ(inventory.capabilities_size(), 9);
  EXPECT_TRUE(std::none_of(
      inventory.capabilities().begin(), inventory.capabilities().end(),
      [](const proto::CapabilityDescriptor& descriptor) {
        return pangrova::capability::isDiskAnnRetrieval(descriptor.ref()) ||
            pangrova::capability::isHnswRetrieval(descriptor.ref()) ||
            pangrova::capability::isIvfPqRetrieval(descriptor.ref()) ||
            pangrova::capability::isBm25Retrieval(descriptor.ref()) ||
            pangrova::capability::isGeneratedValuesOverlay(descriptor.ref());
      }));
}

TEST(NativeCommandGetCapabilitiesTest,
     AdvertisesOverlayWithoutAnnParsedCache) {
  const auto cache_path = std::filesystem::temp_directory_path() /
      ("pangrova-overlay-capability-cache-" + std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count()));
  pangrova::artifact::ImmutableArtifactCache::instance().configure(
      cache_path.string(), 1024 * 1024);
  if (pangrova::vector::diskann::DiskAnnIndexCache::instance().enabled()) {
    pangrova::vector::diskann::DiskAnnIndexCache::instance().shutdown();
  }

  auto inventory = parseInventory(parseResponse(
      dispatchCommand(buildGetCapabilitiesCommand())));
  EXPECT_TRUE(std::any_of(
      inventory.capabilities().begin(), inventory.capabilities().end(),
      [](const proto::CapabilityDescriptor& descriptor) {
        return pangrova::capability::isGeneratedValuesOverlay(descriptor.ref());
      }));
  EXPECT_TRUE(std::none_of(
      inventory.capabilities().begin(), inventory.capabilities().end(),
      [](const proto::CapabilityDescriptor& descriptor) {
        return pangrova::capability::isDiskAnnRetrieval(descriptor.ref());
      }));

  pangrova::artifact::ImmutableArtifactCache::instance().shutdown();
  std::error_code ignored;
  std::filesystem::remove_all(cache_path, ignored);
}

TEST(NativeCommandGetCapabilitiesTest, RejectsWrongProtocolMajor) {
  auto response = parseResponse(dispatchCommand(buildGetCapabilitiesCommand(1)));
  EXPECT_FALSE(response.success());
  EXPECT_EQ(
      response.error().error_class(), proto::ERROR_CLASS_PROTOCOL_MISMATCH);
  EXPECT_NE(response.error().message().find("protocol major"), std::string::npos);
}

TEST(NativeCommandGetCapabilitiesTest, RejectsMalformedEnvelope) {
  auto response = parseResponse(dispatchCommand({0xff, 0xff, 0xff}));
  EXPECT_FALSE(response.success());
  EXPECT_FALSE(response.error().message().empty());
}

}  // namespace
}  // namespace pangrova::jni::test
