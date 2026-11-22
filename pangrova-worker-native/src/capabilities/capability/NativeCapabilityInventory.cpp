#include "capabilities/capability/NativeCapabilityInventory.h"

#include <map>
#include <string>

#include "capabilities/artifact/ImmutableArtifactCache.h"
#include "capabilities/capability/FunctionContractEncoding.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "google/protobuf/descriptor.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "vector/diskann/DiskAnnIndexCache.h"

namespace pangrova::capability {
namespace {
namespace proto = protocol::v2;

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

void normalizeDescriptor(google::protobuf::FileDescriptorProto* descriptor) {
  for (auto& message : *descriptor->mutable_message_type()) {
    clearDerivedFields(&message);
  }
  for (auto& extension : *descriptor->mutable_extension()) {
    extension.clear_json_name();
  }
}

google::protobuf::FileDescriptorSet canonicalDescriptorSet() {
  std::map<std::string, const google::protobuf::FileDescriptor*> files;
  collectDescriptorFiles(proto::RuntimeCommand::descriptor()->file(), files);
  google::protobuf::FileDescriptorSet descriptor_set;
  for (const auto& [name, descriptor] : files) {
    (void)name;
    auto* output = descriptor_set.add_file();
    descriptor->CopyTo(output);
    normalizeDescriptor(output);
  }
  return descriptor_set;
}

}  // namespace

const proto::ProtocolVersion& canonicalProtocolVersion() {
  static const auto version = [] {
    proto::ProtocolVersion identity;
    identity.set_major(2);
    identity.set_minor(0);
    identity.set_descriptor_set_sha256(functionContractDigest(canonicalDescriptorSet()));
    identity.set_rollout_epoch("ai-native-function-resource-r3");
    return identity;
  }();
  return version;
}

proto::CapabilityInventory runtimeCapabilityInventory(const facebook::velox::exec::FunctionRegistrySnapshot& registry) {
  proto::CapabilityInventory inventory;
  auto* inventory_v2 = &inventory;
  *inventory_v2->mutable_protocol() = canonicalProtocolVersion();
  *inventory_v2->add_capabilities() =
      pangrova::capability::exactTopKRetrievalDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::rrfRetrievalMergeDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::linearRetrievalMergeDescriptor();
  if (registry.scalars().contains(std::string(pangrova::capability::kGovernedRerankFunction))) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::crossEncoderRerankDescriptor();
  }
  if (registry.scalars().contains("ai_complete")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiTextGenerationDescriptor();
  }
  if (registry.scalars().contains("ai_describe_image")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiMultimodalGenerationDescriptor();
  }
  if (registry.scalars().contains("ai_embed")) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::aiEmbeddingDescriptor();
  }
  *inventory_v2->add_capabilities() =
      pangrova::capability::diskAnnArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::hnswArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::ivfPqArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::bm25ArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::generatedValuesArtifactBuildDescriptor();
  *inventory_v2->add_capabilities() =
      pangrova::capability::tabularExportDescriptor();
  // Retrieval is advertised only when the node-local immutable artifact
  // substrate is actually ready. The scheduler therefore never routes a
  // DiskANN task to a Worker that merely contains the software implementation
  // but cannot satisfy its storage contract.
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled() &&
      pangrova::vector::diskann::DiskAnnIndexCache::instance().enabled()) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::diskAnnRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::filteredDiskAnnRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::hnswRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::ivfPqRetrievalDescriptor();
    *inventory_v2->add_capabilities() =
        pangrova::capability::bm25RetrievalDescriptor();
  }
  // The generated-value overlay only needs checksum-verified immutable object
  // materialization. It must not inherit the independent ANN parsed-cache
  // readiness gate.
  if (pangrova::artifact::ImmutableArtifactCache::instance().enabled()) {
    *inventory_v2->add_capabilities() =
        pangrova::capability::generatedValuesOverlayDescriptor();
  }
  inventory_v2->add_supported_relation_type_urls(
      pangrova::capability::kRelationDetailEnvelopeTypeUrl);
  inventory_v2->set_candidate_schema_version(
      pangrova::capability::kRetrievalCandidateSchemaVersion);
  inventory_v2->set_result_envelope_version(1);
  inventory_v2->set_native_engine_version("0.1.0");
  for (const auto* rel :
       {"ReadRel", "FilterRel", "ProjectRel", "AggregateRel", "HashJoinRel",
        "MergeJoinRel", "JoinRel", "NestedLoopJoinRel", "ExchangeRel", "ExtensionLeafRel",
        "SortRel", "FetchRel",
        "ConsistentPartitionWindowRel", "SetRel", "CrossRel",
        "ExtensionSingleRel"}) {
    inventory_v2->add_supported_substrait_rel_types(rel);
  }
  for (const auto* expression : {"FieldReference", "Literal", "ScalarFunction",
                                 "Cast", "IfThen", "Nested", "WindowFunction"}) {
    inventory_v2->add_supported_substrait_expression_types(expression);
  }
  return inventory;
}

}  // namespace pangrova::capability
