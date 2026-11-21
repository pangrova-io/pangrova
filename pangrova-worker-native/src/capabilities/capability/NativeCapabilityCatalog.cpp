#include "capabilities/capability/NativeCapabilityCatalog.h"

#include "substrait/type.pb.h"

namespace pangrova::capability {
namespace {

constexpr char kExactTopKSignature[] =
    "\xEA\x4C\x6E\x76\x73\xF6\x06\x73"
    "\xA5\x1E\xE3\x53\x9D\x38\x66\x0D"
    "\x03\xDD\x9B\x2C\x54\x1A\x23\x1B"
    "\x46\x84\x81\x0E\xFC\xCE\x14\x09";

constexpr char kRrfMergeSignature[] =
    "\x32\x66\x98\xD1\xA9\x83\x9E\xE9"
    "\xD5\xC4\x90\x41\x58\xB6\x6A\x7B"
    "\xF2\x29\xAB\xFE\xEA\xEC\x4E\xD3"
    "\x3A\xFD\x4F\xD3\xEA\xD6\xF4\xD9";

constexpr char kLinearMergeSignature[] =
    "\x82\x7C\x89\xE4\x37\xA9\x73\x85"
    "\x8D\x1F\x63\x70\x2B\x2C\x7E\x25"
    "\x74\xC5\x93\xF3\x53\x72\x08\x49"
    "\xD2\x46\xA9\x0A\x42\x04\x7D\x0F";

constexpr char kCrossEncoderRerankSignature[] =
    "\x1B\x4F\xBD\xB7\x15\x23\x23\xD8"
    "\x10\x48\x20\x93\x21\x74\x38\x3E"
    "\x5C\x0D\xAA\x62\xFA\x17\x5C\xE9"
    "\xDB\xAD\x77\xFD\x4F\x97\x16\x3A";

constexpr char kAiTextGenerationSignature[] =
    "\x92\xBD\xD3\x26\xE7\x84\x88\xC3"
    "\x23\xF1\x52\x37\x63\xA1\x0A\x9C"
    "\xA7\xAE\x85\xD2\x89\xC7\x77\x11"
    "\xDB\x83\x59\x63\xAB\x09\xB0\xC5";

constexpr char kAiMultimodalGenerationSignature[] =
    "\x75\x78\x4B\x6E\xEE\x3F\xE4\x20"
    "\x88\x2E\xCB\x65\x15\x99\x15\x3C"
    "\x13\xDD\x04\xA5\x14\x52\x3C\x8D"
    "\xC8\xF9\xC2\x5C\xBD\xE4\x0E\xB3";

constexpr char kAiEmbeddingSignature[] =
    "\x9D\x33\xF2\x84\x54\xD1\xD6\x82"
    "\xEF\x49\x89\xB6\x9C\x72\xC3\x57"
    "\x35\xFA\x7D\xC2\x91\xF7\x57\xBF"
    "\x07\x9E\x9C\xFD\x1B\xF3\xA6\xD7";

constexpr char kRrfOptionsSignature[] =
    "\xCB\x89\x95\x20\x80\xE5\x91\x65"
    "\x0E\xB2\xD3\x3C\x6C\xFB\x3C\x89"
    "\x2E\xB8\xAC\x1D\x09\x5B\x66\x8D"
    "\x7B\x98\xD4\xC2\x25\x63\xE7\xBA";

constexpr char kLinearOptionsSignature[] =
    "\x23\xA3\x8C\x80\xAB\xDE\xF8\x21"
    "\xE0\x92\xAD\x92\xB4\x7A\x32\x52"
    "\x53\x5F\x6C\xB9\xAF\x67\x2B\xAD"
    "\x1A\x63\x25\x9F\x3D\xC7\x65\x32";

constexpr char kDiskAnnBuildSignature[] =
    "\xCC\x34\xA0\x99\xDB\xCB\xF4\x26"
    "\xE5\x12\x8D\x1E\xFE\xF7\xE5\x4D"
    "\xA0\x67\x69\x11\xBB\xC2\x21\xDD"
    "\x6A\x4A\x04\xF0\xD4\x55\xE8\xDE";

constexpr char kDiskAnnRetrievalSignature[] =
    "\x60\x3A\x3F\x4D\xEB\xD0\xD7\xD8"
    "\x30\xBD\xF0\x4C\x52\xF7\x89\xB7"
    "\x0E\x18\x23\x47\xBB\x7A\x7E\x52"
    "\xB2\xE3\x0D\xA5\x14\xF6\xDA\x82";

constexpr char kDiskAnnFormatSignature[] =
    "\xDD\x6E\xCA\x5D\x83\x6F\xFA\xDA"
    "\x26\x38\x4E\x13\xF0\x5F\x68\x4A"
    "\x48\xEE\x22\x1F\xED\x31\x80\x24"
    "\xF8\xD0\x6A\x91\x58\xED\x13\x2F";

constexpr char kHnswBuildSignature[] =
    "\xFB\xE3\x64\x17\xD9\xA1\xF6\xFD"
    "\x4E\x4D\xA3\x16\x35\x96\xB2\x8E"
    "\x80\xBC\x1A\x76\x1D\x72\x8E\x25"
    "\x9B\x04\x5E\xE2\x12\x94\x8E\xAB";

constexpr char kHnswRetrievalSignature[] =
    "\x66\xA9\x57\xF2\xB4\x2F\x50\xA0"
    "\xE5\x66\x5A\x76\xB4\xD0\xD8\x76"
    "\xB1\x81\x14\x18\x99\x2C\x9E\x6E"
    "\x35\x1F\x5A\xCF\xC1\x3A\xA8\xA7";

constexpr char kHnswFormatSignature[] =
    "\x4F\xF1\xA3\x1A\x97\x36\xBD\x57"
    "\xC4\x07\x36\xD1\x8E\x1A\x73\x8C"
    "\xD8\x3B\x05\xA3\xB0\x37\x3A\xB8"
    "\x68\x9C\x73\x76\x6F\x60\x13\x5A";

constexpr char kIvfPqBuildSignature[] =
    "\x65\xEF\xC3\x07\x59\x1D\x43\xD1"
    "\x7D\xC7\x39\x7B\x80\x94\x5B\x27"
    "\xAF\xDB\xDB\xE4\xF5\x27\x90\xBE"
    "\x7F\x8F\x8A\x9D\x4D\x5B\x9F\xC9";

constexpr char kIvfPqRetrievalSignature[] =
    "\xC6\x11\xF0\xA6\xF3\x6E\xEF\x10"
    "\xDC\x54\xFE\x27\xD5\x4F\xA6\xA0"
    "\x43\xB9\xB5\x86\xA2\xC7\xAB\xF0"
    "\x19\x23\x7A\x46\x8C\x68\xDE\x90";

constexpr char kIvfPqFormatSignature[] =
    "\xD8\x41\xFC\x5B\x1B\x4F\x83\xDE"
    "\xC9\xEA\x0B\x45\xD2\x88\x58\xFA"
    "\xC0\xCE\xF8\xEF\xE9\xFB\xCB\x7B"
    "\x26\xFF\xC6\x73\x11\xE1\x87\x2A";

constexpr char kBm25BuildSignature[] =
    "\x9A\xD9\x5B\x04\x7D\xF1\x32\x0D"
    "\xEA\x29\x53\x0B\xAE\x56\x64\x36"
    "\xB8\xEE\x0A\xC7\x2A\x9A\x78\xA2"
    "\xF5\xD1\x97\x38\xCC\xB6\x35\x56";

constexpr char kBm25RetrievalSignature[] =
    "\xD7\xF4\x53\xCA\xB4\x6B\xCA\x0C"
    "\xCA\x2E\xBF\x86\xFB\xE7\x55\x98"
    "\xD7\x09\xAC\xFA\x56\x52\x9D\xE9"
    "\x44\x5A\x9A\x36\x2F\x11\x33\x4C";

constexpr char kBm25FormatSignature[] =
    "\x94\xEB\x1C\x3A\x58\xB2\xD3\xEF"
    "\x94\x7A\x3F\xBE\x79\x49\x1A\xE2"
    "\x7F\x77\x28\xAE\xBF\x38\x45\xFE"
    "\xBB\x30\x1A\x0B\x16\x81\x11\x06";

constexpr char kGeneratedValuesBuildSignature[] =
    "\x8B\x22\xE6\x2D\x0A\x4A\x22\x8C"
    "\x69\xB8\xED\xC3\xC9\xE8\x92\x04"
    "\x81\xBD\x6B\x83\xC5\x4D\x16\x8A"
    "\xEB\x6E\x62\xB0\xFA\xBC\xBD\x5F";

constexpr char kGeneratedValuesFormatSignature[] =
    "\x39\x69\x97\xD5\x1C\x3A\x82\x4E"
    "\x49\xBD\x93\xB1\x2C\xAD\x27\x11"
    "\xA1\x14\xB9\x4F\xF8\xA5\x2A\xB9"
    "\x57\x82\x9F\x4A\xAB\x08\x16\x98";

constexpr char kGeneratedValuesOverlaySignature[] =
    "\x02\x33\xB8\x1F\x55\xD5\xF5\x48"
    "\xC0\xD4\x83\xB4\xDA\x2B\xC8\xC7"
    "\x55\x7A\xE7\x42\xC1\x6D\xF1\x9E"
    "\xF3\xBA\x73\x68\x4A\x19\x8F\xF5";

constexpr char kTabularExportSignature[] =
    "\x21\x4E\xFE\x8E\x4A\xAB\x98\xCD"
    "\x87\x51\x9A\x90\x1E\x84\x07\xF3"
    "\xBF\x4E\xB6\x4B\xF8\x52\x7A\x86"
    "\x45\x8D\xD5\x55\x60\x02\xA2\x62";

constexpr char kTabularExportFormatSignature[] =
    "\x8E\xF0\xB6\xB1\xE7\xD6\x74\xA7"
    "\x58\x51\x07\x76\x7A\x65\x52\x36"
    "\xF2\xB5\x42\x01\x2C\x79\x8F\xCF"
    "\x9C\x7A\x52\x57\x77\x5A\xC2\xF4";

constexpr char kPhysicalRowLocatorSignature[] =
    "\xCD\x02\x82\xE4\x13\x15\x9F\xD4"
    "\x13\xEC\xE1\x66\x3D\xB7\x77\xD3"
    "\xC1\x7E\x2B\xDE\xE4\xB0\x6D\xCC"
    "\xD2\xAF\xA0\xCC\xBF\x4F\x22\x48";

void setScoreArgumentType(
    pangrova::protocol::v2::SignatureArgument* argument) {
  auto* logical_type = argument->mutable_type();
  logical_type->set_canonical_sql("DOUBLE");
  logical_type->mutable_substrait_type()->mutable_fp64();
}

void setArgumentType(
    pangrova::protocol::v2::SignatureArgument* argument,
    const std::string& name,
    const std::string& canonical_sql) {
  argument->set_name(name);
  auto* type = argument->mutable_type();
  type->set_canonical_sql(canonical_sql);
  if (canonical_sql == "DOUBLE") {
    type->mutable_substrait_type()->mutable_fp64();
  } else if (canonical_sql == "VARCHAR") {
    type->mutable_substrait_type()->mutable_string();
  } else if (canonical_sql == "BIGINT") {
    type->mutable_substrait_type()->mutable_i64();
  }
}

bool matchesCapability(
    const pangrova::protocol::v2::CapabilityRef& capability,
    std::string_view id,
    std::string_view version,
    const std::string& signature,
    std::string_view implementation_build_id) {
  return capability.capability_id() == id &&
      capability.semantic_version() == version &&
      capability.signature_sha256() == signature &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == implementation_build_id);
}

pangrova::protocol::v2::CapabilityDescriptor mergeDescriptor(
    std::string_view id,
    const std::string& signature,
    const char* options_signature) {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(id);
  ref->set_semantic_version(kRetrievalMergeVersion);
  ref->set_signature_sha256(signature);
  ref->set_implementation_build_id(kRetrievalMergeBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RETRIEVAL_MERGE);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "score", "DOUBLE");
  setArgumentType(descriptor.add_arguments(), "source_id", "VARCHAR");
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.set_options_schema_sha256(options_signature, 32);
  auto* limits = descriptor.mutable_limits();
  limits->set_max_input_bytes(kRetrievalMergeMaxCandidateBytes);
  limits->set_max_output_bytes(kRetrievalMergeMaxCandidateBytes);
  limits->set_max_candidate_rows(kRetrievalMergeMaxCandidateRows);
  limits->set_max_candidate_bytes(kRetrievalMergeMaxCandidateBytes);
  return descriptor;
}

}  // namespace

std::string exactTopKRetrievalSignature() {
  return std::string(kExactTopKSignature, 32);
}

bool isExactTopKRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kExactTopKRetrievalId &&
      capability.semantic_version() == kExactTopKRetrievalVersion &&
      capability.signature_sha256() == exactTopKRetrievalSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor
exactTopKRetrievalDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kExactTopKRetrievalId);
  ref->set_semantic_version(kExactTopKRetrievalVersion);
  ref->set_signature_sha256(exactTopKRetrievalSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RETRIEVAL_SOURCE);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_TABULAR);

  auto* score = descriptor.add_arguments();
  score->set_name("score");
  setScoreArgumentType(score);

  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  auto* limits = descriptor.mutable_limits();
  limits->set_max_input_bytes(256ULL * 1024 * 1024);
  limits->set_max_output_bytes(256ULL * 1024 * 1024);
  limits->set_max_candidate_rows(kExactTopKMaxCandidateRows);
  limits->set_max_candidate_bytes(256ULL * 1024 * 1024);
  return descriptor;
}

std::string rrfRetrievalMergeSignature() {
  return std::string(kRrfMergeSignature, 32);
}

std::string linearRetrievalMergeSignature() {
  return std::string(kLinearMergeSignature, 32);
}

bool isRrfRetrievalMerge(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kRrfRetrievalMergeId, kRetrievalMergeVersion,
      rrfRetrievalMergeSignature(), kRetrievalMergeBuildId);
}

bool isLinearRetrievalMerge(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kLinearRetrievalMergeId, kRetrievalMergeVersion,
      linearRetrievalMergeSignature(), kRetrievalMergeBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor rrfRetrievalMergeDescriptor() {
  return mergeDescriptor(
      kRrfRetrievalMergeId, rrfRetrievalMergeSignature(),
      kRrfOptionsSignature);
}

pangrova::protocol::v2::CapabilityDescriptor linearRetrievalMergeDescriptor() {
  return mergeDescriptor(
      kLinearRetrievalMergeId, linearRetrievalMergeSignature(),
      kLinearOptionsSignature);
}

std::string crossEncoderRerankSignature() {
  return std::string(kCrossEncoderRerankSignature, 32);
}

bool isCrossEncoderRerank(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kCrossEncoderRerankId &&
      capability.semantic_version() == kCrossEncoderRerankVersion &&
      capability.signature_sha256() == crossEncoderRerankSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kRetrievalMergeBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor crossEncoderRerankDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kCrossEncoderRerankId);
  ref->set_semantic_version(kCrossEncoderRerankVersion);
  ref->set_signature_sha256(crossEncoderRerankSignature());
  ref->set_implementation_build_id(kRetrievalMergeBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RERANK);
  descriptor.set_placement(
      EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_TEXT);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "score", "DOUBLE");
  setArgumentType(descriptor.add_arguments(), "query", "VARCHAR");
  setArgumentType(descriptor.add_arguments(), "document", "VARCHAR");
  descriptor.set_deterministic(false);
  descriptor.set_retry_class(RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
  descriptor.set_required_privilege("AI_MODEL_USAGE");
  auto* limits = descriptor.mutable_limits();
  limits->set_max_batch_items(kCrossEncoderRerankMaxRows);
  limits->set_max_candidate_rows(kCrossEncoderRerankMaxRows);
  limits->set_max_input_bytes(kRetrievalMergeMaxCandidateBytes);
  limits->set_max_output_bytes(kRetrievalMergeMaxCandidateBytes);
  limits->set_max_candidate_bytes(kRetrievalMergeMaxCandidateBytes);
  return descriptor;
}

std::string aiTextGenerationSignature() {
  return std::string(kAiTextGenerationSignature, 32);
}

std::string aiMultimodalGenerationSignature() {
  return std::string(kAiMultimodalGenerationSignature, 32);
}

std::string aiEmbeddingSignature() {
  return std::string(kAiEmbeddingSignature, 32);
}

bool isAiTextGeneration(const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kAiTextGenerationId, kAiInvocationVersion,
      aiTextGenerationSignature(), kAiInvocationBuildId);
}

bool isAiMultimodalGeneration(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kAiMultimodalGenerationId, kAiInvocationVersion,
      aiMultimodalGenerationSignature(), kAiInvocationBuildId);
}

bool isAiEmbedding(const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kAiEmbeddingId, kAiInvocationVersion,
      aiEmbeddingSignature(), kAiInvocationBuildId);
}

namespace {

using namespace pangrova::protocol::v2;

CapabilityDescriptor aiInvocationDescriptor(
    std::string_view id,
    const std::string& signature,
    std::initializer_list<Modality> modalities) {
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(id);
  ref->set_semantic_version(kAiInvocationVersion);
  ref->set_signature_sha256(signature);
  ref->set_implementation_build_id(kAiInvocationBuildId);
  descriptor.set_kind(CAPABILITY_KIND_MODEL_INVOCATION);
  descriptor.set_placement(EXECUTION_PLACEMENT_WORKER_LOCAL_GOVERNED_CALLBACK);
  descriptor.add_execution_modes(EXECUTION_MODE_SYNC);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  for (auto modality : modalities) {
    descriptor.add_modalities(modality);
  }
  setArgumentType(descriptor.add_arguments(), "model", "VARCHAR");
  descriptor.set_deterministic(false);
  descriptor.set_retry_class(RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
  descriptor.set_required_privilege("AI_MODEL_USAGE");
  return descriptor;
}

}  // namespace

pangrova::protocol::v2::CapabilityDescriptor aiTextGenerationDescriptor() {
  return aiInvocationDescriptor(
      kAiTextGenerationId, aiTextGenerationSignature(), {MODALITY_TEXT});
}

pangrova::protocol::v2::CapabilityDescriptor aiMultimodalGenerationDescriptor() {
  return aiInvocationDescriptor(
      kAiMultimodalGenerationId, aiMultimodalGenerationSignature(),
      {MODALITY_TEXT, MODALITY_IMAGE, MODALITY_AUDIO, MODALITY_VIDEO,
       MODALITY_DOCUMENT});
}

pangrova::protocol::v2::CapabilityDescriptor aiEmbeddingDescriptor() {
  return aiInvocationDescriptor(
      kAiEmbeddingId, aiEmbeddingSignature(),
      {MODALITY_TEXT, MODALITY_IMAGE, MODALITY_AUDIO, MODALITY_VIDEO,
       MODALITY_DOCUMENT, MODALITY_VECTOR});
}

std::string diskAnnArtifactBuildSignature() {
  return std::string(kDiskAnnBuildSignature, 32);
}

std::string diskAnnArtifactFormatSignature() {
  return std::string(kDiskAnnFormatSignature, 32);
}

std::string physicalRowLocatorSignature() {
  return std::string(kPhysicalRowLocatorSignature, 32);
}

bool isDiskAnnArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kDiskAnnArtifactBuildId &&
      capability.semantic_version() == kDiskAnnArtifactBuildVersion &&
      capability.signature_sha256() == diskAnnArtifactBuildSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef diskAnnArtifactFormat() {
  pangrova::protocol::v2::ArtifactFormatRef format;
  format.set_format_id(kDiskAnnArtifactFormatId);
  format.set_major_version(kDiskAnnArtifactFormatMajor);
  format.set_minor_version(0);
  format.set_schema_sha256(diskAnnArtifactFormatSignature());
  format.set_writer_build_id(kExactTopKRetrievalBuildId);
  return format;
}

pangrova::protocol::v2::RowLocatorContract physicalRowLocatorContract() {
  using namespace pangrova::protocol::v2;
  RowLocatorContract contract;
  contract.set_schema_version(1);
  contract.set_kind(ROW_LOCATOR_KIND_PHYSICAL_ROW_POSITION);
  contract.set_schema_sha256(physicalRowLocatorSignature());
  contract.set_source_object_scoped(true);
  return contract;
}

pangrova::protocol::v2::CapabilityDescriptor diskAnnArtifactBuildDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kDiskAnnArtifactBuildId);
  ref->set_semantic_version(kDiskAnnArtifactBuildVersion);
  ref->set_signature_sha256(diskAnnArtifactBuildSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_BACKGROUND_SINK);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "row_position", "BIGINT");
  auto* vector = descriptor.add_arguments();
  vector->set_name("vector");
  vector->mutable_type()->set_canonical_sql("VECTOR<FLOAT>");
  vector->mutable_type()
      ->mutable_substrait_type()
      ->mutable_list()
      ->mutable_type()
      ->mutable_fp32();
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("CREATE INDEX");
  auto* limits = descriptor.mutable_limits();
  limits->set_max_batch_items(kDiskAnnArtifactMaxRows);
  limits->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  limits->set_max_output_bytes(2ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = diskAnnArtifactFormat();
  return descriptor;
}

namespace {
constexpr char kFilteredDiskAnnSignature[] = "\xae\x26\xc5\x78\x46\xbf\x73\x27\x44\x79\x75\xab\xda\xef\x0e\xe5\xd4\xe0\x3a\xa8\x55\xb9\x02\xcb\x01\x92\xc8\xb3\x60\x04\x77\x16";
}

bool isFilteredDiskAnnRetrieval(const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kFilteredDiskAnnRetrievalId &&
      capability.semantic_version() == "1.0.0" &&
      capability.signature_sha256() == std::string(kFilteredDiskAnnSignature, 32) &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor filteredDiskAnnRetrievalDescriptor() {
  auto descriptor = diskAnnRetrievalDescriptor();
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kFilteredDiskAnnRetrievalId);
  ref->set_semantic_version("1.0.0");
  ref->set_signature_sha256(kFilteredDiskAnnSignature, 32);
  descriptor.set_options_schema_sha256(kFilteredDiskAnnSignature, 32);
  return descriptor;
}

std::string diskAnnRetrievalSignature() {
  return std::string(kDiskAnnRetrievalSignature, 32);
}

bool isDiskAnnRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kDiskAnnRetrievalId &&
      capability.semantic_version() == kDiskAnnRetrievalVersion &&
      capability.signature_sha256() == diskAnnRetrievalSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor diskAnnRetrievalDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kDiskAnnRetrievalId);
  ref->set_semantic_version(kDiskAnnRetrievalVersion);
  ref->set_signature_sha256(diskAnnRetrievalSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RETRIEVAL_SOURCE);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  auto* score = descriptor.add_arguments();
  score->set_name("score");
  setScoreArgumentType(score);
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.set_options_schema_sha256(kDiskAnnRetrievalSignature, 32);
  auto* limits = descriptor.mutable_limits();
  limits->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  limits->set_max_output_bytes(256ULL * 1024 * 1024);
  limits->set_max_candidate_rows(kExactTopKMaxCandidateRows);
  limits->set_max_candidate_bytes(256ULL * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = diskAnnArtifactFormat();
  return descriptor;
}

namespace {

pangrova::protocol::v2::ArtifactFormatRef annArtifactFormat(
    std::string_view id, uint32_t major, const std::string& signature) {
  pangrova::protocol::v2::ArtifactFormatRef format;
  format.set_format_id(id);
  format.set_major_version(major);
  format.set_minor_version(0);
  format.set_schema_sha256(signature);
  format.set_writer_build_id(kExactTopKRetrievalBuildId);
  return format;
}

pangrova::protocol::v2::CapabilityDescriptor annBuildDescriptor(
    std::string_view id,
    std::string_view version,
    const std::string& signature,
    uint64_t max_rows,
    const pangrova::protocol::v2::ArtifactFormatRef& format) {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(id);
  ref->set_semantic_version(version);
  ref->set_signature_sha256(signature);
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_BACKGROUND_SINK);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "row_position", "BIGINT");
  auto* vector = descriptor.add_arguments();
  vector->set_name("vector");
  vector->mutable_type()->set_canonical_sql("VECTOR<FLOAT>");
  vector->mutable_type()
      ->mutable_substrait_type()
      ->mutable_list()
      ->mutable_type()
      ->mutable_fp32();
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("CREATE INDEX");
  descriptor.mutable_limits()->set_max_batch_items(max_rows);
  descriptor.mutable_limits()->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  descriptor.mutable_limits()->set_max_output_bytes(2ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = format;
  return descriptor;
}

pangrova::protocol::v2::CapabilityDescriptor annRetrievalDescriptor(
    std::string_view id,
    std::string_view version,
    const std::string& signature,
    const pangrova::protocol::v2::ArtifactFormatRef& format) {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(id);
  ref->set_semantic_version(version);
  ref->set_signature_sha256(signature);
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RETRIEVAL_SOURCE);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  auto* score = descriptor.add_arguments();
  score->set_name("score");
  setScoreArgumentType(score);
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.set_options_schema_sha256(signature);
  auto* limits = descriptor.mutable_limits();
  limits->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  limits->set_max_output_bytes(256ULL * 1024 * 1024);
  limits->set_max_candidate_rows(kExactTopKMaxCandidateRows);
  limits->set_max_candidate_bytes(256ULL * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = format;
  return descriptor;
}

}  // namespace

std::string hnswArtifactBuildSignature() {
  return std::string(kHnswBuildSignature, 32);
}

std::string hnswArtifactFormatSignature() {
  return std::string(kHnswFormatSignature, 32);
}

bool isHnswArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kHnswArtifactBuildId, kHnswArtifactBuildVersion,
      hnswArtifactBuildSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef hnswArtifactFormat() {
  return annArtifactFormat(
      kHnswArtifactFormatId, kHnswArtifactFormatMajor,
      hnswArtifactFormatSignature());
}

pangrova::protocol::v2::CapabilityDescriptor hnswArtifactBuildDescriptor() {
  return annBuildDescriptor(
      kHnswArtifactBuildId, kHnswArtifactBuildVersion,
      hnswArtifactBuildSignature(), kHnswArtifactMaxRows,
      hnswArtifactFormat());
}

std::string hnswRetrievalSignature() {
  return std::string(kHnswRetrievalSignature, 32);
}

bool isHnswRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kHnswRetrievalId, kHnswRetrievalVersion,
      hnswRetrievalSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor hnswRetrievalDescriptor() {
  return annRetrievalDescriptor(
      kHnswRetrievalId, kHnswRetrievalVersion, hnswRetrievalSignature(),
      hnswArtifactFormat());
}

std::string ivfPqArtifactBuildSignature() {
  return std::string(kIvfPqBuildSignature, 32);
}

std::string ivfPqArtifactFormatSignature() {
  return std::string(kIvfPqFormatSignature, 32);
}

bool isIvfPqArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kIvfPqArtifactBuildId, kIvfPqArtifactBuildVersion,
      ivfPqArtifactBuildSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef ivfPqArtifactFormat() {
  return annArtifactFormat(
      kIvfPqArtifactFormatId, kIvfPqArtifactFormatMajor,
      ivfPqArtifactFormatSignature());
}

pangrova::protocol::v2::CapabilityDescriptor ivfPqArtifactBuildDescriptor() {
  return annBuildDescriptor(
      kIvfPqArtifactBuildId, kIvfPqArtifactBuildVersion,
      ivfPqArtifactBuildSignature(), kIvfPqArtifactMaxRows,
      ivfPqArtifactFormat());
}

std::string ivfPqRetrievalSignature() {
  return std::string(kIvfPqRetrievalSignature, 32);
}

bool isIvfPqRetrieval(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kIvfPqRetrievalId, kIvfPqRetrievalVersion,
      ivfPqRetrievalSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor ivfPqRetrievalDescriptor() {
  return annRetrievalDescriptor(
      kIvfPqRetrievalId, kIvfPqRetrievalVersion, ivfPqRetrievalSignature(),
      ivfPqArtifactFormat());
}

std::string bm25ArtifactBuildSignature() {
  return std::string(kBm25BuildSignature, 32);
}

std::string bm25ArtifactFormatSignature() {
  return std::string(kBm25FormatSignature, 32);
}

bool isBm25ArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kBm25ArtifactBuildId &&
      capability.semantic_version() == kBm25ArtifactBuildVersion &&
      capability.signature_sha256() == bm25ArtifactBuildSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef bm25ArtifactFormat() {
  pangrova::protocol::v2::ArtifactFormatRef format;
  format.set_format_id(kBm25ArtifactFormatId);
  format.set_major_version(kBm25ArtifactFormatMajor);
  format.set_minor_version(0);
  format.set_schema_sha256(bm25ArtifactFormatSignature());
  format.set_writer_build_id(kExactTopKRetrievalBuildId);
  return format;
}

pangrova::protocol::v2::CapabilityDescriptor bm25ArtifactBuildDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kBm25ArtifactBuildId);
  ref->set_semantic_version(kBm25ArtifactBuildVersion);
  ref->set_signature_sha256(bm25ArtifactBuildSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_BACKGROUND_SINK);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_TEXT);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "row_position", "BIGINT");
  setArgumentType(descriptor.add_arguments(), "text", "VARCHAR");
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("CREATE INDEX");
  auto* limits = descriptor.mutable_limits();
  limits->set_max_batch_items(kBm25ArtifactMaxRows);
  limits->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  limits->set_max_output_bytes(2ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = bm25ArtifactFormat();
  return descriptor;
}

std::string bm25RetrievalSignature() {
  return std::string(kBm25RetrievalSignature, 32);
}

bool isBm25Retrieval(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return capability.capability_id() == kBm25RetrievalId &&
      capability.semantic_version() == kBm25RetrievalVersion &&
      capability.signature_sha256() == bm25RetrievalSignature() &&
      (capability.implementation_build_id().empty() ||
       capability.implementation_build_id() == kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor bm25RetrievalDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kBm25RetrievalId);
  ref->set_semantic_version(kBm25RetrievalVersion);
  ref->set_signature_sha256(bm25RetrievalSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_RETRIEVAL_SOURCE);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_TEXT);
  descriptor.add_modalities(MODALITY_TABULAR);
  auto* score = descriptor.add_arguments();
  score->set_name("score");
  setScoreArgumentType(score);
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.set_options_schema_sha256(kBm25RetrievalSignature, 32);
  auto* limits = descriptor.mutable_limits();
  limits->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  limits->set_max_output_bytes(256ULL * 1024 * 1024);
  limits->set_max_candidate_rows(kExactTopKMaxCandidateRows);
  limits->set_max_candidate_bytes(256ULL * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = bm25ArtifactFormat();
  return descriptor;
}

std::string generatedValuesArtifactBuildSignature() {
  return std::string(kGeneratedValuesBuildSignature, 32);
}

std::string generatedValuesArtifactFormatSignature() {
  return std::string(kGeneratedValuesFormatSignature, 32);
}

bool isGeneratedValuesArtifactBuild(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kGeneratedValuesArtifactBuildId,
      kGeneratedValuesArtifactBuildVersion,
      generatedValuesArtifactBuildSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef generatedValuesArtifactFormat() {
  return annArtifactFormat(
      kGeneratedValuesArtifactFormatId, kGeneratedValuesArtifactFormatMajor,
      generatedValuesArtifactFormatSignature());
}

pangrova::protocol::v2::CapabilityDescriptor
generatedValuesArtifactBuildDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kGeneratedValuesArtifactBuildId);
  ref->set_semantic_version(kGeneratedValuesArtifactBuildVersion);
  ref->set_signature_sha256(generatedValuesArtifactBuildSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_BACKGROUND_SINK);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "row_position", "BIGINT");
  auto* value = descriptor.add_arguments();
  value->set_name("generated_value");
  value->mutable_type()->set_canonical_sql("VECTOR<FLOAT>");
  value->mutable_type()
      ->mutable_substrait_type()
      ->mutable_list()
      ->mutable_type()
      ->mutable_fp32();
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.mutable_limits()->set_max_batch_items(
      kGeneratedValuesArtifactMaxRows);
  descriptor.mutable_limits()->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  descriptor.mutable_limits()->set_max_output_bytes(2ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = generatedValuesArtifactFormat();
  return descriptor;
}

std::string generatedValuesOverlaySignature() {
  return std::string(kGeneratedValuesOverlaySignature, 32);
}

bool isGeneratedValuesOverlay(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kGeneratedValuesOverlayId, kGeneratedValuesOverlayVersion,
      generatedValuesOverlaySignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::CapabilityDescriptor generatedValuesOverlayDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kGeneratedValuesOverlayId);
  ref->set_semantic_version(kGeneratedValuesOverlayVersion);
  ref->set_signature_sha256(generatedValuesOverlaySignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_MATERIALIZER);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_execution_modes(EXECUTION_MODE_STREAMING);
  descriptor.add_modalities(MODALITY_VECTOR);
  descriptor.add_modalities(MODALITY_TABULAR);
  setArgumentType(descriptor.add_arguments(), "source_object_id", "VARCHAR");
  setArgumentType(descriptor.add_arguments(), "row_position", "BIGINT");
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.mutable_limits()->set_max_input_bytes(2ULL * 1024 * 1024 * 1024);
  descriptor.mutable_limits()->set_max_output_bytes(2ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = generatedValuesArtifactFormat();
  return descriptor;
}

std::string tabularExportSignature() {
  return std::string(kTabularExportSignature, 32);
}

std::string tabularExportFormatSignature() {
  return std::string(kTabularExportFormatSignature, 32);
}

bool isTabularExport(
    const pangrova::protocol::v2::CapabilityRef& capability) {
  return matchesCapability(
      capability, kTabularExportId, kTabularExportVersion,
      tabularExportSignature(), kExactTopKRetrievalBuildId);
}

pangrova::protocol::v2::ArtifactFormatRef tabularExportFormat() {
  return annArtifactFormat(
      kTabularExportFormatId, kTabularExportFormatMajor,
      tabularExportFormatSignature());
}

pangrova::protocol::v2::CapabilityDescriptor tabularExportDescriptor() {
  using namespace pangrova::protocol::v2;
  CapabilityDescriptor descriptor;
  auto* ref = descriptor.mutable_ref();
  ref->set_capability_id(kTabularExportId);
  ref->set_semantic_version(kTabularExportVersion);
  ref->set_signature_sha256(tabularExportSignature());
  ref->set_implementation_build_id(kExactTopKRetrievalBuildId);
  descriptor.set_kind(CAPABILITY_KIND_BACKGROUND_SINK);
  descriptor.set_placement(EXECUTION_PLACEMENT_NATIVE);
  descriptor.add_execution_modes(EXECUTION_MODE_BATCH);
  descriptor.add_execution_modes(EXECUTION_MODE_STREAMING);
  descriptor.add_modalities(MODALITY_TABULAR);
  descriptor.set_deterministic(true);
  descriptor.set_retry_class(RETRY_CLASS_IDEMPOTENT);
  descriptor.set_required_privilege("SELECT");
  descriptor.mutable_limits()->set_max_batch_items(kTabularExportMaxRows);
  descriptor.mutable_limits()->set_max_input_bytes(8ULL * 1024 * 1024 * 1024);
  descriptor.mutable_limits()->set_max_output_bytes(8ULL * 1024 * 1024 * 1024);
  *descriptor.add_supported_artifact_formats() = tabularExportFormat();
  return descriptor;
}

}  // namespace pangrova::capability
