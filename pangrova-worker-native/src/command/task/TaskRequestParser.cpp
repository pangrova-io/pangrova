// Deserializes the canonical v2 TaskRequest into Native executor configuration.
#include "command/task/TaskRequestParser.h"
#include "engine/resource/WorkerStorageBinding.h"
#include "command/task/PhysicalWriteAdmission.h"
#include "command/task/WorkerLocalCopyInput.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <chrono>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "capabilities/capability/LogicalTypeContract.h"
#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "engine/shuffle/ShuffleTransferLimits.h"
#include "engine/split/PaimonLocalFileUri.h"
#include "engine/split/SplitConfig.h"

#include "pangrova/protocol/v2/connector.pb.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "pangrova/protocol/v2/task.pb.h"

namespace pangrova::jni {

namespace {

namespace proto = pangrova::protocol::v2;
using executor::ResourceLimitConfig;
using split::ColumnMappingConfig;
using split::DeleteFileConfig;
using split::IcebergConfig;
using split::PaimonConfig;
using split::SplitConfig;
using split::TypePromotionConfig;

constexpr uint32_t kProtocolMajor = 2;
constexpr uint32_t kProtocolMinor = 0;
constexpr const char* kRolloutEpoch = "ai-native-function-resource-r3";

int64_t checkedInt64(uint64_t value, const char* field) {
  if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    throw std::runtime_error(std::string(field) + " exceeds int64 range");
  }
  return static_cast<int64_t>(value);
}

std::vector<split::IcebergManifestPartitionSpec> parseManifestPartitionSpecs(
    const proto::SystemTableSplitDetail& system) {
  if (system.iceberg_partition_specs_size() > 1024) {
    throw std::runtime_error("Iceberg partition specs exceed their bound");
  }
  std::vector<split::IcebergManifestPartitionSpec> result;
  std::unordered_set<int32_t> specIds;
  for (const auto& spec : system.iceberg_partition_specs()) {
    if (spec.spec_id() < 0 || !specIds.insert(spec.spec_id()).second ||
        spec.fields_size() > 1024) {
      throw std::runtime_error("Invalid Iceberg manifest partition spec binding");
    }
    split::IcebergManifestPartitionSpec parsed;
    parsed.spec_id = spec.spec_id();
    std::unordered_set<int32_t> fieldIds;
    std::unordered_set<std::string> names;
    for (const auto& field : spec.fields()) {
      if (field.transform().empty() || field.result_type().empty() ||
          field.transform().size() > 128 || field.result_type().size() > 128 ||
          field.field_id() <= 0 || !fieldIds.insert(field.field_id()).second ||
          field.name().size() > 1024 ||
          (!field.name().empty() && !names.insert(field.name()).second)) {
        throw std::runtime_error("Invalid Iceberg manifest partition field binding");
      }
      parsed.fields.push_back({field.transform(), field.result_type(),
          field.field_id(), field.name()});
    }
    result.push_back(std::move(parsed));
  }
  return result;
}

void collectDescriptorFiles(
    const google::protobuf::FileDescriptor* descriptor,
    std::map<std::string, const google::protobuf::FileDescriptor*>& files) {
  if (!files.emplace(descriptor->name(), descriptor).second) {
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

std::string sha256(const void* data, size_t size) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size = 0;
  if (EVP_Digest(data, size, digest.data(), &digest_size,
                 EVP_sha256(), nullptr) != 1 || digest_size != 32) {
    throw std::runtime_error("Failed to compute SHA-256");
  }
  return std::string(
      reinterpret_cast<const char*>(digest.data()), digest_size);
}

void appendLengthPrefixed(std::string& output, const std::string& value);

std::string hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (size_t i = 0; i < bytes.size(); ++i) {
    const auto value = static_cast<unsigned char>(bytes[i]);
    result[i * 2] = digits[value >> 4U];
    result[i * 2 + 1] = digits[value & 0x0fU];
  }
  return result;
}

std::pair<std::string, std::string> dependencyFingerprints(
    const proto::TaskRequest& request) {
  std::set<std::string> scopes;
  std::set<std::string> versions;
  for (const auto& split : request.splits()) {
    scopes.emplace("source\0" + split.source_id());
    versions.emplace(
        "source\0" + split.source_id() + "\0" + split.source_snapshot());
  }
  for (const auto& binding : request.operator_bindings()) {
    for (const auto& artifact : binding.artifacts()) {
      scopes.emplace(
          "artifact\0" + artifact.artifact_type() + "\0" +
          artifact.artifact_id());
      versions.emplace(
          "artifact\0" + artifact.artifact_type() + "\0" +
          artifact.artifact_id() + "\0" + artifact.generation() + "\0" +
          artifact.source_id() + "\0" + artifact.source_snapshot());
    }
  }
  std::string scope_input;
  std::string version_input;
  for (const auto& scope : scopes) appendLengthPrefixed(scope_input, scope);
  for (const auto& version : versions) appendLengthPrefixed(version_input, version);
  return {
      hex(sha256(scope_input.data(), scope_input.size())),
      hex(sha256(version_input.data(), version_input.size()))};
}

bool constantTimeEqual(const std::string& left, const std::string& right) {
  return left.size() == right.size() &&
      CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

bool isArtifactTask(proto::TaskKind task_kind) {
  return task_kind == proto::TASK_KIND_BACKGROUND_BUILD ||
      task_kind == proto::TASK_KIND_BACKGROUND_EXPORT;
}

proto::ArtifactExecutionKind artifactExecutionKind(proto::TaskKind task_kind) {
  if (task_kind == proto::TASK_KIND_BACKGROUND_BUILD) {
    return proto::ARTIFACT_EXECUTION_KIND_BUILD;
  }
  if (task_kind == proto::TASK_KIND_BACKGROUND_EXPORT) {
    return proto::ARTIFACT_EXECUTION_KIND_EXPORT;
  }
  return proto::ARTIFACT_EXECUTION_KIND_UNSPECIFIED;
}

void appendLengthPrefixed(std::string& output, const std::string& value) {
  if (value.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("Artifact dependency field exceeds uint32 range");
  }
  const auto size = static_cast<uint32_t>(value.size());
  output.push_back(static_cast<char>((size >> 24) & 0xffU));
  output.push_back(static_cast<char>((size >> 16) & 0xffU));
  output.push_back(static_cast<char>((size >> 8) & 0xffU));
  output.push_back(static_cast<char>(size & 0xffU));
  output.append(value);
}

uint32_t nextUtf8CodePoint(std::string_view text, size_t& offset) {
  const auto first = static_cast<unsigned char>(text[offset++]);
  if (first < 0x80) {
    return first;
  }
  int continuation_count = 0;
  uint32_t result = 0;
  uint32_t minimum = 0;
  if ((first & 0xE0) == 0xC0) {
    continuation_count = 1;
    result = first & 0x1F;
    minimum = 0x80;
  } else if ((first & 0xF0) == 0xE0) {
    continuation_count = 2;
    result = first & 0x0F;
    minimum = 0x800;
  } else if ((first & 0xF8) == 0xF0) {
    continuation_count = 3;
    result = first & 0x07;
    minimum = 0x10000;
  } else {
    throw std::runtime_error("Artifact dependency contains invalid UTF-8");
  }
  for (int index = 0; index < continuation_count; ++index) {
    if (offset >= text.size()) {
      throw std::runtime_error("Artifact dependency contains truncated UTF-8");
    }
    const auto byte = static_cast<unsigned char>(text[offset++]);
    if ((byte & 0xC0) != 0x80) {
      throw std::runtime_error("Artifact dependency contains invalid UTF-8");
    }
    result = (result << 6) | (byte & 0x3F);
  }
  if (result < minimum || result > 0x10FFFF ||
      (result >= 0xD800 && result <= 0xDFFF)) {
    throw std::runtime_error("Artifact dependency contains non-canonical UTF-8");
  }
  return result;
}

bool isJavaWhitespace(uint32_t code_point) {
  return (code_point >= 0x0009 && code_point <= 0x000D) ||
      (code_point >= 0x001C && code_point <= 0x0020) ||
      code_point == 0x1680 ||
      (code_point >= 0x2000 && code_point <= 0x2006) ||
      (code_point >= 0x2008 && code_point <= 0x200A) ||
      code_point == 0x2028 || code_point == 0x2029 ||
      code_point == 0x205F || code_point == 0x3000;
}

void validateArtifactDependencyField(
    const std::string& value, const char* field) {
  if (value.empty()) {
    throw std::runtime_error(
        std::string("Artifact dependency ") + field + " is empty");
  }
  bool has_non_whitespace = false;
  size_t offset = 0;
  while (offset < value.size()) {
    const uint32_t code_point = nextUtf8CodePoint(value, offset);
    if (code_point <= 0x1F ||
        (code_point >= 0x7F && code_point <= 0x9F)) {
      throw std::runtime_error(
          std::string("Artifact dependency ") + field +
          " contains a Unicode control character");
    }
    has_non_whitespace = has_non_whitespace || !isJavaWhitespace(code_point);
  }
  if (!has_non_whitespace) {
    throw std::runtime_error(
        std::string("Artifact dependency ") + field + " is blank");
  }
}

int compareUnsignedUtf8(std::string_view left, std::string_view right) {
  const size_t common_size = std::min(left.size(), right.size());
  for (size_t index = 0; index < common_size; ++index) {
    const auto left_byte = static_cast<unsigned char>(left[index]);
    const auto right_byte = static_cast<unsigned char>(right[index]);
    if (left_byte < right_byte) {
      return -1;
    }
    if (left_byte > right_byte) {
      return 1;
    }
  }
  return left.size() < right.size() ? -1 : left.size() > right.size() ? 1 : 0;
}

int compareArtifactDependencies(
    const proto::ArtifactDependency& left,
    const std::string& right_kind,
    const std::string& right_id,
    const std::string& right_version) {
  int comparison = compareUnsignedUtf8(left.kind(), right_kind);
  if (comparison == 0) {
    comparison = compareUnsignedUtf8(left.id(), right_id);
  }
  if (comparison == 0) {
    comparison = compareUnsignedUtf8(left.version(), right_version);
  }
  return comparison;
}

template <typename Artifact>
void validateArtifactProvenance(const Artifact& artifact) {
  const bool known_execution_kind =
      artifact.execution_kind() == proto::ARTIFACT_EXECUTION_KIND_BUILD ||
      artifact.execution_kind() == proto::ARTIFACT_EXECUTION_KIND_EXPORT;
  const bool known_empty_policy =
      artifact.empty_partition_policy() ==
          proto::ARTIFACT_EMPTY_PARTITION_POLICY_FORBID ||
      artifact.empty_partition_policy() ==
          proto::ARTIFACT_EMPTY_PARTITION_POLICY_ALLOW_ZERO_ROWS;
  if (!known_execution_kind || !known_empty_policy ||
      artifact.dependencies().empty() ||
      !artifact.has_dependency_set_checksum() ||
      artifact.dependency_set_checksum().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256 ||
      artifact.dependency_set_checksum().digest().size() != 32) {
    throw std::runtime_error(
        "Artifact binding has incomplete execution provenance");
  }
  std::string canonical;
  std::string previous_kind;
  std::string previous_id;
  std::string previous_version;
  bool first = true;
  for (const auto& dependency : artifact.dependencies()) {
    validateArtifactDependencyField(dependency.kind(), "kind");
    validateArtifactDependencyField(dependency.id(), "id");
    validateArtifactDependencyField(dependency.version(), "version");
    if (!first) {
      const bool duplicate_identity = dependency.kind() == previous_kind &&
          dependency.id() == previous_id;
      if (duplicate_identity || compareArtifactDependencies(
              dependency, previous_kind, previous_id, previous_version) <= 0) {
        throw std::runtime_error(
            "Artifact dependencies are not canonically ordered and unique");
      }
    }
    first = false;
    previous_kind = dependency.kind();
    previous_id = dependency.id();
    previous_version = dependency.version();
    appendLengthPrefixed(canonical, dependency.kind());
    appendLengthPrefixed(canonical, dependency.id());
    appendLengthPrefixed(canonical, dependency.version());
  }
  const auto actual = sha256(canonical.data(), canonical.size());
  if (!constantTimeEqual(
          artifact.dependency_set_checksum().digest(), actual)) {
    throw std::runtime_error("Artifact dependency-set SHA-256 mismatch");
  }
}

void validateProtocol(const proto::ProtocolVersion& version) {
  if (version.GetReflection()->GetUnknownFields(version).field_count() != 0) {
    throw std::runtime_error("TaskRequest protocol contains unknown fields");
  }
  if (version.major() != kProtocolMajor || version.minor() != kProtocolMinor) {
    throw std::runtime_error(
        "Unsupported TaskRequest protocol " + std::to_string(version.major()) +
        "." + std::to_string(version.minor()));
  }
  if (version.rollout_epoch() != kRolloutEpoch) {
    throw std::runtime_error("TaskRequest rollout epoch mismatch");
  }
  const auto descriptor_set = canonicalDescriptorSet();
  const auto expected = sha256(descriptor_set.data(), descriptor_set.size());
  if (!constantTimeEqual(version.descriptor_set_sha256(), expected)) {
    throw std::runtime_error("TaskRequest canonical descriptor hash mismatch");
  }
}

void validateCallbackAuthority(const proto::TaskRequest& request) {
  const auto& attempt = request.attempt();
  const auto& owner = attempt.execution_owner();
  auto blank = [](const std::string& value) {
    return value.empty() || std::all_of(value.begin(), value.end(),
        [](unsigned char character) { return std::isspace(character); });
  };
  if (blank(attempt.query_or_job_id()) || blank(attempt.task_id()) || attempt.plan_epoch() == 0 ||
      attempt.fencing_token() == 0 || attempt.control_epoch() == 0 || !attempt.has_execution_owner() ||
      blank(owner.owner_id()) || blank(owner.owner_incarnation()) || owner.fencing_token() == 0 ||
      attempt.GetReflection()->GetUnknownFields(attempt).field_count() != 0 ||
      owner.GetReflection()->GetUnknownFields(owner).field_count() != 0) {
    throw std::runtime_error("TaskRequest attempt requires task, control and execution-owner identities");
  }
  checkedInt64(attempt.plan_epoch(), "plan_epoch");
  checkedInt64(attempt.fencing_token(), "fencing_token");
  checkedInt64(attempt.control_epoch(), "control_epoch");
  checkedInt64(owner.fencing_token(), "execution_owner.fencing_token");
  if (attempt.stage_id() < 0 || attempt.partition_id() < 0 ||
      attempt.attempt_id() > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    throw std::runtime_error("TaskRequest attempt ordinal exceeds Java accounting domain");
  }
  const auto& security = request.security();
  if (!request.has_security() || blank(security.tenant_id()) || blank(security.principal()) ||
      security.GetReflection()->GetUnknownFields(security).field_count() != 0 ||
      request.trace_context().GetReflection()->GetUnknownFields(request.trace_context()).field_count() != 0) {
    throw std::runtime_error("TaskRequest security or trace authority is incomplete or unknown");
  }
  if (request.deadline_epoch_ms() == 0) throw std::runtime_error("TaskRequest requires a positive deadline");
  checkedInt64(request.deadline_epoch_ms(), "deadline_epoch_ms");
}

void validatePlanChecksum(const proto::TaskRequest& request) {
  if (request.substrait_plan().empty()) {
    throw std::runtime_error("TaskRequest requires a non-empty Substrait plan");
  }
  if (!request.has_substrait_plan_checksum() ||
      request.substrait_plan_checksum().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256) {
    throw std::runtime_error("TaskRequest requires a SHA-256 plan checksum");
  }
  const auto actual = sha256(
      request.substrait_plan().data(), request.substrait_plan().size());
  if (!constantTimeEqual(
          request.substrait_plan_checksum().digest(), actual)) {
    throw std::runtime_error("TaskRequest Substrait plan checksum mismatch");
  }
}

split::FileFormatType parseFileFormat(proto::FileFormat format) {
  switch (format) {
    case proto::FILE_FORMAT_PARQUET:
      return split::FileFormatType::PARQUET;
    case proto::FILE_FORMAT_ORC:
      return split::FileFormatType::ORC;
    case proto::FILE_FORMAT_LANCE:
      return split::FileFormatType::LANCE;
    case proto::FILE_FORMAT_METADATA_JSON:
      return split::FileFormatType::METADATA_JSON;
    case proto::FILE_FORMAT_METADATA_DIRECTORY:
      return split::FileFormatType::METADATA_DIRECTORY;
    case proto::FILE_FORMAT_METADATA_AVRO:
      return split::FileFormatType::METADATA_AVRO;
    case proto::FILE_FORMAT_UNSPECIFIED:
    default:
      throw std::runtime_error("FileSplitDetail requires a supported file format");
  }
}

std::string deleteTypeToString(proto::DeleteFileType type) {
  switch (type) {
    case proto::DELETE_FILE_TYPE_POSITION:
      return "position";
    case proto::DELETE_FILE_TYPE_EQUALITY:
      return "equality";
    case proto::DELETE_FILE_TYPE_DELETION_VECTOR:
      return "deletion_vector";
    case proto::DELETE_FILE_TYPE_UNSPECIFIED:
    default:
      throw std::runtime_error("Iceberg delete file type is unspecified");
  }
}

std::string icebergReadModeToString(proto::IcebergReadMode mode) {
  switch (mode) {
    case proto::ICEBERG_READ_MODE_SNAPSHOT:
      return "FULL";
    case proto::ICEBERG_READ_MODE_TIME_TRAVEL:
      return "TIME_TRAVEL";
    case proto::ICEBERG_READ_MODE_INCREMENTAL:
      return "INCREMENTAL";
    case proto::ICEBERG_READ_MODE_UNSPECIFIED:
    default:
      throw std::runtime_error("Iceberg read mode is unspecified");
  }
}

std::string icebergContentToString(int32_t content) {
  switch (content) {
    case 0:
      return "DATA";
    case 1:
      return "POSITION_DELETES";
    case 2:
      return "EQUALITY_DELETES";
    default:
      throw std::runtime_error("Unsupported Iceberg content type");
  }
}

std::string paimonReadModeToString(proto::PaimonReadMode mode) {
  switch (mode) {
    case proto::PAIMON_READ_MODE_APPEND:
      return "APPEND";
    case proto::PAIMON_READ_MODE_PRIMARY_KEY:
      return "PRIMARY_KEY";
    case proto::PAIMON_READ_MODE_CHANGELOG:
      return "CHANGELOG";
    case proto::PAIMON_READ_MODE_INCREMENTAL:
      return "INCREMENTAL";
    case proto::PAIMON_READ_MODE_UNSPECIFIED:
    default:
      throw std::runtime_error("Paimon read mode is unspecified");
  }
}

std::string changelogProducerToString(proto::PaimonChangelogProducer producer) {
  switch (producer) {
    case proto::PAIMON_CHANGELOG_PRODUCER_NONE:
      return "NONE";
    case proto::PAIMON_CHANGELOG_PRODUCER_INPUT:
      return "INPUT";
    case proto::PAIMON_CHANGELOG_PRODUCER_LOOKUP:
      return "LOOKUP";
    case proto::PAIMON_CHANGELOG_PRODUCER_FULL_COMPACTION:
      return "FULL_COMPACTION";
    case proto::PAIMON_CHANGELOG_PRODUCER_UNSPECIFIED:
    default:
      throw std::runtime_error("Paimon changelog producer is unspecified");
  }
}

std::string mergeEngineToString(proto::PaimonMergeEngine engine) {
  switch (engine) {
    case proto::PAIMON_MERGE_ENGINE_DEDUPLICATE:
      return "deduplicate";
    case proto::PAIMON_MERGE_ENGINE_PARTIAL_UPDATE:
      return "partial_update";
    case proto::PAIMON_MERGE_ENGINE_AGGREGATION:
      return "aggregation";
    case proto::PAIMON_MERGE_ENGINE_FIRST_ROW:
      return "first_row";
    case proto::PAIMON_MERGE_ENGINE_UNSPECIFIED:
    default:
      throw std::runtime_error("Paimon merge engine is unspecified");
  }
}

IcebergConfig parseIceberg(const proto::IcebergSplitDetail& detail) {
  IcebergConfig config;
  config.snapshot_id = detail.snapshot_id();
  config.data_sequence_number = detail.data_sequence_number();
  config.spec_id = detail.spec_id();
  config.content = icebergContentToString(detail.content());
  config.read_mode = icebergReadModeToString(detail.read_mode());
  config.from_snapshot_id = detail.from_snapshot_id();
  config.to_snapshot_id = detail.to_snapshot_id();
  config.delete_files.reserve(detail.delete_files_size());
  for (const auto& file : detail.delete_files()) {
    DeleteFileConfig parsed;
    parsed.file_path = file.opaque_location();
    parsed.delete_type = deleteTypeToString(file.delete_type());
    parsed.equality_fields.assign(
        file.equality_fields().begin(), file.equality_fields().end());
    parsed.record_count = file.record_count();
    parsed.data_sequence_number = file.data_sequence_number();
    parsed.dv_file_path = file.deletion_vector_location();
    config.delete_files.push_back(std::move(parsed));
  }
  return config;
}

void parseSchemaEvolution(const proto::SchemaEvolution& evolution, SplitConfig& config);

PaimonConfig parsePaimon(const proto::PaimonSplitDetail& detail) {
  if (!detail.has_merge_policy()) throw std::invalid_argument("Paimon split requires exact SDK merge policy");
  PaimonConfig config;
  config.merge_engine = mergeEngineToString(detail.merge_engine());
  config.changelog_mode = detail.changelog_mode() ||
      detail.read_mode() == proto::PAIMON_READ_MODE_CHANGELOG;
  config.primary_keys.assign(
      detail.primary_keys().begin(), detail.primary_keys().end());
  config.bucket_id = detail.bucket_id();
  config.snapshot_id = detail.snapshot_id();
  config.from_snapshot_id = detail.from_snapshot_id();
  config.to_snapshot_id = detail.to_snapshot_id();
  config.read_mode = paimonReadModeToString(detail.read_mode());
  config.changelog_producer =
      changelogProducerToString(detail.changelog_producer());
  config.deletion_vectors_enabled = detail.deletion_vectors_enabled();
  config.needs_merge = detail.needs_merge();
  config.ignore_delete = detail.merge_policy().ignore_delete();
  config.remove_record_on_delete = detail.merge_policy().remove_record_on_delete();
  config.sequence_column = detail.sequence_column();
  config.aggregation_specs.insert(
      detail.aggregation_specs().begin(), detail.aggregation_specs().end());
  config.lsm_levels.reserve(detail.lsm_levels_size());
  for (const auto& level : detail.lsm_levels()) {
    split::LsmLevelConfig parsed;
    parsed.level = level.level();
    for (const auto& file : level.files()) {
      if (file.opaque_location().empty() || file.file_size() == 0 || !file.has_scan()
          || !file.scan().has_schema_evolution()
          || file.scan().row_start() > file.scan().row_end()
          || file.scan().row_end() > file.scan().physical_rows()
          || file.scan().physical_rows() > std::numeric_limits<int64_t>::max()
          || file.min_sequence_number() > file.max_sequence_number()) {
        throw std::invalid_argument("Paimon LSM file has invalid exact physical metadata");
      }
      if (file.scan().has_deletion_vector()) {
        const auto& vector = file.scan().deletion_vector();
        if (vector.opaque_location().empty() || vector.physical_rows() != file.scan().physical_rows()
            || vector.byte_length() < 4 || vector.byte_length() > 64 * 1024 * 1024
            || vector.byte_offset() > UINT64_MAX - vector.byte_length()
            || (vector.has_cardinality() && vector.cardinality() > vector.physical_rows())) {
          throw std::invalid_argument("Paimon deletion vector has invalid exact file range metadata");
        }
      }
      split::PhysicalFileConfig physical;
      physical.file_path = file.opaque_location();
      physical.file_format = parseFileFormat(file.file_format());
      physical.file_size = file.file_size();
      physical.min_sequence_number = file.min_sequence_number();
      physical.max_sequence_number = file.max_sequence_number();
      SplitConfig evolution;
      parseSchemaEvolution(file.scan().schema_evolution(), evolution);
      physical.column_mappings = std::move(evolution.column_mappings);
      physical.type_promotions = std::move(evolution.type_promotions);
      proto::FileSplitDetail read;
      read.set_file_format(file.file_format());
      *read.mutable_physical_file_scan() = file.scan();
      physical.file_scan_detail = read.SerializeAsString();
      parsed.files.push_back(std::move(physical));
    }
    parsed.total_record_count = level.total_record_count();
    config.lsm_levels.push_back(std::move(parsed));
  }
  return config;
}

void parseSchemaEvolution(
    const proto::SchemaEvolution& evolution, SplitConfig& config) {
  config.column_mappings.reserve(evolution.column_mappings_size());
  for (const auto& mapping : evolution.column_mappings()) {
    ColumnMappingConfig parsed;
    parsed.source_id = mapping.source_field_id();
    parsed.target_id = mapping.target_field_id();
    parsed.source_name = mapping.source_name();
    parsed.target_name = mapping.target_name();
    parsed.default_value = mapping.default_value();
    config.column_mappings.push_back(std::move(parsed));
  }
  config.type_promotions.reserve(evolution.type_promotions_size());
  for (const auto& promotion : evolution.type_promotions()) {
    TypePromotionConfig parsed;
    parsed.field_id = promotion.field_id();
    parsed.source_type = promotion.source_type();
    parsed.target_type = promotion.target_type();
    parsed.column_name = promotion.column_name();
    config.type_promotions.push_back(std::move(parsed));
  }
}

SplitConfig parseSplit(const proto::DataSplit& split, const proto::TaskRequest& request) {
  if (split.split_id().empty() || split.connector_id().empty() ||
      split.source_id().empty() || split.source_snapshot().empty() ||
      split.data_object_id().empty() ||
      split.opaque_location().empty()) {
    throw std::runtime_error(
        "DataSplit requires split, connector, source, snapshot, object and location identities");
  }
  proto::FileSplitDetail detail;
  if (!split.has_connector_detail() ||
      !split.connector_detail().UnpackTo(&detail)) {
    throw std::runtime_error(
        "DataSplit connector_detail must be v2 FileSplitDetail");
  }

  SplitConfig config;
  config.split_id = split.split_id();
  config.connector_id = split.connector_id();
  config.table_name = split.source_id();
  config.source_snapshot = split.source_snapshot();
  config.data_object_id = split.data_object_id();
  config.file_path = split.opaque_location();
  config.file_format = parseFileFormat(detail.file_format());
  config.file_scan_detail = detail.SerializeAsString();
  if (detail.has_system_table()) {
    const bool directory = detail.system_table().kind() ==
        proto::SystemTableSplitDetail::PAIMON_BRANCHES;
    const bool icebergAvro = detail.system_table().kind() ==
            proto::SystemTableSplitDetail::ICEBERG_MANIFESTS ||
        detail.system_table().kind() == proto::SystemTableSplitDetail::ICEBERG_FILES ||
        detail.system_table().kind() == proto::SystemTableSplitDetail::ICEBERG_ENTRIES;
    const bool paimonFiles = detail.system_table().kind() ==
        proto::SystemTableSplitDetail::PAIMON_FILES;
    const bool avro = icebergAvro || paimonFiles;
    const bool files = detail.system_table().kind() ==
            proto::SystemTableSplitDetail::ICEBERG_FILES ||
        detail.system_table().kind() == proto::SystemTableSplitDetail::ICEBERG_ENTRIES;
    if ((directory && (config.file_format != split::FileFormatType::METADATA_DIRECTORY ||
            split.start_offset() != 0 || split.length() != 0)) ||
        (!directory && (config.file_format != (avro
                ? split::FileFormatType::METADATA_AVRO : split::FileFormatType::METADATA_JSON) ||
            split.start_offset() != 0 || split.length() == 0))) {
      throw std::runtime_error(
          "System table split requires a pinned source matching its read unit");
    }
    if (!directory && (!detail.system_table().branch_name().empty() ||
        detail.system_table().directory_mtime_ms() != 0 ||
        detail.system_table().directory_device() != 0 ||
        detail.system_table().directory_inode() != 0 ||
        detail.system_table().directory_zone_offset_seconds() != 0)) {
      throw std::runtime_error("Non-branch system table has directory identity");
    }
    if (!icebergAvro && (detail.system_table().iceberg_partition_specs_size() != 0 ||
        detail.system_table().iceberg_format_version() != 0 ||
        detail.system_table().iceberg_snapshot_sequence_number() != 0)) {
      throw std::runtime_error("Non-Iceberg manifests split has manifest identity");
    }
    if (!files && (!detail.system_table().iceberg_manifest_list_uri().empty() ||
        detail.system_table().iceberg_manifest_list_length() != 0 ||
        !detail.system_table().iceberg_manifest_list_sha256().empty() ||
        detail.system_table().iceberg_child_spec_id() != 0 ||
        detail.system_table().iceberg_child_content() != 0)) {
      throw std::runtime_error("Non-Iceberg files split has child manifest identity");
    }
    if (!paimonFiles && detail.system_table().has_paimon_files()) {
      throw std::runtime_error("Non-Paimon files split has Paimon manifest context");
    }
    switch (detail.system_table().kind()) {
      case proto::SystemTableSplitDetail::ICEBERG_SNAPSHOTS:
        if (split.connector_id() != "iceberg" ||
            split.source_snapshot() != split.opaque_location() ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            !split.source_id().ends_with("$snapshots")) {
          throw std::runtime_error("Iceberg snapshots split source_id kind mismatch");
        }
        config.system_table = split::SystemTableKind::ICEBERG_SNAPSHOTS;
        break;
      case proto::SystemTableSplitDetail::ICEBERG_REFS:
        if (split.connector_id() != "iceberg" ||
            split.source_snapshot() != split.opaque_location() ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            !split.source_id().ends_with("$refs")) {
          throw std::runtime_error("Iceberg refs split source_id kind mismatch");
        }
        config.system_table = split::SystemTableKind::ICEBERG_REFS;
        break;
      case proto::SystemTableSplitDetail::ICEBERG_HISTORY:
        if (split.connector_id() != "iceberg" ||
            split.source_snapshot() != split.opaque_location() ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            !split.source_id().ends_with("$history")) {
          throw std::runtime_error("Iceberg history split source_id kind mismatch");
        }
        config.system_table = split::SystemTableKind::ICEBERG_HISTORY;
        break;
      case proto::SystemTableSplitDetail::ICEBERG_MANIFESTS:
      {
        const auto& system = detail.system_table();
        if (split.connector_id() != "iceberg" ||
            !split.source_id().ends_with("$manifests") ||
            split.source_snapshot() != split.opaque_location() ||
            split.data_object_id() != split.opaque_location() ||
            split.partition_values_size() != 0 ||
            system.snapshot_id() == 0 ||
            system.snapshot_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            system.iceberg_snapshot_sequence_number() >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            system.object_sha256().size() != 32 ||
            (system.iceberg_format_version() != 2 && system.iceberg_format_version() != 3) ||
            !system.tag_name().empty() || system.schema_id() != 0) {
          throw std::runtime_error("Iceberg manifests split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::ICEBERG_MANIFESTS;
        config.system_table_snapshot_id = system.snapshot_id();
        config.system_table_object_sha256 = system.object_sha256();
        config.system_table_iceberg_format_version = system.iceberg_format_version();
        config.system_table_iceberg_snapshot_sequence_number =
            system.iceberg_snapshot_sequence_number();
        config.system_table_iceberg_partition_specs = parseManifestPartitionSpecs(system);
        break;
      }
      case proto::SystemTableSplitDetail::ICEBERG_FILES:
      case proto::SystemTableSplitDetail::ICEBERG_ENTRIES:
      {
        const auto& system = detail.system_table();
        if (split.connector_id() != "iceberg" ||
            !split.source_id().ends_with(system.kind() ==
                    proto::SystemTableSplitDetail::ICEBERG_ENTRIES
                    ? "$entries" : "$files") ||
            split.source_snapshot() != system.iceberg_manifest_list_uri() ||
            split.data_object_id() != split.opaque_location() ||
            split.partition_values_size() != 0 ||
            split.source_snapshot() == split.opaque_location() ||
            system.snapshot_id() == 0 ||
            system.snapshot_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            system.iceberg_snapshot_sequence_number() >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            system.object_sha256().size() != 32 ||
            (system.iceberg_format_version() != 2 && system.iceberg_format_version() != 3) ||
            system.iceberg_manifest_list_uri().empty() ||
            system.iceberg_manifest_list_length() == 0 ||
            system.iceberg_manifest_list_sha256().size() != 32 ||
            system.iceberg_child_spec_id() < 0 ||
            (system.iceberg_child_content() != 0 && system.iceberg_child_content() != 1) ||
            system.iceberg_partition_specs_size() == 0 ||
            !system.tag_name().empty() || system.schema_id() != 0) {
          throw std::runtime_error("Iceberg files split identity or kind mismatch");
        }
        config.system_table = system.kind() ==
                proto::SystemTableSplitDetail::ICEBERG_ENTRIES
                ? split::SystemTableKind::ICEBERG_ENTRIES
                : split::SystemTableKind::ICEBERG_FILES;
        config.system_table_snapshot_id = system.snapshot_id();
        config.system_table_object_sha256 = system.object_sha256();
        config.system_table_iceberg_format_version = system.iceberg_format_version();
        config.system_table_iceberg_snapshot_sequence_number =
            system.iceberg_snapshot_sequence_number();
        config.system_table_iceberg_manifest_list_uri = system.iceberg_manifest_list_uri();
        config.system_table_iceberg_manifest_list_length =
            system.iceberg_manifest_list_length();
        config.system_table_iceberg_manifest_list_sha256 =
            system.iceberg_manifest_list_sha256();
        config.system_table_iceberg_child_spec_id = system.iceberg_child_spec_id();
        config.system_table_iceberg_child_content = system.iceberg_child_content();
        config.system_table_iceberg_partition_specs = parseManifestPartitionSpecs(system);
        if (std::none_of(config.system_table_iceberg_partition_specs.begin(),
                config.system_table_iceberg_partition_specs.end(), [&](const auto& spec) {
                  return spec.spec_id == system.iceberg_child_spec_id();
                })) {
          throw std::runtime_error("Iceberg child manifest partition spec is unbound");
        }
        break;
      }
      case proto::SystemTableSplitDetail::PAIMON_FILES:
      {
        const auto& system = detail.system_table();
        const auto& context = system.paimon_files();
        if (split.connector_id() != "paimon" || !split.source_id().ends_with("$files") ||
            split.data_object_id() != split.opaque_location() ||
            split.partition_values_size() != 0 || system.snapshot_id() == 0 ||
            system.snapshot_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
            system.object_sha256().size() != 32 || !system.tag_name().empty() || system.schema_id() != 0 ||
            !system.has_paimon_files() || context.files().empty() || context.schemas().empty() ||
            context.snapshot().uri().empty() || context.snapshot().length() == 0 ||
            context.snapshot().length() > 64 * 1024 * 1024 || context.snapshot().sha256().size() != 32 ||
            split.source_snapshot() != context.snapshot().uri() ||
            split.source_snapshot() == split.opaque_location() || context.ByteSizeLong() > 64 * 1024 * 1024) {
          throw std::runtime_error("Paimon files split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_FILES;
        config.system_table_snapshot_id = system.snapshot_id();
        config.system_table_object_sha256 = system.object_sha256();
        config.system_table_paimon_files_context = context.SerializeAsString();
        break;
      }
      case proto::SystemTableSplitDetail::PAIMON_SNAPSHOTS:
        if (split.connector_id() != "paimon" ||
            !split.source_id().ends_with("$snapshots") ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() == 0 ||
            !detail.system_table().tag_name().empty() ||
            detail.system_table().object_sha256().size() != 32) {
          throw std::runtime_error("Paimon snapshot split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_SNAPSHOTS;
        config.system_table_snapshot_id = detail.system_table().snapshot_id();
        config.system_table_object_sha256 = detail.system_table().object_sha256();
        break;
      case proto::SystemTableSplitDetail::PAIMON_TAGS:
        if (split.connector_id() != "paimon" ||
            !split.source_id().ends_with("$tags") ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            detail.system_table().tag_name().empty() ||
            !split.opaque_location().ends_with(
                "/tag-" + detail.system_table().tag_name()) ||
            detail.system_table().object_sha256().size() != 32) {
          throw std::runtime_error("Paimon tag split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_TAGS;
        config.system_table_tag_name = detail.system_table().tag_name();
        config.system_table_object_sha256 = detail.system_table().object_sha256();
        break;
      case proto::SystemTableSplitDetail::PAIMON_OPTIONS:
        if (split.connector_id() != "paimon" ||
            !split.source_id().ends_with("$options") ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            !detail.system_table().tag_name().empty() ||
            !split.opaque_location().ends_with(
                "/schema/schema-" +
                std::to_string(detail.system_table().schema_id())) ||
            detail.system_table().object_sha256().size() != 32) {
          throw std::runtime_error("Paimon options split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_OPTIONS;
        config.system_table_schema_id = detail.system_table().schema_id();
        config.system_table_object_sha256 = detail.system_table().object_sha256();
        break;
      case proto::SystemTableSplitDetail::PAIMON_SCHEMAS:
        if (split.connector_id() != "paimon" ||
            !split.source_id().ends_with("$schemas") ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            !detail.system_table().tag_name().empty() ||
            !split.opaque_location().ends_with(
                "/schema/schema-" +
                std::to_string(detail.system_table().schema_id())) ||
            detail.system_table().object_sha256().size() != 32) {
          throw std::runtime_error("Paimon schemas split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_SCHEMAS;
        config.system_table_schema_id = detail.system_table().schema_id();
        config.system_table_object_sha256 = detail.system_table().object_sha256();
        break;
      case proto::SystemTableSplitDetail::PAIMON_BRANCHES:
      {
        std::string directoryPath;
        try {
          directoryPath = split::decodePaimonLocalFileUri(split.opaque_location());
        } catch (const std::invalid_argument&) {
          throw std::runtime_error("Paimon branches split requires a local file URI");
        }
        if (split.connector_id() != "paimon" ||
            !split.source_id().ends_with("$branches") ||
            split.data_object_id() != split.opaque_location() ||
            detail.system_table().snapshot_id() != 0 ||
            detail.system_table().schema_id() != 0 ||
            !detail.system_table().tag_name().empty() ||
            !detail.system_table().object_sha256().empty() ||
            detail.system_table().branch_name().empty() ||
            detail.system_table().branch_name().find('/') != std::string::npos ||
            detail.system_table().branch_name() == "." ||
            detail.system_table().branch_name() == ".." ||
            detail.system_table().branch_name().find('\\') != std::string::npos ||
            detail.system_table().branch_name().find('\0') != std::string::npos ||
            !directoryPath.ends_with(
                "/branch/branch-" + detail.system_table().branch_name()) ||
            detail.system_table().directory_mtime_ms() < 0 ||
            detail.system_table().directory_inode() == 0 ||
            detail.system_table().directory_zone_offset_seconds() < -18 * 3600 ||
            detail.system_table().directory_zone_offset_seconds() > 18 * 3600 ||
            split.partition_values_size() != 0) {
          throw std::runtime_error("Paimon branches split identity or kind mismatch");
        }
        config.system_table = split::SystemTableKind::PAIMON_BRANCHES;
        config.system_table_branch_name = detail.system_table().branch_name();
        config.system_table_directory_mtime_ms = detail.system_table().directory_mtime_ms();
        config.system_table_directory_device = detail.system_table().directory_device();
        config.system_table_directory_inode = detail.system_table().directory_inode();
        config.system_table_directory_zone_offset_seconds =
            detail.system_table().directory_zone_offset_seconds();
        break;
      }
      default:
        throw std::runtime_error("Unsupported system table split kind");
    }
  } else if (config.file_format == split::FileFormatType::METADATA_JSON ||
      config.file_format == split::FileFormatType::METADATA_DIRECTORY ||
      config.file_format == split::FileFormatType::METADATA_AVRO) {
    throw std::runtime_error(
        "Metadata JSON split requires typed system table detail");
  }
  if (config.file_format == split::FileFormatType::LANCE) {
    if (!split.has_catalog() || split.start_offset() != 0) {
      throw std::runtime_error("Lance DataSplit requires exact catalog and explicit read unit");
    }
    if (detail.has_physical_file_scan()) {
      const auto& file = detail.physical_file_scan();
      if (split.length() < 40 || file.row_start() > file.row_end() || file.row_end() > file.physical_rows()
          || file.physical_rows() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        throw std::runtime_error("Physical Lance file has invalid bytes or physical row bounds");
      }
      config.physical_lance_file = true;
    } else if (!detail.has_scan_partition() || split.length() != 0
        || detail.scan_partition().manifest_sha256().algorithm() != proto::Checksum::ALGORITHM_SHA256
        || detail.scan_partition().manifest_sha256().digest().size() != 32
        || detail.scan_partition().physical_schema_sha256().algorithm() != proto::Checksum::ALGORITHM_SHA256
        || detail.scan_partition().physical_schema_sha256().digest().size() != 32) {
      throw std::runtime_error("Lance DataSplit requires exact catalog and manifest partition, not a byte range");
    }
    config.file_scan_detail = detail.SerializeAsString();
    uint64_t version = 0;
    const auto& snapshot = config.source_snapshot;
    const auto [end, error] = std::from_chars(
        snapshot.data(), snapshot.data() + snapshot.size(), version);
    if (error != std::errc{} ||
        end != snapshot.data() + snapshot.size() || version == 0) {
      throw std::runtime_error(
          "Lance DataSplit requires a non-zero decimal source_snapshot version");
    }
  }
  config.start_offset = checkedInt64(split.start_offset(), "start_offset");
  config.length = checkedInt64(split.length(), "length");

  for (const auto& partition : split.partition_values()) {
    if (partition.column_name().empty() ||
        !partition.value().has_string_value() ||
        !config.partition_values.emplace(
            partition.column_name(), partition.value().string_value()).second) {
      throw std::runtime_error(
          "DataSplit contains an invalid or duplicate string partition value");
    }
  }
  if (detail.has_schema_evolution() && detail.has_schema_descriptor_ref()) {
    throw std::runtime_error(
        "DataSplit must not carry both inline and referenced schema evolution");
  }
  if (detail.has_schema_evolution()) {
    parseSchemaEvolution(detail.schema_evolution(), config);
  } else if (detail.has_schema_descriptor_ref()) {
    const auto ref = detail.schema_descriptor_ref();
    if (ref >= static_cast<uint32_t>(request.schema_descriptors_size())) {
      throw std::runtime_error(
          "DataSplit schema_descriptor_ref is outside TaskRequest.schema_descriptors");
    }
    parseSchemaEvolution(request.schema_descriptors(static_cast<int>(ref)), config);
  }
  for (const auto& [column, subfields] : detail.required_subfields()) {
    if (column.empty() || subfields.paths().empty()) {
      throw std::runtime_error("FileSplitDetail required_subfields is invalid");
    }
    config.required_subfields.emplace(
        column,
        std::vector<std::string>(
            subfields.paths().begin(), subfields.paths().end()));
  }

  if (detail.has_iceberg()) {
    config.iceberg = parseIceberg(detail.iceberg());
  } else if (detail.has_paimon()) {
    config.paimon = parsePaimon(detail.paimon());
    config.paimon->partition_values = config.partition_values;
  }
  return config;
}

ResourceLimitConfig parseResourceGrant(const proto::StageResourceGrant& grant) {
  if (grant.grant_id().empty() || grant.cpu_slots() == 0 || grant.native_memory_bytes() == 0 ||
      grant.GetReflection()->GetUnknownFields(grant).field_count() != 0) {
    throw std::invalid_argument("Canonical task requires a complete resource grant");
  }
  for (int ordinal = 0; ordinal < grant.GetDescriptor()->field_count(); ++ordinal) {
    const auto* field = grant.GetDescriptor()->field(ordinal);
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
      checkedInt64(grant.GetReflection()->GetUInt64(grant, field), field->name().c_str());
    }
  }
  ResourceLimitConfig config;
  config.max_memory_bytes =
      checkedInt64(grant.native_memory_bytes(), "native_memory_bytes");
  config.max_shuffle_buffer_bytes =
      checkedInt64(grant.shuffle_buffer_bytes(), "shuffle_buffer_bytes");
  if (grant.cpu_slots() > static_cast<uint32_t>(
          std::numeric_limits<int32_t>::max())) {
    throw std::runtime_error("cpu_slots exceeds int32 range");
  }
  config.max_threads = static_cast<int32_t>(grant.cpu_slots());
  config.max_spill_bytes = checkedInt64(grant.spill_bytes(), "spill_bytes");
  config.max_candidate_rows =
      checkedInt64(grant.candidate_rows(), "candidate_rows");
  config.max_candidate_bytes =
      checkedInt64(grant.candidate_bytes(), "candidate_bytes");
  config.max_object_store_read_bytes = checkedInt64(
      grant.object_store_read_bytes(), "object_store_read_bytes");
  config.max_object_store_write_bytes = checkedInt64(
      grant.object_store_write_bytes(), "object_store_write_bytes");
  if (grant.provider_concurrency() > static_cast<uint32_t>(
          std::numeric_limits<int32_t>::max())) {
    throw std::runtime_error("provider_concurrency exceeds int32 range");
  }
  config.max_provider_concurrency =
      static_cast<int32_t>(grant.provider_concurrency());
  config.max_provider_items = checkedInt64(grant.provider_items(), "provider_items");
  config.max_provider_attempts = checkedInt64(grant.provider_attempts(), "provider_attempts");
  config.max_provider_request_bytes = checkedInt64(grant.provider_request_bytes(), "provider_request_bytes");
  config.max_provider_response_bytes = checkedInt64(grant.provider_response_bytes(), "provider_response_bytes");
  config.max_media_bytes = checkedInt64(grant.media_bytes(), "media_bytes");
  return config;
}

bool isSupportedNativeCapability(
    const proto::CapabilityRef& capability,
    proto::TaskKind task_kind) {
  if (isArtifactTask(task_kind)) {
    return pangrova::capability::isDiskAnnArtifactBuild(capability) ||
        pangrova::capability::isHnswArtifactBuild(capability) ||
        pangrova::capability::isIvfPqArtifactBuild(capability) ||
        pangrova::capability::isBm25ArtifactBuild(capability) ||
        pangrova::capability::isGeneratedValuesArtifactBuild(capability) ||
        pangrova::capability::isTabularExport(capability) ||
        pangrova::capability::isAiEmbedding(capability);
  }
  return pangrova::capability::isExactTopKRetrieval(capability) ||
      pangrova::capability::isDiskAnnRetrieval(capability) ||
      pangrova::capability::isFilteredDiskAnnRetrieval(capability) ||
      pangrova::capability::isHnswRetrieval(capability) ||
      pangrova::capability::isIvfPqRetrieval(capability) ||
      pangrova::capability::isBm25Retrieval(capability) ||
      pangrova::capability::isRrfRetrievalMerge(capability) ||
      pangrova::capability::isLinearRetrievalMerge(capability) ||
      pangrova::capability::isCrossEncoderRerank(capability) ||
      pangrova::capability::isAiTextGeneration(capability) ||
      pangrova::capability::isAiMultimodalGeneration(capability) ||
      pangrova::capability::isAiEmbedding(capability) ||
      pangrova::capability::isGeneratedValuesOverlay(capability);
}

void validateArtifact(
    const proto::ArtifactBinding& artifact,
    const proto::TaskRequest& request) {
  validateArtifactProvenance(artifact);
  if (artifact.artifact_id().empty() || artifact.artifact_type().empty() ||
      artifact.generation().empty() || artifact.source_id().empty() ||
      artifact.source_snapshot().empty() ||
      artifact.artifact_created_at_epoch_ms() == 0 ||
      artifact.manifest_uri().empty() ||
      !artifact.has_manifest_checksum() ||
      artifact.manifest_checksum().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256 ||
      artifact.manifest_checksum().digest().size() != 32 ||
      !artifact.has_format() || artifact.format().format_id().empty() ||
      artifact.format().major_version() == 0 ||
      artifact.format().schema_sha256().size() != 32 ||
      artifact.format().writer_build_id().empty() ||
      !artifact.has_binding_checksum() ||
      artifact.binding_checksum().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256 ||
      artifact.binding_checksum().digest().size() != 32 ||
      artifact.split_objects().empty()) {
    throw std::runtime_error(
        "OperatorBinding contains an incomplete artifact identity");
  }
  auto unsigned_artifact = artifact;
  unsigned_artifact.clear_binding_checksum();
  const auto serialized = unsigned_artifact.SerializeAsString();
  const auto actual_binding_checksum = sha256(
      serialized.data(), serialized.size());
  if (!constantTimeEqual(
          artifact.binding_checksum().digest(), actual_binding_checksum)) {
    throw std::runtime_error("ArtifactBinding SHA-256 mismatch");
  }
  std::unordered_set<std::string> object_ids;
  std::unordered_set<std::string> source_ordinals;
  std::unordered_set<std::string> artifact_sources;
  std::unordered_set<std::string> work_units;
  for (const auto& object : artifact.split_objects()) {
    if (object.object_id().empty() || object.opaque_uri().empty() ||
        object.object_role().empty() || object.source_object_id().empty() ||
        object.size_bytes() == 0 ||
        (object.row_count() == 0 &&
         artifact.empty_partition_policy() ==
             proto::ARTIFACT_EMPTY_PARTITION_POLICY_FORBID) ||
        !object.has_checksum() ||
        object.checksum().algorithm() != proto::Checksum::ALGORITHM_SHA256 ||
        object.checksum().digest().size() != 32 ||
        object.scope() == proto::ARTIFACT_OBJECT_SCOPE_UNSPECIFIED ||
        !object_ids.emplace(object.object_id()).second ||
        !source_ordinals.emplace(
            std::to_string(object.scope()) + std::string(1, '\0') +
            object.source_object_id() + std::string(1, '\0') +
            std::to_string(object.ordinal())).second) {
      throw std::runtime_error(
          "OperatorBinding contains an invalid artifact object");
    }
    if (object.scope() == proto::ARTIFACT_OBJECT_SCOPE_SOURCE_OBJECT) {
      artifact_sources.emplace(object.source_object_id());
    } else if (object.scope() == proto::ARTIFACT_OBJECT_SCOPE_WORK_UNIT) {
      work_units.emplace(object.source_object_id());
    } else if (object.source_object_id() != artifact.generation()) {
      throw std::runtime_error(
          "Generation artifact object has a mismatched scope identity");
    }
  }
  std::unordered_set<std::string> split_sources;
  for (const auto& split : request.splits()) {
    if (split.source_id() != artifact.source_id() ||
        split.source_snapshot() != artifact.source_snapshot()) {
      throw std::runtime_error(
          "ArtifactBinding source identity differs from a task split");
    }
    split_sources.emplace(split.data_object_id());
  }
  if (isArtifactTask(request.task_kind())) {
    if (!split_sources.empty() || !artifact_sources.empty() || work_units.empty()) {
      throw std::runtime_error(
          "Artifact reduction input must contain only work-unit objects");
    }
  } else if (split_sources.empty() || split_sources != artifact_sources ||
             !work_units.empty()) {
      throw std::runtime_error(
          "ArtifactBinding object coverage differs from task splits");
  }
  std::unordered_set<std::string> property_names;
  for (const auto& property : artifact.properties()) {
    if (property.name().empty() || !property.has_value() ||
        !property_names.emplace(property.name()).second) {
      throw std::runtime_error(
          "OperatorBinding contains invalid artifact properties");
    }
  }
}

void validateArtifactWrite(
    const proto::ArtifactWriteBinding& output,
    const proto::ExecutionPolicy& policy,
    const proto::TaskAttemptId& attempt,
    const proto::TaskRequest& request) {
  validateArtifactProvenance(output);
  if (output.artifact_id().empty() || output.artifact_type().empty() ||
      output.generation().empty() || output.source_id().empty() ||
      output.source_snapshot().empty() ||
      output.staging_uri().empty() || !output.has_format() ||
      output.format().format_id().empty() ||
      output.format().major_version() == 0 ||
      output.format().schema_sha256().size() != 32 ||
      output.publication_fencing_token() == 0 ||
      output.publication_fencing_token() != attempt.fencing_token() ||
      output.work_unit_id().empty() ||
      output.idempotency_key().empty() ||
      output.idempotency_key() != policy.idempotency_key() ||
      output.work_kind() == proto::ARTIFACT_WORK_KIND_UNSPECIFIED ||
      output.execution_kind() != artifactExecutionKind(request.task_kind())) {
    throw std::runtime_error(
        "OperatorBinding output artifact identity, format or fencing is incomplete");
  }
  std::unordered_set<std::string> expected_source_ids;
  const bool source_map = output.work_kind() ==
      proto::ARTIFACT_WORK_KIND_SOURCE_MAP;
  const bool relational_partition = output.work_kind() ==
      proto::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION;
  for (const auto& source_id : output.expected_source_object_ids()) {
    if (source_id.empty() || !expected_source_ids.emplace(source_id).second) {
      throw std::runtime_error(
          "OperatorBinding output artifact source objects are invalid");
    }
  }
  std::unordered_set<std::string> split_source_ids;
  for (const auto& split : request.splits()) {
    if (split.data_object_id().empty() ||
        (source_map && !split_source_ids.emplace(split.data_object_id()).second)) {
      throw std::runtime_error(
          "Background artifact splits require unique data_object_id values");
    }
    if (source_map && (split.source_id() != output.source_id() ||
        split.source_snapshot() != output.source_snapshot())) {
      throw std::runtime_error(
          "Output artifact source identity differs from a fenced task split");
    }
  }
  const bool reduce = output.work_kind() ==
      proto::ARTIFACT_WORK_KIND_DEPENDENCY_REDUCE;
  const bool finalize = output.work_kind() ==
      proto::ARTIFACT_WORK_KIND_GENERATION_FINALIZE;
  if ((source_map && (expected_source_ids.empty() ||
                      expected_source_ids != split_source_ids ||
                      !output.output_scope_id().empty())) ||
      ((reduce || finalize) && (!expected_source_ids.empty() ||
                               request.splits_size() != 0 ||
                               output.output_scope_id().empty())) ||
      (reduce && output.output_scope_id() != output.work_unit_id()) ||
      (finalize && output.output_scope_id() != output.generation()) ||
      (relational_partition && (!expected_source_ids.empty() ||
                               output.output_scope_id() != output.work_unit_id() ||
                               output.has_row_locator() || output.artifact_type() != "TABULAR_EXPORT")) ||
      (output.artifact_type() == "TABULAR_EXPORT" && !relational_partition) ||
      (!source_map && !reduce && !finalize && !relational_partition)) {
    throw std::runtime_error(
        "Output artifact work kind differs from its fenced inputs");
  }
  std::unordered_set<std::string> property_names;
  for (const auto& property : output.properties()) {
    if (property.name().empty() || !property.has_value() ||
        !property_names.emplace(property.name()).second) {
      throw std::runtime_error(
          "OperatorBinding output artifact properties are invalid");
    }
  }
}

void validateOperatorBindings(
    const proto::TaskRequest& request,
    executor::TaskExecutorConfig& config) {
  uint64_t requested_candidate_rows = 0;
  uint64_t requested_candidate_bytes = 0;
  uint64_t requested_fill_keys = 0;
  uint64_t requested_provider_concurrency = 0;
  uint64_t requested_provider_items = 0;
  uint64_t requested_provider_attempts = 0;
  uint64_t requested_provider_request_bytes = 0;
  uint64_t requested_provider_response_bytes = 0;
  uint64_t requested_media_bytes = 0;
  uint64_t requested_object_store_read_bytes = 0;
  uint64_t requested_object_store_write_bytes = 0;
  size_t output_artifact_count = 0;
  const bool background_artifact = isArtifactTask(request.task_kind());
  const auto add_bounded = [](uint64_t& total, uint64_t value,
                              const char* dimension) {
    if (std::numeric_limits<uint64_t>::max() - total < value) {
      throw std::runtime_error(
          std::string("OperatorBinding resource budget overflows: ") +
          dimension);
    }
    total += value;
  };
  for (const auto& binding : request.operator_bindings()) {
    if (binding.plan_node_id().empty() || !binding.has_capability() ||
        binding.capability().capability_id().empty() ||
        binding.capability().semantic_version().empty() ||
        binding.capability().signature_sha256().size() != 32 ||
        !isSupportedNativeCapability(binding.capability(), request.task_kind())) {
      throw std::runtime_error(
          "TaskRequest contains an unsupported OperatorBinding capability");
    }
    if (!binding.has_execution_policy()) {
      throw std::runtime_error(
          "OperatorBinding requires an execution policy");
    }
    if (!binding.has_resources()) {
      throw std::runtime_error(
          "OperatorBinding requires a resource envelope");
    }
    const auto& policy = binding.execution_policy();
    const auto& resources = binding.resources();
    const bool governed_model = binding.has_model();
    const bool common_policy_valid =
        policy.coverage_policy() ==
            proto::COVERAGE_POLICY_REQUIRE_COMPLETE &&
        policy.fallback_policy() == proto::FALLBACK_POLICY_FORBID &&
        policy.retry_class() == (governed_model
            ? proto::RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY
            : proto::RETRY_CLASS_IDEMPOTENT) &&
        (!governed_model || !policy.idempotency_key().empty()) &&
        policy.deadline_epoch_ms() == request.deadline_epoch_ms();
    const bool ai_invocation =
        pangrova::capability::isAiTextGeneration(binding.capability()) ||
        pangrova::capability::isAiMultimodalGeneration(binding.capability()) ||
        pangrova::capability::isAiEmbedding(binding.capability());
    if (governed_model &&
        (!binding.model().has_output_type() ||
         binding.model().output_type().canonical_sql().empty() ||
         !binding.model().output_type().has_substrait_type())) {
      throw std::runtime_error(
          "Governed model OperatorBinding has no exact output type");
    }
    if (pangrova::capability::isAiEmbedding(binding.capability())) {
      (void)pangrova::capability::requireFloatVectorDimension(
          binding.model().output_type());
    }
    if (binding.has_reuse_policy()) {
      const auto& reuse = binding.reuse_policy();
      const bool reusable_capability =
          pangrova::capability::isAiTextGeneration(binding.capability()) &&
          pangrova::capability::aiTextGenerationDescriptor().deterministic();
      if (!reuse.enabled() || !reusable_capability || background_artifact ||
          !governed_model || !reuse.require_complete() || reuse.ttl_ms() == 0 ||
          reuse.ttl_ms() > 7ULL * 24 * 60 * 60 * 1000 ||
          reuse.max_entries() == 0 || reuse.max_entries() > 1'000'000 ||
          reuse.max_bytes() == 0 || reuse.max_bytes() > 16ULL * 1024 * 1024 * 1024 ||
          !std::isfinite(reuse.similarity_threshold()) ||
          reuse.similarity_threshold() < 0.90 ||
          reuse.similarity_threshold() > 1.0 || reuse.key_dimension() < 64 ||
          reuse.key_dimension() > 4'096) {
        throw std::runtime_error(
            "OperatorBinding contains an unsafe execution reuse policy");
      }
      if (!request.has_security() || request.security().tenant_id().empty() ||
          request.security().principal().empty() ||
          request.security().policy_context_hash().empty()) {
        throw std::runtime_error(
            "Reusable operator requires tenant, principal and policy identity");
      }
    }
    const bool exact_materializer =
        pangrova::capability::isGeneratedValuesOverlay(binding.capability());
    const bool workload_policy_valid = background_artifact
        ? !policy.has_approximation() && !policy.idempotency_key().empty()
        : ai_invocation
            ? !policy.has_approximation() &&
                resources.candidate_rows() == 0 &&
                resources.candidate_bytes() == 0
            : exact_materializer
            ? !policy.has_approximation() &&
                resources.candidate_rows() == 0 &&
                resources.candidate_bytes() == 0 &&
                resources.fill_keys() != 0 &&
                resources.object_store_read_bytes() != 0
            : policy.has_approximation() &&
                policy.approximation().candidate_limit() != 0;
    if (!common_policy_valid || !workload_policy_valid) {
      throw std::runtime_error(
          "OperatorBinding execution policy is incomplete or unsafe");
    }
    if (!background_artifact && !ai_invocation && !exact_materializer &&
        resources.candidate_rows() !=
            policy.approximation().candidate_limit()) {
      throw std::runtime_error(
          "OperatorBinding candidate policy and resource envelope differ");
    }
    if (governed_model &&
        (resources.provider_concurrency() == 0 ||
         resources.provider_items() == 0 || resources.provider_attempts() == 0 ||
         resources.provider_request_bytes() == 0 || resources.provider_response_bytes() == 0 ||
         !binding.model().has_request_limits() || binding.model().request_limits().max_input_tokens() == 0 ||
         binding.model().request_limits().max_request_bytes() == 0 ||
         binding.model().request_limits().max_response_bytes() == 0)) {
      throw std::runtime_error(
          "Governed model OperatorBinding has no provider resource budget");
    }
    if (!governed_model &&
        (resources.provider_concurrency() != 0 ||
         resources.provider_items() != 0 || resources.provider_attempts() != 0 ||
         resources.provider_request_bytes() != 0 || resources.provider_response_bytes() != 0 ||
         resources.media_bytes() != 0)) {
      throw std::runtime_error(
          "Non-model OperatorBinding requests provider resources");
    }
    add_bounded(requested_candidate_rows, resources.candidate_rows(),
                "candidate_rows");
    add_bounded(requested_candidate_bytes, resources.candidate_bytes(),
                "candidate_bytes");
    add_bounded(requested_fill_keys, resources.fill_keys(), "fill_keys");
    add_bounded(requested_provider_concurrency,
                resources.provider_concurrency(), "provider_concurrency");
    add_bounded(requested_provider_items, resources.provider_items(), "provider_items");
    add_bounded(requested_provider_attempts, resources.provider_attempts(), "provider_attempts");
    add_bounded(requested_provider_request_bytes, resources.provider_request_bytes(), "provider_request_bytes");
    add_bounded(requested_provider_response_bytes, resources.provider_response_bytes(), "provider_response_bytes");
    add_bounded(requested_media_bytes, resources.media_bytes(), "media_bytes");
    add_bounded(requested_object_store_read_bytes,
                resources.object_store_read_bytes(), "object_store_read_bytes");
    add_bounded(requested_object_store_write_bytes,
                resources.object_store_write_bytes(), "object_store_write_bytes");
    for (const auto& artifact : binding.artifacts()) {
      validateArtifact(artifact, request);
    }
    if (binding.has_output_artifact()) {
      ++output_artifact_count;
      if (!background_artifact) {
        throw std::runtime_error(
            "Output artifact binding is only valid for a background artifact task");
      }
      validateArtifactWrite(
          binding.output_artifact(), policy, request.attempt(), request);
      const auto kind = binding.output_artifact().work_kind();
      const bool source_map = kind == proto::ARTIFACT_WORK_KIND_SOURCE_MAP;
      const bool relational_partition = kind == proto::ARTIFACT_WORK_KIND_RELATIONAL_PARTITION;
      if (((source_map || relational_partition) && binding.artifacts_size() != 0) ||
          (!source_map && !relational_partition && binding.artifacts_size() != 1)) {
        throw std::runtime_error(
            "Artifact work kind differs from its dependency binding count");
      }
    }
    for (const auto& policy_binding : binding.policies()) {
      if (policy_binding.policy_id().empty() ||
          policy_binding.version().empty() ||
          policy_binding.policy_sha256().size() != 32) {
        throw std::runtime_error(
            "OperatorBinding contains an invalid governance policy");
      }
    }
    if (!config.operator_bindings.emplace(
            binding.plan_node_id(), binding).second) {
      throw std::runtime_error(
          "TaskRequest contains duplicate OperatorBinding plan_node_id");
    }
  }
  if (background_artifact && output_artifact_count != 1) {
    throw std::runtime_error(
        "Background artifact task requires exactly one fenced output binding");
  }
  if (background_artifact &&
      (!request.has_resource_grant() ||
       config.resource_limit.max_memory_bytes <= 0 ||
       config.resource_limit.max_spill_bytes <= 0 ||
       config.resource_limit.max_object_store_read_bytes <= 0 ||
       config.resource_limit.max_object_store_write_bytes <= 0)) {
    throw std::runtime_error(
        "Background artifact task requires bounded memory, spill and object-store grants");
  }
  if (request.operator_bindings_size() == 0) {
    return;
  }
  if (!request.has_resource_grant() ||
      request.resource_grant().grant_id().empty() ||
      request.resource_grant().candidate_rows() < requested_candidate_rows ||
      request.resource_grant().candidate_bytes() < requested_candidate_bytes ||
      request.resource_grant().fill_keys() < requested_fill_keys ||
      request.resource_grant().provider_concurrency() <
          requested_provider_concurrency ||
      request.resource_grant().provider_items() < requested_provider_items ||
      request.resource_grant().provider_attempts() < requested_provider_attempts ||
      request.resource_grant().provider_request_bytes() < requested_provider_request_bytes ||
      request.resource_grant().provider_response_bytes() < requested_provider_response_bytes ||
      request.resource_grant().media_bytes() < requested_media_bytes ||
      request.resource_grant().object_store_read_bytes() <
          requested_object_store_read_bytes ||
      request.resource_grant().object_store_write_bytes() <
          requested_object_store_write_bytes ||
      (requested_candidate_rows > 0 &&
       (request.resource_grant().candidate_bytes() == 0 ||
        request.resource_grant().native_memory_bytes() == 0))) {
    throw std::runtime_error(
        "TaskRequest resource grant does not cover OperatorBinding budgets");
  }
}

void validateRuntimeFilterBindings(
    const proto::TaskRequest& request,
    executor::TaskExecutorConfig& config) {
  std::unordered_set<std::string> plan_node_ids;
  config.initial_runtime_filters.reserve(
      request.initial_runtime_filters_size());
  for (const auto& binding : request.initial_runtime_filters()) {
    if (binding.plan_node_id().empty() || binding.filters_size() == 0 ||
        !plan_node_ids.emplace(binding.plan_node_id()).second) {
      throw std::runtime_error(
          "TaskRequest initial runtime-filter binding is invalid or duplicated");
    }
    for (const auto& filter : binding.filters()) {
      if (filter.column_name().empty() ||
          filter.scope() == proto::RUNTIME_FILTER_SCOPE_UNSPECIFIED ||
          filter.predicate_case() == proto::RuntimeFilter::PREDICATE_NOT_SET) {
        throw std::runtime_error(
            "TaskRequest initial runtime filter is incomplete");
      }
    }
    config.initial_runtime_filters.push_back(binding);
  }
}

executor::WriteConfigInfo parseWriteOptions(
    const proto::WriteTaskOptions& options,
    const proto::TaskAttemptId& attempt) {
  if (options.output_binding().has_physical()) {
    return parsePhysicalWriteOptions(options, attempt);
  }
  executor::WriteConfigInfo config;
  config.target_table = options.target_table();
  config.write_mode = options.write_mode();
  config.partition_spec.assign(
      options.partition_spec().begin(), options.partition_spec().end());
  config.file_format = parseFileFormat(options.file_format());
  config.target_file_size = options.target_file_size();

  if (config.file_format != split::FileFormatType::LANCE) {
    if (!options.has_output_binding() ||
        options.output_binding().binding_case() !=
            proto::WriteOutputBinding::kConnector) {
      throw std::runtime_error(
          "Non-Lance write requires WriteOutputBinding.connector");
    }
    const auto& connector = options.output_binding().connector();
    const auto& catalog = connector.catalog();
    const auto& handle = connector.output_handle();
    if (!connector.has_catalog() || !connector.has_output_handle() ||
        catalog.catalog_name().empty() ||
        catalog.connector_provider().empty() ||
        catalog.catalog_incarnation_id().empty() ||
        catalog.catalog_revision() == 0 ||
        !catalog.has_configuration_fingerprint() ||
        catalog.configuration_fingerprint().algorithm() !=
            proto::Checksum::ALGORITHM_SHA256 ||
        catalog.configuration_fingerprint().digest().size() != 32 ||
        handle.type_url().empty() || handle.codec_version() == 0 ||
        handle.payload().empty() ||
        handle.payload().size() > 16 * 1024 * 1024 ||
        !handle.has_payload_checksum() ||
        handle.payload_checksum().algorithm() !=
            proto::Checksum::ALGORITHM_SHA256 ||
        handle.payload_checksum().digest().size() != 32 ||
        !constantTimeEqual(
            sha256(handle.payload().data(), handle.payload().size()),
            handle.payload_checksum().digest())) {
      throw std::runtime_error(
          "ConnectorWriteBinding is incomplete or has an invalid checksum");
    }
    return config;
  }
  if (!options.has_output_binding() ||
      options.output_binding().binding_case() !=
          proto::WriteOutputBinding::kLance) {
    throw std::runtime_error(
        "Lance write requires connector-owned WriteOutputBinding.lance");
  }
  const auto& lance = options.output_binding().lance();
  const auto& catalog = lance.catalog();
  const auto& handle = lance.output_handle();
  std::unordered_set<std::string> column_names;
  if (!lance.has_catalog() || !lance.has_output_handle() ||
      catalog.catalog_name().empty() || catalog.connector_provider().empty() ||
      catalog.catalog_incarnation_id().empty() || catalog.catalog_revision() == 0 ||
      catalog.configuration_fingerprint().algorithm() != proto::Checksum::ALGORITHM_SHA256 ||
      catalog.configuration_fingerprint().digest().size() != 32 ||
      handle.type_url().empty() || handle.codec_version() == 0 ||
      handle.payload().empty() || handle.payload().size() > 16 * 1024 * 1024 ||
      handle.payload_checksum().algorithm() != proto::Checksum::ALGORITHM_SHA256 ||
      !constantTimeEqual(sha256(handle.payload().data(), handle.payload().size()),
                         handle.payload_checksum().digest()) ||
      lance.write_column_names_size() == 0 ||
      lance.write_column_names_size() != handle.write_column_names_size()) {
    throw std::runtime_error("Lance binding requires an exact catalog and output handle");
  }
  for (int i = 0; i < lance.write_column_names_size(); ++i) {
    const auto& name = lance.write_column_names(i);
    if (name.empty() || name != handle.write_column_names(i) ||
        !column_names.emplace(name).second ||
        std::all_of(name.begin(), name.end(), [](unsigned char c) { return std::isspace(c) != 0; })) {
      throw std::runtime_error("Lance binding requires unique pinned write columns");
    }
  }
  const auto blank = [](const std::string& value) {
    return value.empty() || std::all_of(
        value.begin(), value.end(), [](unsigned char character) {
          return std::isspace(character) != 0;
        });
  };
  const auto normalized_relative_prefix = [](const std::string& value) {
    if (value.empty() || value.size() > 2048 || value.front() == '/' ||
        value.back() == '/' || value.find("//") != std::string::npos ||
        value.find('\\') != std::string::npos ||
        value.find("://") != std::string::npos) {
      return false;
    }
    size_t start = 0;
    while (start < value.size()) {
      const auto end = value.find('/', start);
      const auto segment = value.substr(
          start, end == std::string::npos ? std::string::npos : end - start);
      if (segment.empty() || segment == "." || segment == ".." ||
          !std::all_of(segment.begin(), segment.end(), [](unsigned char ch) {
            return std::isalnum(ch) != 0 || ch == '-' || ch == '_' ||
                ch == '.' || ch == '=';
          })) {
        return false;
      }
      if (end == std::string::npos) break;
      start = end + 1;
    }
    return true;
  };
  if (blank(lance.dataset_uri()) || blank(lance.write_id()) ||
      blank(lance.generation()) ||
      !normalized_relative_prefix(lance.transaction_staging_prefix()) ||
      !normalized_relative_prefix(lance.allocation_staging_prefix()) ||
      lance.operation() == proto::LANCE_WRITE_OPERATION_UNSPECIFIED ||
      !lance.has_schema_fence_sha256() ||
      lance.schema_fence_sha256().algorithm() !=
          proto::Checksum::ALGORITHM_SHA256 ||
      lance.schema_fence_sha256().digest().size() != 32) {
    throw std::runtime_error(
        "Lance output binding is incomplete or has an invalid SHA-256 schema fence");
  }
  const bool create =
      lance.operation() == proto::LANCE_WRITE_OPERATION_CREATE;
  if ((create && lance.expected_read_version() != 0) ||
      (!create && lance.expected_read_version() == 0)) {
    throw std::runtime_error(
        "Lance CREATE requires read version 0; APPEND/OVERWRITE require a non-zero version");
  }
  const bool mode_matches =
      (lance.operation() == proto::LANCE_WRITE_OPERATION_CREATE &&
       (config.write_mode == "CREATE" || config.write_mode == "INSERT" ||
        config.write_mode == "OVERWRITE")) ||
      (lance.operation() == proto::LANCE_WRITE_OPERATION_APPEND &&
       (config.write_mode == "INSERT" || config.write_mode == "APPEND")) ||
      (lance.operation() == proto::LANCE_WRITE_OPERATION_OVERWRITE &&
       config.write_mode == "OVERWRITE");
  if (!mode_matches) {
    throw std::runtime_error(
        "Lance output operation does not match canonical write_mode");
  }

  executor::WriteConfigInfo::LanceOutputBinding binding;
  binding.dataset_uri = lance.dataset_uri();
  binding.operation = lance.operation();
  binding.expected_read_version = lance.expected_read_version();
  if ((lance.expected_read_version() == 0) != !lance.has_base_snapshot()) {
    throw std::invalid_argument("Native Dataset write requires its exact accepted positive base");
  }
  if (lance.has_base_snapshot()) {
    const auto& base = lance.base_snapshot();
    if (base.dataset_uri() != lance.dataset_uri() || base.version() != lance.expected_read_version()
        || base.catalog().SerializeAsString() != lance.catalog().SerializeAsString()
        || base.physical_schema_sha256().SerializeAsString() != lance.schema_fence_sha256().SerializeAsString()) {
      throw std::invalid_argument("Native Dataset accepted base differs from its allocation");
    }
    binding.accepted_base_snapshot = base.SerializeAsString();
  }
  std::copy(
      lance.schema_fence_sha256().digest().begin(),
      lance.schema_fence_sha256().digest().end(),
      binding.schema_fence_sha256.begin());
  binding.write_id = lance.write_id();
  binding.transaction_staging_prefix = lance.transaction_staging_prefix();
  binding.generation = lance.generation();
  binding.write_column_names.assign(lance.write_column_names().begin(),
                                    lance.write_column_names().end());
  std::string serialized_attempt;
  serialized_attempt.resize(attempt.ByteSizeLong());
  google::protobuf::io::ArrayOutputStream output(
      serialized_attempt.data(),
      static_cast<int>(serialized_attempt.size()));
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!attempt.SerializeToCodedStream(&coded) || coded.HadError() ||
      serialized_attempt.empty()) {
    throw std::runtime_error("Failed to serialize canonical TaskAttemptId");
  }
  binding.task_attempt.assign(
      serialized_attempt.begin(), serialized_attempt.end());
  if (lance.transaction_staging_prefix() != lance.allocation_staging_prefix() +
          "/attempt-" + hex(sha256(serialized_attempt.data(), serialized_attempt.size()))) {
    throw std::runtime_error("Lance staging prefix does not match the exact task attempt");
  }
  config.lance_output = std::move(binding);
  return config;
}

bool isBlank(std::string_view value) {
  return value.empty() || std::all_of(
      value.begin(), value.end(), [](unsigned char character) {
        return std::isspace(character) != 0;
      });
}

bool sameExecutionOwner(
    const proto::ExecutionOwnerIdentity& left,
    const proto::ExecutionOwnerIdentity& right) {
  return left.owner_id() == right.owner_id() &&
      left.owner_incarnation() == right.owner_incarnation() &&
      left.fencing_token() == right.fencing_token();
}

executor::ShuffleExecutionConfig parseShuffleExecution(
    const proto::ShuffleExecutionBinding& binding,
    const proto::TaskAttemptId& attempt,
    int32_t partition_count) {
  const auto now_millis = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  if (isBlank(binding.shuffle_id()) || binding.endpoints_size() == 0 ||
      binding.plan_epoch() != attempt.plan_epoch() ||
      !binding.has_execution_owner() ||
      !sameExecutionOwner(binding.execution_owner(),
                          attempt.execution_owner()) ||
      !binding.has_write_capability() ||
      binding.write_capability().token().empty() ||
      binding.write_capability().expires_at_epoch_ms() <=
          static_cast<uint64_t>(now_millis)) {
    throw std::runtime_error(
        "TaskRequest shuffle execution binding is incomplete or inconsistent");
  }
  executor::ShuffleExecutionConfig config;
  config.shuffle_id = binding.shuffle_id();
  config.plan_epoch = binding.plan_epoch();
  config.owner_id = binding.execution_owner().owner_id();
  config.owner_incarnation = binding.execution_owner().owner_incarnation();
  config.owner_fencing_token = binding.execution_owner().fencing_token();
  config.write_access_token.assign(
      binding.write_capability().token().begin(),
      binding.write_capability().token().end());
  config.write_access_expires_at_epoch_ms =
      binding.write_capability().expires_at_epoch_ms();
  config.endpoints.reserve(binding.endpoints_size());
  std::vector<bool> covered(static_cast<size_t>(partition_count), false);
  for (const auto& endpoint : binding.endpoints()) {
    if (isBlank(endpoint.host()) || endpoint.port() <= 0 ||
        endpoint.port() > 65535 ||
        endpoint.partition_start() < 0 ||
        endpoint.partition_end() <= endpoint.partition_start() ||
        endpoint.partition_end() > partition_count ||
        (!endpoint.tls_identity().empty() &&
         isBlank(endpoint.tls_identity()))) {
      throw std::runtime_error(
          "TaskRequest shuffle service endpoint is invalid");
    }
    for (int32_t partition = endpoint.partition_start();
         partition < endpoint.partition_end(); ++partition) {
      if (covered[static_cast<size_t>(partition)]) {
        throw std::runtime_error(
            "TaskRequest shuffle service endpoint ranges overlap");
      }
      covered[static_cast<size_t>(partition)] = true;
    }
    config.endpoints.push_back({
        endpoint.host(), endpoint.port(), endpoint.partition_start(),
        endpoint.partition_end(), endpoint.tls_identity()});
  }
  if (std::find(covered.begin(), covered.end(), false) != covered.end()) {
    throw std::runtime_error(
        "TaskRequest shuffle service endpoint ranges have a gap");
  }
  return config;
}

void parseShuffleOptions(
    const proto::ShuffleTaskOptions& shuffle,
    const proto::TaskAttemptId& attempt,
    executor::TaskExecutorConfig& config) {
  if (shuffle.serde() != proto::BATCH_SERDE_ARROW_BATCH ||
      shuffle.partition_count() == 0 ||
      isBlank(shuffle.compression_codec()) || shuffle.batch_size() == 0 ||
      (shuffle.persistence_mode() != proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_ONLY &&
       shuffle.persistence_mode() != proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK &&
       shuffle.persistence_mode() != proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE)) {
    throw std::runtime_error("TaskRequest shuffle options are incomplete");
  }
  if (shuffle.partition_count() > static_cast<uint32_t>(
          std::numeric_limits<int32_t>::max()) ||
      shuffle.batch_size() > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    throw std::runtime_error("shuffle partition_count or batch_size exceeds int32 range");
  }
  if (shuffle.compression_codec() != "none" && shuffle.compression_codec() != "lz4" &&
      shuffle.compression_codec() != "zstd") {
    throw std::runtime_error("shuffle compression_codec requires exactly none, lz4 or zstd");
  }
  const bool remote = shuffle.persistence_mode() ==
      proto::SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE;
  if (remote != shuffle.has_execution_binding()) {
    throw std::runtime_error(
        "TaskRequest shuffle execution binding is incomplete or inconsistent");
  }
  config.num_shuffle_partitions =
      static_cast<int32_t>(shuffle.partition_count());
  config.shuffle_batch_serde = shuffle.serde();
  config.shuffle_compression_codec = shuffle.compression_codec();
  config.shuffle_batch_size = static_cast<int32_t>(shuffle.batch_size());
  config.shuffle_persistence_mode = shuffle.persistence_mode();
  config.shuffle_partition_key_indices.assign(
      shuffle.partition_key_indices().begin(),
      shuffle.partition_key_indices().end());
  if (remote) {
    config.shuffle_execution =
        parseShuffleExecution(
            shuffle.execution_binding(), attempt,
            config.num_shuffle_partitions);
  }
}

std::string serializeCallbackContext(const proto::TaskRequest& request) {
  proto::ExecutionCallbackContext context;
  *context.mutable_protocol() = request.protocol();
  *context.mutable_security() = request.security();
  *context.mutable_attempt() = request.attempt();
  *context.mutable_resource_grant() = request.resource_grant();
  context.set_deadline_epoch_ms(request.deadline_epoch_ms());
  if (request.has_trace_context()) *context.mutable_trace_context() = request.trace_context();
  const auto size = context.ByteSizeLong();
  if (size == 0 || size > 1'048'576) {
    throw std::runtime_error("Canonical callback context exceeds its byte bound");
  }
  std::string bytes(size, '\0');
  google::protobuf::io::ArrayOutputStream output(bytes.data(), static_cast<int>(size));
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!context.SerializeToCodedStream(&coded) || coded.HadError()) {
    throw std::runtime_error("Cannot serialize canonical callback context");
  }
  return bytes;
}

executor::TaskExecutorConfig parseFromProto(const proto::TaskRequest& request) {
  validateWorkerCopyInput(request);
  const auto& attempt = request.attempt();
  if (request.task_kind() == proto::TASK_KIND_UNSPECIFIED) {
    throw std::runtime_error("TaskRequest task_kind is unspecified");
  }

  executor::TaskExecutorConfig config;
  config.task_id = attempt.task_id();
  config.query_id = attempt.query_or_job_id();
  config.stage_id = attempt.stage_id();
  config.partition_id = attempt.partition_id();
  config.plan_epoch = attempt.plan_epoch();
  config.attempt_id = attempt.attempt_id();
  config.fencing_token = attempt.fencing_token();
  config.control_epoch = attempt.control_epoch();
  config.owner_id = attempt.execution_owner().owner_id();
  config.owner_incarnation = attempt.execution_owner().owner_incarnation();
  config.owner_fencing_token = attempt.execution_owner().fencing_token();
  config.task_type = static_cast<int32_t>(request.task_kind());
  config.tenant_id = request.security().tenant_id();
  config.principal = request.security().principal();
  config.policy_context_hash = request.security().policy_context_hash();
  const auto [dependency_scope, dependency_version] =
      dependencyFingerprints(request);
  config.dependency_scope_hash = dependency_scope;
  config.dependency_version_hash = dependency_version;
  config.deadline_epoch_ms =
      checkedInt64(request.deadline_epoch_ms(), "deadline_epoch_ms");
  config.substrait_plan.assign(
      request.substrait_plan().begin(), request.substrait_plan().end());

  config.splits.reserve(request.splits_size());
  std::unordered_set<std::string> split_ids;
  std::unordered_set<std::string> manifest_partitions;
  for (const auto& split : request.splits()) {
    if (!split_ids.emplace(split.split_id()).second) {
      throw std::runtime_error("TaskRequest contains duplicate DataSplit split_id");
    }
    auto parsed = parseSplit(split, request);
    if (parsed.file_format == split::FileFormatType::LANCE) {
      proto::FileSplitDetail detail;
      if (!detail.ParseFromString(parsed.file_scan_detail)) {
        throw std::runtime_error("Malformed canonical file read unit");
      }
      const auto partition_key = split.source_id() + '\0' + split.source_snapshot() + '\0' +
          split.opaque_location() + '\0' + (detail.has_scan_partition()
          ? std::to_string(detail.scan_partition().partition_id()) : detail.physical_file_scan().SerializeAsString());
      if (!manifest_partitions.emplace(partition_key).second) {
        throw std::runtime_error("TaskRequest repeats an immutable read unit");
      }
      parsed.storage_binding = pangrova::resource::workerLocalStorage(request.worker_local_storage_bindings(),
          split.catalog(), request.security().tenant_id(), split.opaque_location());
    }
    if (parsed.paimon.has_value()) {
      for (const auto& level : parsed.paimon->lsm_levels) {
        for (const auto& file : level.files) {
          proto::FileSplitDetail read;
          if (!read.ParseFromString(file.file_scan_detail)) throw std::invalid_argument("Malformed Paimon file read unit");
          if (file.file_format == split::FileFormatType::LANCE || read.physical_file_scan().has_deletion_vector()) {
            parsed.storage_binding = pangrova::resource::workerLocalStorage(request.worker_local_storage_bindings(),
                split.catalog(), request.security().tenant_id(), file.file_path);
            if (read.physical_file_scan().has_deletion_vector()) {
              pangrova::resource::workerLocalStorage(request.worker_local_storage_bindings(), split.catalog(),
                  request.security().tenant_id(), read.physical_file_scan().deletion_vector().opaque_location());
            }
          }
        }
      }
    }
    config.splits.push_back(std::move(parsed));
  }
  config.resource_limit = parseResourceGrant(request.resource_grant());
  applyWorkerCopyInput(request, config);
  validateOperatorBindings(request, config);
  validateRuntimeFilterBindings(request, config);
  if (request.has_execution_options()) {
    const auto& options = request.execution_options();
    config.session_properties.insert(
        options.session_properties().begin(),
        options.session_properties().end());
    if (options.has_write()) {
      config.write_config = parseWriteOptions(options.write(), attempt);
      if (config.write_config.physical_output) {
        auto& output = *config.write_config.physical_output;
        output.storage_binding = pangrova::resource::workerLocalStorage(request.worker_local_storage_bindings(),
            output.binding.catalog(), request.security().tenant_id(), output.binding.files(0).opaque_location());
      }
      if (config.write_config.lance_output) {
        const auto& binding = options.write().output_binding().lance();
        config.write_config.lance_output->storage_binding = pangrova::resource::workerLocalStorage(
            request.worker_local_storage_bindings(), binding.catalog(), request.security().tenant_id(),
            binding.dataset_uri());
      }
    }
    if (options.has_shuffle()) {
      if (!request.has_resource_grant() ||
          config.resource_limit.max_shuffle_buffer_bytes <
          static_cast<int64_t>(pangrova::shuffle::kCanonicalShuffleBatchBytes +
                               sizeof(folly::IOBuf) + 128)) {
        throw std::runtime_error(
            "Shuffle task requires a complete positive buffer byte grant");
      }
      parseShuffleOptions(options.shuffle(), attempt, config);
    }
    config.is_result_task = options.direct_result_output();
    if (config.is_result_task && config.resource_limit.max_shuffle_buffer_bytes <= 0) {
      throw std::runtime_error(
          "Serialized output requires a positive serialized-buffer grant");
    }
  }
  if (request.result_column_types_size() > 0 && !config.is_result_task) {
    throw std::runtime_error(
        "Result column logical types require direct result output");
  }
  std::unordered_set<uint32_t> result_indices;
  for (const auto& column : request.result_column_types()) {
    if (!result_indices.emplace(column.column_index()).second) {
      throw std::runtime_error("Duplicate result column logical type index");
    }
    if (!column.has_logical_type()) {
      throw std::runtime_error("Result column has no logical type");
    }
    const auto dimension =
        pangrova::capability::requireStorageVectorDimension(column.logical_type());
    config.result_column_types.push_back(
        {column.column_index(), dimension, column.logical_type()});
  }
  config.callback_context = serializeCallbackContext(request);
  return config;
}

}  // namespace

executor::TaskExecutorConfig parseTaskConfig(
    const std::vector<uint8_t>& data) {
  return parseFromProto(parseTaskRequest(data));
}

protocol::v2::TaskRequest parseTaskRequest(const std::vector<uint8_t>& data) {
  if (data.empty()) {
    throw std::runtime_error("TaskRequest payload must not be empty");
  }
  proto::TaskRequest request;
  if (data.size() > static_cast<size_t>(std::numeric_limits<int>::max()) ||
      !request.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
    throw std::runtime_error("Failed to parse canonical v2 TaskRequest");
  }
  if (!request.has_protocol() || !request.has_attempt() ||
      !request.GetReflection()->GetUnknownFields(request).empty()) {
    throw std::runtime_error("TaskRequest requires known protocol and attempt identity");
  }
  validateWorkerCopyInput(request);
  validateProtocol(request.protocol());
  validatePlanChecksum(request);
  validateCallbackAuthority(request);
  return request;
}

executor::TaskExecutorConfig taskConfigFromRequest(const proto::TaskRequest& request) {
  return parseFromProto(request);
}

void validateCreationIdentity(const proto::ExecutorCreationRecoveryRequest& identity) {
  const auto& seal = identity.admission_seal();
  const auto& checksum = seal.canonical_request_checksum();
  if (!identity.has_admission_seal() || !identity.has_security() ||
      !seal.has_protocol() || !seal.has_attempt() || !seal.has_canonical_request_checksum() ||
      !identity.GetReflection()->GetUnknownFields(identity).empty() ||
      !seal.GetReflection()->GetUnknownFields(seal).empty() ||
      !checksum.GetReflection()->GetUnknownFields(checksum).empty() ||
      checksum.algorithm() != proto::Checksum::ALGORITHM_SHA256 || checksum.digest().size() != 32) {
    throw std::runtime_error("Creation recovery requires exact canonical identity and SHA-256");
  }
  validateProtocol(seal.protocol());
  proto::TaskRequest authority;
  *authority.mutable_attempt() = seal.attempt();
  *authority.mutable_security() = identity.security();
  authority.set_deadline_epoch_ms(seal.deadline_epoch_ms());
  validateCallbackAuthority(authority);
}

}  // namespace pangrova::jni
