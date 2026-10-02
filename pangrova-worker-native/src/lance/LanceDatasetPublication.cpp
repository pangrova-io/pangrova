#include "lance/LanceDatasetPublication.h"
#include "lance/LanceStorageContext.h"

#include <algorithm>
#include <chrono>
#include <charconv>
#include <cctype>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/unknown_field_set.h"
#include "lance_c_ffi.h"
#include "observability/PangrovaLog.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::lance {
namespace {
namespace proto = protocol::v2;
constexpr size_t kMaxMetadataBytes = 16 * 1024 * 1024;
constexpr size_t kMaxFragmentBytes = 64 * 1024;
constexpr uint64_t kMaxTransactionBytes = 64 * 1024 * 1024;
constexpr int kMaxFragments = 4096;

void require(bool condition, const char* message) {
  if (!condition) throw std::invalid_argument(message);
}

bool text(const std::string& value, size_t maximum = 4096) {
  return !value.empty() && value.size() <= maximum &&
      std::any_of(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
      }) && std::none_of(value.begin(), value.end(), [](unsigned char ch) {
        return ch < 32 || ch == 127;
      });
}

void validateDeadline(uint64_t deadline) {
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  require(deadline > static_cast<uint64_t>(now),
          "Dataset control deadline is missing or expired");
}

bool checksum(const proto::Checksum& value) {
  return value.algorithm() == proto::Checksum::ALGORITHM_SHA256 &&
      value.digest().size() == 32;
}

std::string sha256(const std::string& input) {
  std::array<unsigned char, 32> digest{};
  unsigned int size = 0;
  if (EVP_Digest(input.data(), input.size(), digest.data(), &size,
                 EVP_sha256(), nullptr) != 1 || size != digest.size()) {
    throw std::runtime_error("Dataset metadata SHA-256 calculation failed");
  }
  return {reinterpret_cast<const char*>(digest.data()), digest.size()};
}

bool equal(const std::string& left, const std::string& right) {
  return left.size() == right.size() &&
      CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

std::string hex(const std::string& bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    result.push_back(digits[byte >> 4]);
    result.push_back(digits[byte & 15]);
  }
  return result;
}

std::string canonical(const google::protobuf::Message& message) {
  require(message.ByteSizeLong() <= kMaxMetadataBytes,
          "Dataset metadata exceeds the 16 MiB limit");
  std::string bytes(message.ByteSizeLong(), '\0');
  google::protobuf::io::ArrayOutputStream output(bytes.data(), bytes.size());
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
    throw std::runtime_error("Dataset metadata serialization failed");
  }
  return bytes;
}

void validateKnownFields(const google::protobuf::Message& message) {
  const auto* reflection = message.GetReflection();
  require(reflection->GetUnknownFields(message).empty(),
      "Dataset control metadata contains fields outside the canonical protocol epoch");
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  reflection->ListFields(message, &fields);
  for (const auto* field : fields) {
    if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) continue;
    if (field->is_repeated()) {
      for (int index = 0; index < reflection->FieldSize(message, field); ++index) {
        validateKnownFields(reflection->GetRepeatedMessage(message, field, index));
      }
    } else {
      validateKnownFields(reflection->GetMessage(message, field));
    }
  }
}

void validateCatalog(const proto::CatalogExecutionBinding& catalog) {
  require(text(catalog.catalog_name()) && text(catalog.connector_provider()) &&
      text(catalog.catalog_incarnation_id()) && catalog.catalog_revision() != 0 &&
      checksum(catalog.configuration_fingerprint()),
      "Dataset control requires an exact catalog deployment");
}

void validateSecurity(const proto::SecurityContext& security) {
  require(text(security.tenant_id()) && text(security.principal()),
      "Dataset control requires a tenant and principal");
}

bool relativePrefix(const std::string& prefix) {
  if (!text(prefix, 2048) || prefix.front() == '/' || prefix.back() == '/') return false;
  size_t start = 0;
  while (start < prefix.size()) {
    const auto end = prefix.find('/', start);
    const auto segment = prefix.substr(start, end == std::string::npos
        ? std::string::npos : end - start);
    if (segment.empty() || segment == "." || segment == ".." ||
        !std::all_of(segment.begin(), segment.end(), [](unsigned char ch) {
          return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '=';
        })) return false;
    if (end == std::string::npos) break;
    start = end + 1;
  }
  return true;
}

void validateOwner(const proto::ExecutionOwnerIdentity& owner) {
  require(text(owner.owner_id()) && text(owner.owner_incarnation()) &&
      owner.fencing_token() != 0, "Dataset publication requires a fenced execution owner");
}

void validateAllocation(const proto::LanceWriteBinding& binding) {
  validateCatalog(binding.catalog());
  require(text(binding.dataset_uri(), 8192) && text(binding.write_id()) &&
      text(binding.generation()) && relativePrefix(binding.allocation_staging_prefix()) &&
      binding.transaction_staging_prefix() == binding.allocation_staging_prefix() &&
      checksum(binding.schema_fence_sha256()),
      "Dataset publication requires the original connector allocation");
  require(binding.operation() == proto::LANCE_WRITE_OPERATION_CREATE ||
      binding.operation() == proto::LANCE_WRITE_OPERATION_APPEND ||
      binding.operation() == proto::LANCE_WRITE_OPERATION_OVERWRITE,
      "Dataset publication operation is invalid");
  require((binding.operation() == proto::LANCE_WRITE_OPERATION_CREATE) ==
      (binding.expected_read_version() == 0), "Dataset publication base version is invalid");
  require(binding.expected_read_version() < std::numeric_limits<uint64_t>::max(),
      "Dataset publication base version cannot advance");
  const auto& handle = binding.output_handle();
  require(text(handle.type_url()) && handle.codec_version() > 0 &&
      !handle.payload().empty() && handle.payload().size() <= kMaxMetadataBytes &&
      checksum(handle.payload_checksum()) &&
      equal(sha256(handle.payload()), handle.payload_checksum().digest()) &&
      binding.write_column_names_size() > 0 && binding.write_column_names_size() <= 4096 &&
      handle.write_column_names_size() == binding.write_column_names_size(),
      "Dataset allocation requires its exact checksummed output handle");
  std::unordered_set<std::string> names;
  for (int index = 0; index < binding.write_column_names_size(); ++index) {
    const auto& name = binding.write_column_names(index);
    require(text(name) && name == handle.write_column_names(index) &&
        names.insert(name).second, "Dataset allocation write columns are inconsistent");
  }
}

void validateAttempt(const proto::TaskAttemptId& attempt,
                     const proto::FinalizeDistributedWriteRequest& request) {
  require(attempt.query_or_job_id() == request.execution_id() &&
      text(attempt.task_id()) && attempt.stage_id() >= 0 && attempt.partition_id() >= 0 &&
      attempt.plan_epoch() > 0 && attempt.fencing_token() > 0 &&
      attempt.control_epoch() > 0 && attempt.control_epoch() <= request.control_epoch() &&
      attempt.has_execution_owner() &&
      equal(canonical(attempt.execution_owner()), canonical(request.intent_execution_owner())),
      "Dataset winner attempt differs from the execution owner/control fence");
}

std::string validateFragment(const proto::DistributedWriteFragment& winner,
                            const proto::FinalizeDistributedWriteRequest& request) {
  const auto& binding = request.output_binding().lance();
  validateAttempt(winner.task_attempt(), request);
  require(!winner.commit_metadata().empty() && winner.commit_metadata().size() <= kMaxFragmentBytes &&
      checksum(winner.sha256()) &&
      equal(sha256(winner.commit_metadata()), winner.sha256().digest()),
      "Dataset winner commit metadata is missing, oversized or has an invalid SHA-256");
  proto::LanceWriteFragment fragment;
  require(fragment.ParseFromString(winner.commit_metadata()) &&
      fragment.GetReflection()->GetUnknownFields(fragment).empty() &&
      equal(canonical(fragment), winner.commit_metadata()),
      "Dataset winner commit metadata is not canonical LanceWriteFragment");
  validateKnownFields(fragment);
  require(fragment.contract_version() == 2 && fragment.dataset_uri() == binding.dataset_uri() &&
      fragment.write_id() == binding.write_id() && fragment.operation() == binding.operation() &&
      fragment.read_version() == binding.expected_read_version() &&
      equal(fragment.schema_fence_sha256(), binding.schema_fence_sha256().digest()) &&
      equal(canonical(fragment.task_attempt()), canonical(winner.task_attempt())),
      "Dataset winner metadata differs from its allocation or exact attempt");
  const auto prefix = binding.allocation_staging_prefix() +
      "/attempt-" + hex(sha256(canonical(winner.task_attempt())));
  const auto& transaction = fragment.transaction();
  require(checksum(transaction.sha256()) && transaction.size_bytes() > 0 &&
      transaction.size_bytes() <= kMaxTransactionBytes &&
      transaction.tenant_id() == request.security().tenant_id() &&
      transaction.generation() == binding.generation() && transaction.write_id() == binding.write_id() &&
      equal(canonical(transaction.task_attempt()), canonical(winner.task_attempt())) &&
      transaction.relative_object_key() == prefix +
          "/transaction-" + hex(transaction.sha256().digest()) + ".pb",
      "Dataset transaction reference violates its tenant/generation/attempt namespace");
  return prefix;
}

std::vector<std::string> validatePublication(const proto::FinalizeDistributedWriteRequest& request) {
  validateKnownFields(request);
  validateSecurity(request.security());
  validateOwner(request.execution_owner());
  validateOwner(request.intent_execution_owner());
  const auto& authority = request.execution_owner();
  const auto& intent_owner = request.intent_execution_owner();
  require(authority.fencing_token() >= intent_owner.fencing_token() &&
      (authority.fencing_token() != intent_owner.fencing_token() ||
       equal(canonical(authority), canonical(intent_owner))),
      "Dataset current execution authority is older than or conflicts with the durable intent");
  validateDeadline(request.deadline_epoch_ms());
  require(request.has_output_binding() && request.output_binding().has_lance() &&
      text(request.execution_id()) && text(request.commit_fingerprint()) && request.control_epoch() > 0 &&
      request.ByteSizeLong() <= kMaxMetadataBytes && request.fragments_size() > 0 &&
      request.fragments_size() <= kMaxFragments, "Dataset publication request is incomplete or oversized");
  validateAllocation(request.output_binding().lance());
  require((request.known_published_version() == 0 && !request.has_known_manifest_sha256()) ||
      (request.recovering() && request.known_published_version() ==
          request.output_binding().lance().expected_read_version() + 1 &&
       checksum(request.known_manifest_sha256())),
      "Dataset known publication hint requires recovery, an exact version and SHA-256");
  std::set<std::pair<int32_t, int32_t>> partitions;
  std::unordered_set<std::string> tasks;
  std::vector<std::string> prefixes;
  prefixes.reserve(request.fragments_size());
  uint64_t plan_epoch = 0;
  for (const auto& winner : request.fragments()) {
    const auto& attempt = winner.task_attempt();
    require(partitions.emplace(attempt.stage_id(), attempt.partition_id()).second &&
        tasks.emplace(attempt.task_id()).second &&
        (plan_epoch == 0 || attempt.plan_epoch() == plan_epoch),
        "Dataset publication repeats a winner or mixes plan epochs");
    plan_epoch = attempt.plan_epoch();
    prefixes.push_back(validateFragment(winner, request));
  }
  return prefixes;
}

void ensureRuntime() {
  if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION ||
      lance_ffi_runtime_init() != 0) {
    throw std::runtime_error("Dataset control Lance runtime/ABI initialization failed");
  }
}

void consumeNames(const substrait::Type& type, const substrait::NamedStruct& schema,
                  int& position, unsigned depth) {
  require(depth <= 64, "Dataset schema nesting exceeds 64 levels");
  if (type.has_struct_()) {
    std::unordered_set<std::string> names;
    for (const auto& child : type.struct_().types()) {
      require(position < schema.names_size() && text(schema.names(position)) &&
          names.emplace(schema.names(position)).second,
          "Dataset schema must contain exact unique flattened field names");
      ++position;
      consumeNames(child, schema, position, depth + 1);
    }
  } else if (type.has_list()) {
    consumeNames(type.list().type(), schema, position, depth + 1);
  } else if (type.has_map()) {
    consumeNames(type.map().key(), schema, position, depth + 1);
    consumeNames(type.map().value(), schema, position, depth + 1);
  }
}

void validateStorageVector(const proto::LogicalType& logical) {
  const auto& physical = logical.substrait_type();
  require(physical.has_list() && logical.constraints_size() == 2 &&
      physical.list().nullability() == substrait::Type_Nullability_NULLABILITY_NULLABLE,
      "Dataset VECTOR physical list and constraints are incomplete");
  const auto& element = physical.list().type();
  const bool is_double = element.has_fp64();
  require((is_double && element.fp64().nullability() == substrait::Type_Nullability_NULLABILITY_REQUIRED) ||
      (element.has_fp32() && element.fp32().nullability() == substrait::Type_Nullability_NULLABILITY_REQUIRED),
      "Dataset VECTOR requires FLOAT32 or FLOAT64 elements");
  uint32_t dimension = 0;
  const auto& sql = logical.canonical_sql();
  const auto end = sql.find_first_of(",)", 7);
  require(end != std::string::npos, "Dataset VECTOR canonical dimension is missing");
  const auto [parsed, error] = std::from_chars(sql.data() + 7, sql.data() + end, dimension);
  require(error == std::errc{} && parsed == sql.data() + end && dimension > 0 && dimension <= 65'535,
      "Dataset VECTOR dimension is invalid");
  const auto canonical_sql = "VECTOR(" + std::to_string(dimension) + (is_double ? ",DOUBLE)" : ")");
  require(sql == canonical_sql, "Dataset VECTOR SQL spelling differs from its element precision");
  bool has_dimension = false;
  bool has_element = false;
  for (const auto& constraint : logical.constraints()) {
    if (constraint.name() == "dimension" && constraint.integer_value() == dimension && !has_dimension) {
      has_dimension = true;
    } else if (constraint.name() == "element_type" &&
               constraint.string_value() == (is_double ? "FLOAT64" : "FLOAT32") && !has_element) {
      has_element = true;
    } else {
      throw std::invalid_argument("Dataset VECTOR contains inconsistent or duplicate constraints");
    }
  }
  require(has_dimension && has_element, "Dataset VECTOR dimension or element constraint is missing");
}
}  // namespace

void LanceDatasetPublication::validateSchemaRequest(const proto::DatasetSchemaFingerprintRequest& request) {
  validateKnownFields(request);
  validateCatalog(request.catalog());
  validateSecurity(request.security());
  validateDeadline(request.deadline_epoch_ms());
  const auto& schema = request.schema();
  require(request.ByteSizeLong() <= kMaxMetadataBytes && schema.has_struct_() &&
      schema.struct_().types_size() > 0 && schema.struct_().types_size() <= 4096 &&
      schema.names_size() <= 65536 && request.column_types_size() == schema.struct_().types_size(),
      "Dataset schema requires bounded physical and logical column types");
  substrait::Type root;
  *root.mutable_struct_() = schema.struct_();
  int position = 0;
  consumeNames(root, schema, position, 0);
  require(position == schema.names_size(), "Dataset schema contains unused flattened field names");
  for (int index = 0; index < request.column_types_size(); ++index) {
    const auto& logical = request.column_types(index);
    require(text(logical.canonical_sql()) && logical.has_substrait_type() &&
        equal(canonical(logical.substrait_type()), canonical(schema.struct_().types(index))),
        "Dataset logical column type differs from its physical schema");
    if (logical.canonical_sql().starts_with("VECTOR(")) {
      validateStorageVector(logical);
    }
  }
}

std::array<uint8_t, 32> LanceDatasetPublication::schemaFingerprint(const ArrowSchema& schema) {
  ensureRuntime();
  std::array<uint8_t, 32> result{};
  if (lance_ffi_schema_fingerprint(&schema, result.data(), result.size()) != 0) {
    const char* error = lance_ffi_last_error();
    throw std::runtime_error(std::string("Dataset physical schema fingerprint failed: ") +
        (error ? error : "no FFI diagnostic"));
  }
  return result;
}

proto::FinalizeDistributedWriteResult LanceDatasetPublication::finalizeWrite(
    const proto::FinalizeDistributedWriteRequest& request, const std::string& storageBinding) {
  auto prefixes = validatePublication(request);
  ensureRuntime();
  const auto& binding = request.output_binding().lance();
  std::vector<LanceCCommitFragment> fragments;
  fragments.reserve(prefixes.size());
  for (int index = 0; index < request.fragments_size(); ++index) {
    const auto& bytes = request.fragments(index).commit_metadata();
    fragments.push_back({reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(),
                         prefixes[static_cast<size_t>(index)].c_str()});
  }
  validateDeadline(request.deadline_epoch_ms());
  uint64_t version = 0;
  std::array<uint8_t, 32> manifest{};
  const auto& schema = binding.schema_fence_sha256().digest();
  LanceStorageContext storage(storageBinding);
  const int32_t rc = lance_ffi_commit_fragments(storage.get(),
      binding.dataset_uri().c_str(), binding.write_id().c_str(), request.security().tenant_id().c_str(),
      binding.generation().c_str(), static_cast<int32_t>(binding.operation()), binding.expected_read_version(),
      reinterpret_cast<const uint8_t*>(schema.data()), schema.size(), fragments.data(), fragments.size(),
      8, request.recovering() ? 1 : 0, request.known_published_version(), request.deadline_epoch_ms(),
      &version, manifest.data(), manifest.size());
  proto::FinalizeDistributedWriteResult result;
  result.set_commit_fingerprint(request.commit_fingerprint());
  const std::string digest(reinterpret_cast<const char*>(manifest.data()), manifest.size());
  const bool exact_hint = request.known_published_version() == 0 ||
      (version == request.known_published_version() && equal(digest, request.known_manifest_sha256().digest()));
  if (rc != 0 || version != binding.expected_read_version() + 1 || !exact_hint ||
      std::all_of(manifest.begin(), manifest.end(), [](uint8_t byte) { return byte == 0; })) {
    const char* error = rc == 0 ? nullptr : lance_ffi_last_error();
    PANGROVA_LOG(ERROR) << "event=dataset_publication_outcome_unknown execution_id=" << request.execution_id()
        << " control_epoch=" << request.control_epoch() << " ffi_code=" << rc
        << " diagnostic=" << (error ? error : "publication evidence mismatch");
    result.set_outcome(proto::WRITE_PUBLICATION_OUTCOME_UNKNOWN);
    return result;
  }
  result.set_outcome(proto::WRITE_PUBLICATION_OUTCOME_COMMITTED);
  result.set_published_version(version);
  result.mutable_manifest_sha256()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  result.mutable_manifest_sha256()->set_digest(digest);
  return result;
}

proto::DatasetScanPartitionsResult LanceDatasetPublication::scanPartitions(
    const proto::DatasetScanPartitionsRequest& request, const std::string& storageBinding) {
  validateKnownFields(request);
  validateCatalog(request.catalog());
  validateSecurity(request.security());
  validateDeadline(request.deadline_epoch_ms());
  require(text(request.dataset_uri(), 8192) && request.version() > 0 &&
      request.page_size() > 0 && request.page_size() <= 256 && checksum(request.physical_schema_sha256()),
      "Dataset scan requires a bounded immutable manifest partition request");
  ensureRuntime();
  LanceStorageContext storage(storageBinding);
  const auto bytes = canonical(request);
  LanceCBuffer page{};
  const auto rc = lance_ffi_scan_partitions(storage.get(), reinterpret_cast<const uint8_t*>(bytes.data()),
                                          bytes.size(), &page);
  if (rc != 0) {
    const char* error = lance_ffi_last_error();
    throw std::runtime_error(std::string("Dataset partition enumeration failed: ") +
                             (error ? error : "no FFI diagnostic"));
  }
  proto::DatasetScanPartitionsResult result;
  const bool parsed = page.len <= kMaxMetadataBytes && result.ParseFromArray(page.data, page.len);
  lance_ffi_buffer_free(page);
  if (!parsed || result.partitions_size() > static_cast<int>(request.page_size()) ||
      result.version() != request.version() || !checksum(result.manifest_sha256()) ||
      canonical(result.physical_schema_sha256()) != canonical(request.physical_schema_sha256())) {
    throw std::runtime_error("Dataset partition result violates the bounded manifest contract");
  }
  return result;
}

}  // namespace pangrova::lance
