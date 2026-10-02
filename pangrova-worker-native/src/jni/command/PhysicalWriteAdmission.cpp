#include "jni/command/PhysicalWriteAdmission.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <array>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>

namespace pangrova::jni {
namespace {
namespace proto = protocol::v2;

std::string bytes(const google::protobuf::Message& message) {
  if (message.ByteSizeLong() == 0 || message.ByteSizeLong() > 64 * 1024 * 1024) {
    throw std::invalid_argument("Physical write metadata exceeds its canonical message bound");
  }
  std::string value(message.ByteSizeLong(), '\0');
  google::protobuf::io::ArrayOutputStream output(value.data(), static_cast<int>(value.size()));
  google::protobuf::io::CodedOutputStream coded(&output);
  coded.SetSerializationDeterministic(true);
  if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
    throw std::runtime_error("Cannot serialize canonical physical write metadata");
  }
  return value;
}

std::string digest(const std::string& value) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> output{};
  unsigned int length = 0;
  if (EVP_Digest(value.data(), value.size(), output.data(), &length, EVP_sha256(), nullptr) != 1 || length != 32) {
    throw std::runtime_error("Cannot compute physical metadata SHA-256");
  }
  return {reinterpret_cast<const char*>(output.data()), length};
}

void checksum(const proto::Checksum& expected, const std::string& value) {
  const auto actual = digest(value);
  if (expected.algorithm() != proto::Checksum::ALGORITHM_SHA256 || expected.digest().size() != actual.size() ||
      CRYPTO_memcmp(expected.digest().data(), actual.data(), actual.size()) != 0) {
    throw std::invalid_argument("Physical write metadata checksum differs from its allocation");
  }
}

std::string hex(const std::string& value) {
  constexpr char alphabet[] = "0123456789abcdef";
  std::string output;
  output.reserve(value.size() * 2);
  for (unsigned char character : value) {
    output += alphabet[character >> 4];
    output += alphabet[character & 15];
  }
  return output;
}

void validateIdentity(const proto::PhysicalFileWriteBinding& binding) {
  const auto& catalog = binding.catalog();
  const auto& handle = binding.output_handle();
  if (!binding.has_catalog() || !binding.has_output_handle() || catalog.catalog_name().empty() ||
      catalog.connector_provider().empty() || catalog.catalog_incarnation_id().empty() || catalog.catalog_revision() == 0 ||
      catalog.configuration_fingerprint().algorithm() != proto::Checksum::ALGORITHM_SHA256 ||
      catalog.configuration_fingerprint().digest().size() != 32 || handle.type_url().empty() || handle.codec_version() == 0 ||
      handle.payload().empty() || handle.payload().size() > 16 * 1024 * 1024) {
    throw std::invalid_argument("Physical output requires its exact catalog and persisted handle authority");
  }
  checksum(handle.payload_checksum(), handle.payload());
  if (binding.write_id().empty() || binding.generation().empty() || binding.base_snapshot_id() < -1 ||
      binding.schema_id() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
      binding.physical_schema().empty() || binding.physical_schema().size() > 16 * 1024 * 1024 ||
      binding.columns_size() == 0 || binding.columns_size() > 4096 || binding.columns_size() != handle.write_column_names_size()) {
    throw std::invalid_argument("Physical output has an incomplete write/schema identity");
  }
  checksum(binding.physical_schema_checksum(), binding.physical_schema());
  std::set<int32_t> ids;
  std::set<std::string> names;
  for (int index = 0; index < binding.columns_size(); ++index) {
    const auto& column = binding.columns(index);
    if (column.field_id() < 0 || !ids.insert(column.field_id()).second || column.name().empty() ||
        !names.insert(column.name()).second || column.name() != handle.write_column_names(index) || !column.has_logical_type()) {
      throw std::invalid_argument("Physical output columns differ from ordered persisted handle identities");
    }
  }
}

void validatePolicy(const proto::PhysicalFileWriteBinding& binding, const proto::WriteTaskOptions& options) {
  const auto& policy = binding.policy();
  if (!binding.has_policy() || binding.file_format() != options.file_format() || policy.write_mode() != options.write_mode() ||
      binding.file_format() != proto::FILE_FORMAT_LANCE || policy.write_mode() != "INSERT" || policy.bucket_count() != 1 ||
      policy.partition_field_ids_size() != 0 || policy.primary_key_field_ids_size() != 0 ||
      options.partition_spec_size() != 0 || policy.writer_buffer_hint_bytes() == 0 ||
      policy.writer_buffer_hint_bytes() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    throw std::invalid_argument("Physical writer requires its supported allocated append policy and explicit buffer hint");
  }
  auto declaration = policy;
  declaration.clear_declaration_checksum();
  checksum(policy.declaration_checksum(), bytes(declaration));
}

void validateFiles(const proto::PhysicalFileWriteBinding& binding, const proto::TaskAttemptId& attempt) {
  const auto suffix = "attempt-" + hex(digest(bytes(attempt)));
  if (binding.files_size() != 1 || binding.attempt_prefix() != binding.allocation_prefix() + "/" + suffix) {
    throw std::invalid_argument("Physical file allocation is not bound to the complete canonical attempt");
  }
  const auto& file = binding.files(0);
  if (!file.has_task_attempt() || bytes(file.task_attempt()) != bytes(attempt) || file.bucket_id() != 0 || file.file_ordinal() != 0 ||
      file.partition_size() != 0 || file.restored_max_sequence() < -1 || file.file_source() != "APPEND" ||
      file.file_name().find_first_of("/\\") != std::string::npos || !file.file_name().ends_with("." + suffix + ".lance") ||
      file.opaque_location() != binding.allocation_prefix() + "/" + file.file_name() ||
      file.restored_file_set_checksum().algorithm() != proto::Checksum::ALGORITHM_SHA256 ||
      file.restored_file_set_checksum().digest().size() != 32) {
    throw std::invalid_argument("Physical file path/source provenance differs from its sealed SDK allocation");
  }
  auto allocation = file;
  allocation.clear_allocation_checksum();
  checksum(file.allocation_checksum(), bytes(allocation));
}
}  // namespace

executor::WriteConfigInfo parsePhysicalWriteOptions(
    const protocol::v2::WriteTaskOptions& options, const protocol::v2::TaskAttemptId& attempt) {
  const auto& binding = options.output_binding().physical();
  validateIdentity(binding);
  validatePolicy(binding, options);
  validateFiles(binding, attempt);
  executor::WriteConfigInfo config;
  config.target_table = options.target_table();
  config.write_mode = options.write_mode();
  config.target_file_size = options.target_file_size();
  config.file_format = split::FileFormatType::LANCE;
  executor::WriteConfigInfo::PhysicalOutputBinding output;
  output.binding = binding;
  const auto canonical = bytes(attempt);
  output.task_attempt.assign(canonical.begin(), canonical.end());
  config.physical_output = std::move(output);
  return config;
}
}  // namespace pangrova::jni
