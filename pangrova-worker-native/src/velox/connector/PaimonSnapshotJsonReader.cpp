#include "velox/connector/PaimonSnapshotJsonReader.h"

#include <stdexcept>
#include <optional>
#include <algorithm>
#include <cctype>
#include <iomanip>
#include <limits>
#include <sstream>
#include <vector>

#include <openssl/evp.h>

#include "simdjson.h"
#include "velox/common/file/File.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kMaxSnapshotBytes = 64 * 1024 * 1024;

int64_t requiredLong(const simdjson::dom::element& object, const char* key) {
  auto field = object[key];
  if (field.error()) {
    throw std::runtime_error(std::string("Missing Paimon snapshot field: ") + key);
  }
  auto value = field.get_int64();
  if (value.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot integer: ") + key);
  }
  return value.value();
}

std::string requiredString(
    const simdjson::dom::element& object, const char* key) {
  auto field = object[key];
  if (field.error()) {
    throw std::runtime_error(std::string("Missing Paimon snapshot field: ") + key);
  }
  auto value = field.get_string();
  if (value.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot string: ") + key);
  }
  return std::string(value.value());
}

std::optional<int64_t> optionalLong(
    const simdjson::dom::element& object, const char* key) {
  auto field = object[key];
  if (field.error() == simdjson::NO_SUCH_FIELD) return std::nullopt;
  if (field.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot field: ") + key);
  }
  if (field.value().is_null()) return std::nullopt;
  auto value = field.get_int64();
  if (value.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot integer: ") + key);
  }
  return value.value();
}

std::optional<std::string> optionalString(
    const simdjson::dom::element& object, const char* key) {
  auto field = object[key];
  if (field.error() == simdjson::NO_SUCH_FIELD) return std::nullopt;
  if (field.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot field: ") + key);
  }
  if (field.value().is_null()) return std::nullopt;
  auto value = field.get_string();
  if (value.error()) {
    throw std::runtime_error(std::string("Invalid Paimon snapshot string: ") + key);
  }
  return std::string(value.value());
}

std::optional<facebook::velox::Timestamp> optionalTagCreationTime(
    const simdjson::dom::element& object) {
  auto field = object["tagCreateTime"];
  if (field.error() == simdjson::NO_SUCH_FIELD) return std::nullopt;
  if (field.error()) throw std::runtime_error("Invalid Paimon tag creation time");
  if (field.value().is_null()) return std::nullopt;
  auto array = field.get_array();
  if (array.error()) throw std::runtime_error("Invalid Paimon tag creation time");
  std::vector<int64_t> parts;
  for (auto part : array.value()) {
    auto number = part.get_int64();
    if (number.error()) throw std::runtime_error("Invalid Paimon tag creation time");
    parts.push_back(number.value());
  }
  if ((parts.size() != 6 && parts.size() != 7) ||
      (parts.size() == 7 && (parts[6] < 0 || parts[6] >= 1000000000))) {
    throw std::runtime_error("Invalid Paimon tag creation time");
  }
  std::ostringstream iso;
  iso << std::setfill('0') << std::setw(4) << parts[0] << '-'
      << std::setw(2) << parts[1] << '-' << std::setw(2) << parts[2]
      << 'T' << std::setw(2) << parts[3] << ':' << std::setw(2)
      << parts[4] << ':' << std::setw(2) << parts[5] << '.'
      << std::setw(9) << (parts.size() == 7 ? parts[6] : 0);
  const auto timestampText = iso.str();
  auto parsed = facebook::velox::util::fromTimestampString(
      facebook::velox::StringView(timestampText),
      facebook::velox::util::TimestampParseMode::kIso8601);
  if (!parsed.hasValue()) throw std::runtime_error("Invalid Paimon tag creation time");
  return parsed.value();
}

std::optional<std::string> optionalTagRetention(
    const simdjson::dom::element& object,
    simdjson::padded_string& padded) {
  auto field = object["tagTimeRetained"];
  if (field.error() == simdjson::NO_SUCH_FIELD) return std::nullopt;
  if (field.error()) throw std::runtime_error("Invalid Paimon tag retention");
  if (field.value().is_null()) return std::nullopt;
  // Jackson serializes java.time.Duration as seconds with nine fractional digits.
  simdjson::ondemand::parser parser;
  auto document = parser.iterate(padded);
  if (document.error()) throw std::runtime_error("Invalid Paimon tag retention");
  auto token = document["tagTimeRetained"].raw_json_token();
  if (token.error()) throw std::runtime_error("Invalid Paimon tag retention");
  auto raw = token.value();
  while (!raw.empty() && std::isspace(static_cast<unsigned char>(raw.back()))) {
    raw.remove_suffix(1);
  }
  const auto dot = raw.find('.');
  const bool negative = !raw.empty() && raw.front() == '-';
  if (raw.empty() || (negative && raw.size() == 1) ||
      (dot != std::string_view::npos && raw.find('.', dot + 1) != std::string_view::npos)) {
    throw std::runtime_error("Invalid Paimon tag retention");
  }
  const auto whole = raw.substr(negative ? 1 : 0,
      (dot == std::string_view::npos ? raw.size() : dot) - (negative ? 1 : 0));
  const auto fraction = dot == std::string_view::npos ? std::string_view{} : raw.substr(dot + 1);
  if (whole.empty() || fraction.size() > 9 ||
      (dot != std::string_view::npos && fraction.empty()) ||
      !std::all_of(whole.begin(), whole.end(), ::isdigit) ||
      !std::all_of(fraction.begin(), fraction.end(), ::isdigit)) {
    throw std::runtime_error("Invalid Paimon tag retention");
  }
  uint64_t seconds;
  try {
    seconds = std::stoull(std::string(whole));
  } catch (const std::exception&) {
    throw std::runtime_error("Invalid Paimon tag retention");
  }
  if (seconds > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    throw std::runtime_error("Invalid Paimon tag retention");
  }
  std::string fractional(fraction);
  while (!fractional.empty() && fractional.back() == '0') fractional.pop_back();
  const int64_t effectiveSeconds = negative && (seconds != 0 || !fractional.empty())
      ? -static_cast<int64_t>(seconds) : static_cast<int64_t>(seconds);
  std::string result = "PT";
  if (const auto hours = effectiveSeconds / 3600; hours != 0) {
    result += std::to_string(hours) + "H";
  }
  if (const auto minutes = effectiveSeconds % 3600 / 60; minutes != 0) {
    result += std::to_string(minutes) + "M";
  }
  const auto remainder = effectiveSeconds % 60;
  if (remainder != 0 || !fractional.empty() || result == "PT") {
    result += negative && !fractional.empty() && remainder == 0
        ? "-0" : std::to_string(remainder);
    if (!fractional.empty()) result += "." + fractional;
    result += "S";
  }
  return result;
}

facebook::velox::RowVectorPtr schemaRow(const simdjson::dom::element& object,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::memory::MemoryPool* pool, int64_t schemaId) {
  auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(outputType, 1, pool));
  for (size_t column = 0; column < outputType->size(); ++column) {
    const auto& name = outputType->nameOf(column);
    auto child = vector->childAt(column);
    if (name == "schema_id") {
      child->asFlatVector<int64_t>()->set(0, schemaId);
    } else if (name == "update_time") {
      child->asFlatVector<facebook::velox::Timestamp>()->set(0,
          facebook::velox::Timestamp::fromMillis(requiredLong(object, "timeMillis")));
    } else if (name == "comment") {
      auto value = optionalString(object, "comment");
      if (value) {
        child->asFlatVector<facebook::velox::StringView>()->set(
            0, facebook::velox::StringView(*value));
      } else {
        child->setNull(0, true);
      }
    } else {
      const char* key = name == "partition_keys" ? "partitionKeys" :
          name == "primary_keys" ? "primaryKeys" : name.c_str();
      auto field = object[key];
      if (field.error() || field.value().is_null()) {
        throw std::runtime_error("Missing Paimon schema field: " + name);
      }
      auto value = simdjson::minify(field.value());
      child->asFlatVector<facebook::velox::StringView>()->set(
          0, facebook::velox::StringView(value));
    }
  }
  return vector;
}

}  // namespace

PaimonSnapshotJsonReader::PaimonSnapshotJsonReader(const PaimonSnapshotReadUnit& unit,
    facebook::velox::RowTypePtr outputType, facebook::velox::memory::MemoryPool* pool,
    const MetadataReadContext& reads)
    : reads_(reads), output_type_(std::move(outputType)), pool_(pool),
      expected_length_(unit.object.length), expected_snapshot_id_(unit.snapshotId),
      expected_sha256_(unit.object.sha256), tag_name_(unit.tagName),
      expected_schema_id_(unit.schemaId), schema_history_(unit.schemaHistory) {
  if (expected_length_ == 0 || expected_length_ > kMaxSnapshotBytes ||
      (expected_snapshot_id_ == 0 && tag_name_.empty() && !expected_schema_id_) ||
      (expected_snapshot_id_ != 0 && !tag_name_.empty()) ||
      (expected_schema_id_ && (expected_snapshot_id_ != 0 || !tag_name_.empty())) ||
      (schema_history_ && !expected_schema_id_) ||
      expected_sha256_.size() != 32 ||
      !output_type_ || !pool_) {
    throw std::invalid_argument("Invalid pinned Paimon snapshot reader input");
  }
  file_ = reads_.open(unit.object.uri);
  if (file_->size() != expected_length_) {
    throw std::runtime_error("Pinned Paimon snapshot object length changed");
  }
  validateOutputSchema();
}

void PaimonSnapshotJsonReader::validateOutputSchema() const {
  for (size_t i = 0; i < output_type_->size(); ++i) {
    const auto& name = output_type_->nameOf(i);
    const auto type = output_type_->childAt(i)->kind();
    if (schema_history_) {
      const bool numeric = name == "schema_id";
      const bool timestamp = name == "update_time";
      const bool textual = name == "fields" || name == "partition_keys" ||
          name == "primary_keys" || name == "options" || name == "comment";
      if (!((numeric && type == facebook::velox::TypeKind::BIGINT) ||
            (timestamp && type == facebook::velox::TypeKind::TIMESTAMP) ||
            (textual && type == facebook::velox::TypeKind::VARCHAR))) {
        throw std::runtime_error("Invalid Paimon schemas output schema: " + name);
      }
      continue;
    }
    if (expected_schema_id_) {
      if ((name != "key" && name != "value") ||
          type != facebook::velox::TypeKind::VARCHAR) {
        throw std::runtime_error("Invalid Paimon options output schema: " + name);
      }
      continue;
    }
    if (!tag_name_.empty()) {
      const bool numeric = name == "snapshot_id" || name == "schema_id" ||
          name == "record_count";
      const bool textual = name == "tag_name" || name == "time_retained";
      const bool timestamp = name == "commit_time" || name == "create_time";
      if (!((numeric && type == facebook::velox::TypeKind::BIGINT) ||
            (textual && type == facebook::velox::TypeKind::VARCHAR) ||
            (timestamp && type == facebook::velox::TypeKind::TIMESTAMP))) {
        throw std::runtime_error("Invalid Paimon tags output schema: " + name);
      }
      continue;
    }
    const bool numeric = name == "snapshot_id" || name == "schema_id" ||
        name == "commit_identifier" || name == "total_record_count" ||
        name == "delta_record_count" || name == "changelog_record_count" ||
        name == "watermark" || name == "next_row_id";
    const bool textual = name == "commit_user" || name == "commit_kind" ||
        name == "base_manifest_list" || name == "delta_manifest_list" ||
        name == "changelog_manifest_list";
    if (!((numeric && type == facebook::velox::TypeKind::BIGINT) ||
          (textual && type == facebook::velox::TypeKind::VARCHAR) ||
          (name == "commit_time" &&
           type == facebook::velox::TypeKind::TIMESTAMP))) {
      throw std::runtime_error("Invalid Paimon snapshots output schema: " + name);
    }
  }
}

facebook::velox::RowVectorPtr PaimonSnapshotJsonReader::next(uint64_t maxRows) {
  reads_.checkpoint();
  if (maxRows == 0) {
    throw std::invalid_argument("Metadata scan batch size must be positive");
  }
  if (expected_schema_id_ && !schema_history_ && options_loaded_) {
    return nextOptions(maxRows);
  }
  if (emitted_) {
    return nullptr;
  }
  std::string json(expected_length_, '\0');
  file_->pread(0, expected_length_, json.data());
  std::string actualSha256(32, '\0');
  unsigned int digestSize = 0;
  if (EVP_Digest(json.data(), json.size(),
          reinterpret_cast<unsigned char*>(actualSha256.data()),
          &digestSize, EVP_sha256(), nullptr) != 1 || digestSize != 32 ||
      actualSha256 != expected_sha256_) {
    throw std::runtime_error("Pinned Paimon snapshot object checksum changed");
  }
  simdjson::padded_string padded(json);
  simdjson::dom::parser parser;
  auto parsed = parser.parse(padded);
  if (parsed.error() || !parsed.value().is_object()) {
    throw std::runtime_error("Invalid Paimon snapshot JSON object");
  }
  const auto object = parsed.value();
  if (expected_schema_id_) {
    const auto schemaId = requiredLong(object, "id");
    if (schemaId < 0 || static_cast<uint64_t>(schemaId) != *expected_schema_id_) {
      throw std::runtime_error("Pinned Paimon schema ID differs from JSON object");
    }
    if (schema_history_) {
      auto row = schemaRow(object, output_type_, pool_, schemaId);
      emitted_ = true;
      return row;
    }
    auto field = object["options"];
    if (field.error()) throw std::runtime_error("Missing Paimon schema options");
    auto entries = field.get_object();
    if (entries.error()) throw std::runtime_error("Invalid Paimon schema options");
    for (auto entry : entries.value()) {
      auto value = entry.value.get_string();
      if (value.error()) throw std::runtime_error("Invalid Paimon option value");
      options_.emplace_back(std::string(entry.key), std::string(value.value()));
    }
    std::sort(options_.begin(), options_.end());
    options_loaded_ = true;
    return nextOptions(maxRows);
  }
  const auto snapshotId = requiredLong(object, "id");
  if (snapshotId <= 0 || (expected_snapshot_id_ != 0 &&
      static_cast<uint64_t>(snapshotId) != expected_snapshot_id_)) {
    throw std::runtime_error("Pinned Paimon snapshot ID differs from JSON object");
  }
  auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(output_type_, 1, pool_));
  if (!tag_name_.empty()) {
    for (size_t column = 0; column < output_type_->size(); ++column) {
      const auto& name = output_type_->nameOf(column);
      auto child = vector->childAt(column);
      if (name == "tag_name" || name == "time_retained") {
        auto value = name == "tag_name" ? std::optional<std::string>(tag_name_) :
            optionalTagRetention(object, padded);
        if (value) {
          child->asFlatVector<facebook::velox::StringView>()->set(
              0, facebook::velox::StringView(*value));
        } else {
          child->setNull(0, true);
        }
      } else if (name == "commit_time") {
        child->asFlatVector<facebook::velox::Timestamp>()->set(0,
            facebook::velox::Timestamp::fromMillis(requiredLong(object, "timeMillis")));
      } else if (name == "create_time") {
        auto value = optionalTagCreationTime(object);
        if (value) {
          child->asFlatVector<facebook::velox::Timestamp>()->set(0, *value);
        } else {
          child->setNull(0, true);
        }
      } else {
        auto value = name == "snapshot_id" ? std::optional<int64_t>(snapshotId) :
            name == "schema_id" ?
                std::optional<int64_t>(requiredLong(object, "schemaId")) :
                optionalLong(object, "totalRecordCount");
        if (value) {
          child->asFlatVector<int64_t>()->set(0, *value);
        } else {
          child->setNull(0, true);
        }
      }
    }
    emitted_ = true;
    return vector;
  }
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    auto child = vector->childAt(column);
    if (name == "commit_time") {
      child->asFlatVector<facebook::velox::Timestamp>()->set(0,
          facebook::velox::Timestamp::fromMillis(requiredLong(object, "timeMillis")));
      continue;
    }
    if (name == "commit_user" || name == "commit_kind" ||
        name == "base_manifest_list" || name == "delta_manifest_list" ||
        name == "changelog_manifest_list") {
      const char* key = name == "commit_user" ? "commitUser" :
          name == "commit_kind" ? "commitKind" :
          name == "base_manifest_list" ? "baseManifestList" :
          name == "delta_manifest_list" ? "deltaManifestList" : "changelogManifestList";
      auto value = name == "changelog_manifest_list" ? optionalString(object, key) :
          std::optional<std::string>(requiredString(object, key));
      if (value) {
        child->asFlatVector<facebook::velox::StringView>()->set(
            0, facebook::velox::StringView(*value));
      } else {
        child->setNull(0, true);
      }
      continue;
    }
    const char* key = name == "schema_id" ? "schemaId" :
        name == "commit_identifier" ? "commitIdentifier" :
        name == "total_record_count" ? "totalRecordCount" :
        name == "delta_record_count" ? "deltaRecordCount" :
        name == "changelog_record_count" ? "changelogRecordCount" :
        name == "watermark" ? "watermark" : "nextRowId";
    auto value = name == "snapshot_id" ? std::optional<int64_t>(snapshotId) :
        name == "schema_id" || name == "commit_identifier" ?
            std::optional<int64_t>(requiredLong(object, key)) : optionalLong(object, key);
    if (value) {
      child->asFlatVector<int64_t>()->set(0, *value);
    } else {
      child->setNull(0, true);
    }
  }
  emitted_ = true;
  return vector;
}

facebook::velox::RowVectorPtr PaimonSnapshotJsonReader::nextOptions(uint64_t maxRows) {
  if (option_cursor_ == options_.size()) return nullptr;
  const auto rows = std::min<uint64_t>(maxRows, options_.size() - option_cursor_);
  auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(output_type_, rows, pool_));
  for (size_t column = 0; column < output_type_->size(); ++column) {
    auto child = vector->childAt(column)->asFlatVector<facebook::velox::StringView>();
    const bool key = output_type_->nameOf(column) == "key";
    for (uint64_t row = 0; row < rows; ++row) {
      const auto& option = options_[option_cursor_ + row];
      const auto& value = key ? option.first : option.second;
      child->set(row, facebook::velox::StringView(value));
    }
  }
  option_cursor_ += rows;
  return vector;
}

}  // namespace pangrova::velox
