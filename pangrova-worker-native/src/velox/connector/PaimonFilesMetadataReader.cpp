#include "velox/connector/PaimonFilesMetadataReader.h"

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <unordered_set>

#include "folly/json.h"
#include "openssl/crypto.h"
#include "openssl/evp.h"
#include "velox/common/file/File.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {
using namespace facebook::velox;
constexpr uint64_t kMaxMetadataBytes = 64 * 1024 * 1024;
constexpr uint64_t kMaxBatchRows = 1024;

const folly::dynamic& optional(const folly::dynamic& object, const char* name) {
  static const folly::dynamic null;
  if (!object.isObject()) throw std::runtime_error("Invalid Paimon metadata record");
  auto it = object.find(name);
  return it == object.items().end() ? null : it->second;
}

int64_t number(const folly::dynamic& object, const char* name) {
  const auto& value = optional(object, name);
  if (!value.isInt()) throw std::runtime_error(std::string("Invalid Paimon integer: ") + name);
  return value.asInt();
}

const std::string& text(const folly::dynamic& object, const char* name) {
  const auto& value = optional(object, name);
  if (!value.isString()) throw std::runtime_error(std::string("Invalid Paimon text: ") + name);
  return value.getString();
}

void validateSnapshot(const protocol::v2::PinnedMetadataObject& object, uint64_t id,
    const MetadataReadContext& reads) {
  if (object.uri().empty() || object.length() == 0 || object.length() > kMaxMetadataBytes
      || object.sha256().size() != 32) throw std::runtime_error("Invalid pinned Paimon snapshot");
  auto file = reads.open(object.uri());
  if (file->size() != object.length()) throw std::runtime_error("Pinned Paimon snapshot size changed");
  std::string bytes(object.length(), '\0');
  file->pread(0, bytes.size(), bytes.data());
  std::array<unsigned char, 32> digest{};
  unsigned int size = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &size, EVP_sha256(), nullptr) != 1
      || size != digest.size() || CRYPTO_memcmp(digest.data(), object.sha256().data(), size) != 0)
    throw std::runtime_error("Pinned Paimon snapshot SHA-256 changed");
  const auto snapshot = folly::parseJson(bytes);
  if (number(snapshot, "id") < 0
      || static_cast<uint64_t>(number(snapshot, "id")) != id)
    throw std::runtime_error("Paimon files snapshot identity differs");
}

std::u16string javaKey(const std::string& utf8) {
  std::u16string result;
  for (size_t index = 0; index < utf8.size();) {
    uint32_t point = static_cast<uint8_t>(utf8[index++]);
    int continuation = 0;
    uint32_t minimum = 0;
    if (point >= 0xc2 && point <= 0xdf) { point &= 0x1f; continuation = 1; minimum = 0x80; }
    else if (point >= 0xe0 && point <= 0xef) { point &= 0xf; continuation = 2; minimum = 0x800; }
    else if (point >= 0xf0 && point <= 0xf4) { point &= 7; continuation = 3; minimum = 0x10000; }
    else if (point >= 0x80) throw std::runtime_error("Invalid UTF-8 Paimon field name");
    for (int count = 0; count < continuation; ++count) {
      if (index == utf8.size()) throw std::runtime_error("Truncated UTF-8 Paimon field name");
      uint8_t byte = utf8[index++];
      if ((byte & 0xc0) != 0x80) throw std::runtime_error("Invalid UTF-8 Paimon field name");
      point = (point << 6) | (byte & 0x3f);
    }
    if (point < minimum || point > 0x10ffff || (point >= 0xd800 && point <= 0xdfff))
      throw std::runtime_error("Invalid Paimon Unicode field name");
    if (point <= 0xffff) result.push_back(static_cast<char16_t>(point));
    else {
      point -= 0x10000;
      result.push_back(static_cast<char16_t>(0xd800 + (point >> 10)));
      result.push_back(static_cast<char16_t>(0xdc00 + (point & 0x3ff)));
    }
  }
  return result;
}

void putString(const VectorPtr& vector, int row, const std::string& value) {
  vector->asFlatVector<StringView>()->set(row, StringView(value));
}

const std::unordered_map<std::string, TypeKind>& columns() {
  static const std::unordered_map<std::string, TypeKind> kinds{
      {"partition", TypeKind::VARCHAR}, {"bucket", TypeKind::INTEGER}, {"file_path", TypeKind::VARCHAR},
      {"file_format", TypeKind::VARCHAR}, {"schema_id", TypeKind::BIGINT}, {"level", TypeKind::INTEGER},
      {"record_count", TypeKind::BIGINT}, {"file_size_in_bytes", TypeKind::BIGINT}, {"min_key", TypeKind::VARCHAR},
      {"max_key", TypeKind::VARCHAR}, {"null_value_counts", TypeKind::VARCHAR}, {"min_value_stats", TypeKind::VARCHAR},
      {"max_value_stats", TypeKind::VARCHAR}, {"min_sequence_number", TypeKind::BIGINT},
      {"max_sequence_number", TypeKind::BIGINT}, {"creation_time", TypeKind::TIMESTAMP},
      {"deleteRowCount", TypeKind::BIGINT}, {"file_source", TypeKind::VARCHAR},
      {"first_row_id", TypeKind::BIGINT}, {"write_cols", TypeKind::ARRAY}};
  return kinds;
}
}  // namespace

PaimonFilesMetadataReader::Schema PaimonFilesMetadataReader::schema(const std::string& json, uint64_t id) {
  auto object = folly::parseJson(json);
  if (number(object, "id") < 0 || static_cast<uint64_t>(number(object, "id")) != id)
    throw std::runtime_error("Paimon file schema identity differs");
  const auto& fields = optional(object, "fields");
  if (!fields.isArray()) throw std::runtime_error("Invalid Paimon file schema fields");
  Schema result;
  std::unordered_set<int64_t> ids;
  std::unordered_set<std::string> names;
  for (const auto& field : fields) {
    auto name = text(field, "name");
    auto fieldId = number(field, "id");
    if (fieldId < 0 || name.empty() || !ids.insert(fieldId).second || !names.insert(name).second)
      throw std::runtime_error("Invalid duplicate Paimon file schema field");
    const auto& declaration = optional(field, "type");
    auto scalar = declaration.isString() ? PaimonScalarType::parse(declaration.asString())
        : PaimonScalarType{PaimonScalarType::Kind::COMPLEX};
    result.fields.push_back({fieldId, name, scalar});
  }
  auto positions = [&](const char* member, bool trimmed) {
    std::vector<int> indices;
    const auto& values = optional(object, member);
    if (values.isNull()) return indices;
    if (!values.isArray()) throw std::runtime_error("Invalid Paimon key declaration");
    for (const auto& value : values) {
      if (!value.isString()) throw std::runtime_error("Invalid Paimon key field name");
      if (trimmed && std::any_of(result.partitions.begin(), result.partitions.end(),
          [&](int index) { return result.fields[index].name == value.asString(); })) continue;
      auto found = std::find_if(result.fields.begin(), result.fields.end(),
          [&](const auto& field) { return field.name == value.asString(); });
      if (found == result.fields.end()) throw std::runtime_error("Paimon key is absent from schema");
      indices.push_back(std::distance(result.fields.begin(), found));
    }
    return indices;
  };
  result.partitions = positions("partitionKeys", false);
  result.keys = positions("primaryKeys", true);
  result.sorted.resize(result.fields.size());
  std::iota(result.sorted.begin(), result.sorted.end(), 0);
  std::sort(result.sorted.begin(), result.sorted.end(), [&](int left, int right) {
    return javaKey(result.fields[left].name) < javaKey(result.fields[right].name);
  });
  return result;
}

PaimonFilesMetadataReader::PaimonFilesMetadataReader(const protocol::v2::PinnedMetadataObject& manifest,
    uint64_t snapshotId, const std::string& serialized,
    RowTypePtr outputType, memory::MemoryPool* pool, const MetadataReadContext& reads)
    : output_type_(std::move(outputType)), pool_(pool) {
  protocol::v2::PaimonFilesReadContext context;
  if (manifest.uri().empty() || manifest.length() == 0 || manifest.sha256().size() != 32
      || snapshotId == 0 || serialized.size() > kMaxMetadataBytes || !context.ParseFromString(serialized)
      || context.files().empty() || context.schemas().empty())
    throw std::runtime_error("Invalid Paimon files read context");
  validateSnapshot(context.snapshot(), snapshotId, reads);
  read_schema_id_ = context.read_schema_id();
  for (const auto& source : context.schemas()) {
    if (source.schema_json().size() > kMaxMetadataBytes ||
        !schemas_.emplace(source.schema_id(), schema(source.schema_json(), source.schema_id())).second)
      throw std::runtime_error("Duplicate Paimon file schema identity");
  }
  if (!schemas_.count(read_schema_id_)) throw std::runtime_error("Paimon read schema is not frozen");
  for (const auto& file : context.files()) {
    if (file.identifier_sha256().size() != 32 || file.file_uri().empty()
        || !selected_.emplace(file.identifier_sha256(), Selection{file.file_uri()}).second)
      throw std::runtime_error("Invalid duplicate Paimon file selection");
  }
  remaining_ = selected_.size();
  validateOutput();
  input_ = std::make_unique<AvroContainerReader>(reads.open(manifest.uri()), manifest.length(),
      manifest.sha256(), reads.cancellationToken());
}

void PaimonFilesMetadataReader::validateOutput() const {
  for (uint32_t index = 0; index < output_type_->size(); ++index) {
    auto it = columns().find(output_type_->nameOf(index));
    const auto& type = output_type_->childAt(index);
    if (it == columns().end() || type->kind() != it->second ||
        (type->isArray() && !type->childAt(0)->isVarchar()))
      throw std::runtime_error("Unsupported Paimon files output field or type");
  }
}

std::string PaimonFilesMetadataReader::identifier(const folly::dynamic& entry) {
  const auto& file = optional(entry, "_FILE");
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (!digest || EVP_DigestInit_ex(digest.get(), EVP_sha256(), nullptr) != 1)
    throw std::runtime_error("Cannot initialize Paimon file identity digest");
  auto bytes = [&](const std::string_view value) {
    if (EVP_DigestUpdate(digest.get(), value.data(), value.size()) != 1)
      throw std::runtime_error("Cannot hash Paimon file identity");
  };
  auto integer = [&](int64_t value) {
    if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())
      throw std::runtime_error("Paimon file identity integer overflow");
    uint32_t encoded = static_cast<uint32_t>(value);
    std::array<char, 4> buffer{};
    for (size_t index = 0; index < 4; ++index) buffer[index] = static_cast<char>(encoded >> (24 - index * 8));
    bytes(std::string_view(buffer.data(), buffer.size()));
  };
  auto binary = [&](const folly::dynamic& value) {
    if (value.isNull()) integer(-1);
    else {
      if (!value.isString()) throw std::runtime_error("Invalid Paimon file identity bytes");
      integer(value.asString().size());
      bytes(value.asString());
    }
  };
  constexpr char domain[] = "pangrova-paimon-file-entry-v1\0";
  bytes(std::string_view(domain, sizeof(domain) - 1));
  binary(optional(entry, "_PARTITION"));
  integer(number(entry, "_BUCKET"));
  integer(number(file, "_LEVEL"));
  binary(optional(file, "_FILE_NAME"));
  const auto& extras = optional(file, "_EXTRA_FILES");
  if (!extras.isArray()) throw std::runtime_error("Invalid Paimon extra file identity");
  integer(extras.size());
  for (const auto& extra : extras) binary(extra);
  binary(optional(file, "_EMBEDDED_FILE_INDEX"));
  binary(optional(file, "_EXTERNAL_PATH"));
  std::array<unsigned char, 32> output{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(digest.get(), output.data(), &size) != 1 || size != output.size())
    throw std::runtime_error("Cannot finalize Paimon file identity");
  return std::string(reinterpret_cast<const char*>(output.data()), output.size());
}

std::string PaimonFilesMetadataReader::partition(const folly::dynamic& entry) const {
  const auto& read = schemas_.at(read_schema_id_);
  PaimonBinaryRowView value(text(entry, "_PARTITION"));
  if (value.arity() != static_cast<int>(read.partitions.size()))
    throw std::runtime_error("Paimon partition field count differs");
  std::string result = "{";
  for (size_t index = 0; index < read.partitions.size(); ++index) {
    if (index) result += ", ";
    const auto& type = read.fields[read.partitions[index]].type;
    result += value.text(index, type, type, true).value_or("null");
  }
  return result + "}";
}

std::string PaimonFilesMetadataReader::key(const folly::dynamic& data, bool lower) const {
  const auto& source = schemas_.at(number(data, "_SCHEMA_ID"));
  PaimonBinaryRowView value(text(data, lower ? "_MIN_KEY" : "_MAX_KEY"));
  if (value.arity() == 0) return {};
  std::vector<int> indices = source.keys;
  if (indices.empty()) {
    indices.resize(source.fields.size());
    std::iota(indices.begin(), indices.end(), 0);
  }
  if (value.arity() != static_cast<int>(indices.size())) throw std::runtime_error("Paimon key arity differs");
  std::string result = "[";
  for (size_t index = 0; index < indices.size(); ++index) {
    if (index) result += ", ";
    const auto& type = source.fields[indices[index]].type;
    result += value.text(index, type, type).value_or("null");
  }
  return result + "]";
}

std::array<std::string, 3> PaimonFilesMetadataReader::stats(const folly::dynamic& data) const {
  const auto& source = schemas_.at(number(data, "_SCHEMA_ID"));
  const auto& read = schemas_.at(read_schema_id_);
  const auto& statistics = optional(data, "_VALUE_STATS");
  PaimonBinaryRowView low(text(statistics, "_MIN_VALUES"));
  PaimonBinaryRowView high(text(statistics, "_MAX_VALUES"));
  const auto& counts = optional(statistics, "_NULL_COUNTS");
  const auto& dense = optional(data, "_VALUE_STATS_COLS");
  if ((!counts.isNull() && !counts.isArray()) || (!dense.isNull() && !dense.isArray()))
    throw std::runtime_error("Invalid Paimon statistics collection");
  bool mapping = source.fields.size() != read.fields.size();
  if (!mapping) for (size_t index = 0; index < read.fields.size(); ++index)
    mapping = mapping || read.fields[index].id != source.fields[index].id;
  size_t length = (mapping || !dense.isNull()) ? read.fields.size()
      : std::min(read.fields.size(), static_cast<size_t>(low.arity()));
  std::array<std::string, 3> result{"{", "{", "{"};
  size_t emitted = 0;
  for (int readIndex : read.sorted) {
    if (static_cast<size_t>(readIndex) >= length) continue;
    const auto& field = read.fields[readIndex];
    auto origin = std::find_if(source.fields.begin(), source.fields.end(),
        [&](const auto& old) { return old.id == field.id; });
    int index = origin == source.fields.end() ? -1 : std::distance(source.fields.begin(), origin);
    if (index >= 0 && !dense.isNull()) {
      auto position = std::find(dense.begin(), dense.end(), folly::dynamic(origin->name));
      index = position == dense.end() ? -1 : std::distance(dense.begin(), position);
    }
    std::optional<std::string> nulls, minimum, maximum;
    if (origin == source.fields.end()) nulls = std::to_string(number(data, "_ROW_COUNT"));
    else if (index >= 0) {
      minimum = low.text(index, origin->type, field.type);
      maximum = high.text(index, origin->type, field.type);
      if (!counts.isNull()) {
        if (static_cast<size_t>(index) >= counts.size()) throw std::runtime_error("Paimon null counts are truncated");
        if (!counts[index].isNull()) {
          if (!counts[index].isInt() || counts[index].asInt() < 0) throw std::runtime_error("Invalid Paimon null count");
          nulls = std::to_string(counts[index].asInt());
        }
      }
    }
    const std::array<std::optional<std::string>, 3> values{nulls, minimum, maximum};
    for (size_t column = 0; column < values.size(); ++column) {
      if (emitted) result[column] += ", ";
      result[column] += field.name + "=" + values[column].value_or("null");
    }
    ++emitted;
  }
  for (auto& value : result) value += "}";
  return result;
}

void PaimonFilesMetadataReader::emit(const folly::dynamic& entry, const std::string& uri,
    const RowVectorPtr& output, int row) const {
  const auto& data = optional(entry, "_FILE");
  std::optional<std::array<std::string, 3>> statistics;
  static const std::unordered_map<std::string, const char*> integers{
      {"schema_id", "_SCHEMA_ID"}, {"level", "_LEVEL"}, {"record_count", "_ROW_COUNT"},
      {"file_size_in_bytes", "_FILE_SIZE"}, {"min_sequence_number", "_MIN_SEQUENCE_NUMBER"},
      {"max_sequence_number", "_MAX_SEQUENCE_NUMBER"}, {"deleteRowCount", "_DELETE_ROW_COUNT"},
      {"first_row_id", "_FIRST_ROW_ID"}};
  for (uint32_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    auto vector = output->childAt(column);
    if (auto it = integers.find(name); it != integers.end()) {
      const auto& value = optional(data, it->second);
      if (value.isNull()) vector->setNull(row, true);
      else if (!value.isInt()) throw std::runtime_error("Invalid Paimon file integer value");
      else if (vector->typeKind() == TypeKind::INTEGER) {
        if (value.asInt() < std::numeric_limits<int32_t>::min() || value.asInt() > std::numeric_limits<int32_t>::max())
          throw std::runtime_error("Paimon file integer overflow");
        vector->asFlatVector<int32_t>()->set(row, value.asInt());
      } else vector->asFlatVector<int64_t>()->set(row, value.asInt());
    } else if (name == "bucket") vector->asFlatVector<int32_t>()->set(row, number(entry, "_BUCKET"));
    else if (name == "partition") putString(vector, row, partition(entry));
    else if (name == "file_path") putString(vector, row, uri);
    else if (name == "file_format") {
      auto fileName = text(data, "_FILE_NAME");
      auto dot = fileName.rfind('.');
      if (dot == std::string::npos || dot + 1 == fileName.size()) throw std::runtime_error("Invalid Paimon file format suffix");
      putString(vector, row, fileName.substr(dot + 1));
    } else if (name == "min_key" || name == "max_key") {
      auto value = key(data, name == "min_key");
      if (value.empty()) vector->setNull(row, true);
      else putString(vector, row, value);
    } else if (name == "null_value_counts" || name == "min_value_stats" || name == "max_value_stats") {
      if (!statistics) statistics = stats(data);
      putString(vector, row, (*statistics)[name == "null_value_counts" ? 0 : name == "min_value_stats" ? 1 : 2]);
    } else if (name == "creation_time") {
      const auto& value = optional(data, "_CREATION_TIME");
      if (value.isNull()) vector->setNull(row, true);
      else vector->asFlatVector<Timestamp>()->set(row, Timestamp::fromMillis(value.asInt()));
    } else if (name == "file_source") {
      const auto& value = optional(data, "_FILE_SOURCE");
      if (value.isNull()) vector->setNull(row, true);
      else if (value.asInt() == 0 || value.asInt() == 1) putString(vector, row, value.asInt() == 0 ? "APPEND" : "COMPACT");
      else throw std::runtime_error("Unknown Paimon file source");
    } else if (name == "write_cols") {
      const auto& value = optional(data, "_WRITE_COLS");
      if (value.isNull()) vector->setNull(row, true);
      else {
        if (!value.isArray()) throw std::runtime_error("Invalid Paimon written columns");
        auto* array = vector->as<ArrayVector>();
        const auto offset = array->elements()->size();
        if (value.size() > 1'000'000 - static_cast<size_t>(offset)) throw std::runtime_error("Paimon column array exceeds batch budget");
        array->elements()->resize(offset + value.size());
        array->setOffsetAndSize(row, offset, value.size());
        for (size_t index = 0; index < value.size(); ++index) {
          if (value[index].isNull()) array->elements()->setNull(offset + index, true);
          else putString(array->elements(), offset + index, value[index].asString());
        }
      }
    }
  }
}

RowVectorPtr PaimonFilesMetadataReader::next(uint64_t maxRows) {
  if (maxRows == 0) throw std::invalid_argument("Paimon metadata batch size must be positive");
  std::vector<std::pair<folly::dynamic, std::string>> rows;
  uint64_t bytes = 0;
  while (rows.size() < std::min(maxRows, kMaxBatchRows) && bytes < 8 * 1024 * 1024) {
    auto record = input_->nextRecord();
    if (!record) {
      if (remaining_ != 0) throw std::runtime_error("Selected Paimon file is absent from its pinned manifest");
      break;
    }
    if (number(*record, "_VERSION") != 2) throw std::runtime_error("Unsupported Paimon manifest entry version");
    const auto kind = number(*record, "_KIND");
    if (kind != 0 && kind != 1) throw std::runtime_error("Unknown Paimon manifest entry kind");
    auto found = selected_.find(identifier(*record));
    if (found == selected_.end() || kind != 0) continue;
    if (found->second.seen) throw std::runtime_error("Duplicate selected Paimon manifest ADD");
    const auto& data = optional(*record, "_FILE");
    const auto& external = optional(data, "_EXTERNAL_PATH");
    if ((!external.isNull() && external.asString() != found->second.uri) ||
        (external.isNull() && !found->second.uri.ends_with("/" + text(data, "_FILE_NAME"))))
      throw std::runtime_error("Selected Paimon physical URI differs from manifest identity");
    if (number(data, "_SCHEMA_ID") < 0 || !schemas_.count(number(data, "_SCHEMA_ID")))
      throw std::runtime_error("Selected Paimon file schema is not frozen");
    found->second.seen = true;
    --remaining_;
    bytes += input_->lastRecordSize();
    (*record)["_FILE"].erase("_EMBEDDED_FILE_INDEX");
    (*record)["_FILE"].erase("_EXTRA_FILES");
    (*record)["_FILE"].erase("_KEY_STATS");
    rows.emplace_back(std::move(*record), found->second.uri);
  }
  if (rows.empty()) return nullptr;
  auto result = std::dynamic_pointer_cast<RowVector>(BaseVector::create(output_type_, rows.size(), pool_));
  for (size_t index = 0; index < rows.size(); ++index) emit(rows[index].first, rows[index].second, result, index);
  return result;
}
}  // namespace pangrova::velox
