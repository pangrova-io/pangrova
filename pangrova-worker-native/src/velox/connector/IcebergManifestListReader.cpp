#include "velox/connector/IcebergManifestListReader.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_set>

#include "split/PathUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kMaxBatchRows = 1024;
constexpr uint64_t kMaxSummaryFields = 1024;

int64_t decimalMetadata(const IcebergAvroContainerReader& container,
    const std::string& key) {
  const auto value = container.metadataValue(key);
  int64_t parsed = 0;
  const auto [end, error] = std::from_chars(
      value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::runtime_error("Invalid Iceberg manifest-list metadata: " + key);
  }
  return parsed;
}

int64_t requiredInteger(const folly::dynamic& record, const char* field) {
  const auto& value = record[field];
  if (!value.isInt()) {
    throw std::runtime_error(std::string("Invalid Iceberg manifest integer: ") + field);
  }
  return value.asInt();
}

int32_t requiredInt32(const folly::dynamic& record, const char* field) {
  const auto value = requiredInteger(record, field);
  if (value < std::numeric_limits<int32_t>::min() ||
      value > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error(std::string("Iceberg manifest int32 overflow: ") + field);
  }
  return static_cast<int32_t>(value);
}

std::string requiredString(const folly::dynamic& record, const char* field) {
  const auto& value = record[field];
  if (!value.isString()) {
    throw std::runtime_error(std::string("Invalid Iceberg manifest string: ") + field);
  }
  return value.asString();
}

int32_t littleInt(std::string_view bytes) {
  if (bytes.size() != sizeof(uint32_t)) {
    throw std::runtime_error("Invalid Iceberg partition integer bound width");
  }
  uint32_t bits = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    bits |= static_cast<uint32_t>(static_cast<uint8_t>(bytes[index])) << (index * 8);
  }
  return std::bit_cast<int32_t>(bits);
}

int64_t littleLong(std::string_view bytes) {
  if (bytes.size() == sizeof(uint32_t)) return littleInt(bytes);
  if (bytes.size() != sizeof(uint64_t)) {
    throw std::runtime_error("Invalid Iceberg partition long bound width");
  }
  uint64_t bits = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    bits |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[index])) << (index * 8);
  }
  return std::bit_cast<int64_t>(bits);
}

std::string dateFromDays(int64_t days) {
  using namespace std::chrono;
  if (days < std::numeric_limits<int32_t>::min() ||
      days > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error("Iceberg partition date exceeds supported range");
  }
  const auto date = year_month_day{sys_days{year{1970}/1/1} +
      std::chrono::days{static_cast<int32_t>(days)}};
  const auto yearValue = static_cast<int>(date.year());
  if (!date.ok() || yearValue < 0 || yearValue > 9999) {
    throw std::runtime_error("Iceberg partition date exceeds supported year range");
  }
  char value[32]{};
  std::snprintf(value, sizeof(value), "%04d-%02u-%02u", yearValue,
      static_cast<unsigned>(date.month()), static_cast<unsigned>(date.day()));
  return value;
}

int64_t floorDivide(int64_t value, int64_t denominator) {
  auto quotient = value / denominator;
  if (value % denominator < 0) --quotient;
  return quotient;
}

std::string humanInteger(const std::string& transform, int32_t value) {
  if (transform == "year") {
    return std::to_string(static_cast<int64_t>(1970) + value);
  }
  if (transform == "month") {
    const auto year = 1970 + floorDivide(value, 12);
    const auto month = 1 + value - floorDivide(value, 12) * 12;
    char result[32]{};
    std::snprintf(result, sizeof(result), "%04lld-%02lld",
        static_cast<long long>(year), static_cast<long long>(month));
    return result;
  }
  if (transform == "day") return dateFromDays(value);
  if (transform == "hour") {
    const auto days = floorDivide(value, 24);
    const auto hour = value - days * 24;
    char result[40]{};
    std::snprintf(result, sizeof(result), "%s-%02lld", dateFromDays(days).c_str(),
        static_cast<long long>(hour));
    return result;
  }
  return std::to_string(value);
}

std::string humanBound(const split::IcebergManifestPartitionField& field,
    const folly::dynamic& bound) {
  if (bound.isNull()) return "null";
  if (!bound.isString()) {
    throw std::runtime_error("Iceberg manifest partition bound is not Avro bytes");
  }
  const auto& encoded = bound.asString();
  const std::string_view bytes(encoded.data(), encoded.size());
  if (field.transform == "void") return "null";
  if (field.result_type == "int") {
    return humanInteger(field.transform, littleInt(bytes));
  }
  if (field.result_type == "long") return std::to_string(littleLong(bytes));
  if (field.result_type == "date") return dateFromDays(littleInt(bytes));
  if (field.result_type == "boolean") {
    if (bytes.size() != 1 || (bytes[0] != 0 && bytes[0] != 1)) {
      throw std::runtime_error("Invalid Iceberg partition boolean bound");
    }
    return bytes[0] == 1 ? "true" : "false";
  }
  if (field.result_type == "string") return std::string(bytes);
  throw std::runtime_error("Unsupported Iceberg partition summary bound type: " +
      field.result_type);
}

struct Summary {
  bool containsNull;
  std::optional<bool> containsNan;
  std::string lower;
  std::string upper;
};

bool isDataFileCount(const std::string& name) {
  return name == "added_data_files_count" ||
      name == "existing_data_files_count" ||
      name == "deleted_data_files_count";
}

bool isDeleteFileCount(const std::string& name) {
  return name == "added_delete_files_count" ||
      name == "existing_delete_files_count" ||
      name == "deleted_delete_files_count";
}

const char* countSource(const std::string& name) {
  if (name.starts_with("added_")) return "added_files_count";
  if (name.starts_with("existing_")) return "existing_files_count";
  return "deleted_files_count";
}

}  // namespace

IcebergManifestListReader::IcebergManifestListReader(
    const std::string& uri, uint64_t expectedLength, uint64_t snapshotId,
    uint64_t sequenceNumber, int32_t formatVersion,
    const std::string& expectedSha256,
    const std::vector<split::IcebergManifestPartitionSpec>& specs,
    facebook::velox::RowTypePtr outputType,
    facebook::velox::memory::MemoryPool* pool)
    : output_type_(std::move(outputType)), pool_(pool) {
  if (!output_type_ || !pool_ || snapshotId == 0 ||
      (formatVersion != 2 && formatVersion != 3)) {
    throw std::invalid_argument("Invalid Iceberg manifests reader selection");
  }
  const auto path = split::metadataObjectPath(uri);
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!filesystem) throw std::runtime_error("No Native filesystem for Iceberg manifest list");
  container_ = std::make_unique<IcebergAvroContainerReader>(
      filesystem->openFileForRead(path), expectedLength, expectedSha256);
  const auto headerSnapshot = decimalMetadata(*container_, "snapshot-id");
  const auto headerSequence = decimalMetadata(*container_, "sequence-number");
  if (headerSnapshot <= 0 || static_cast<uint64_t>(headerSnapshot) != snapshotId ||
      headerSequence < 0 || static_cast<uint64_t>(headerSequence) != sequenceNumber ||
      decimalMetadata(*container_, "format-version") != formatVersion) {
    throw std::runtime_error("Pinned Iceberg manifest-list header identity differs");
  }
  for (const auto& spec : specs) {
    if (spec.spec_id < 0 || spec.fields.size() > kMaxSummaryFields ||
        !specs_.emplace(spec.spec_id, spec.fields).second) {
      throw std::runtime_error("Invalid Iceberg partition summary spec binding");
    }
  }
  validateOutputSchema();
}

void IcebergManifestListReader::validateOutputSchema() const {
  static const std::unordered_set<std::string> kColumns = {
      "content", "path", "length", "partition_spec_id", "added_snapshot_id",
      "added_data_files_count", "existing_data_files_count",
      "deleted_data_files_count", "added_delete_files_count",
      "existing_delete_files_count", "deleted_delete_files_count",
      "partition_summaries"};
  std::unordered_set<std::string> names;
  for (size_t index = 0; index < output_type_->size(); ++index) {
    const auto& name = output_type_->nameOf(index);
    const auto kind = output_type_->childAt(index)->kind();
    const auto expected = name == "path"
        ? facebook::velox::TypeKind::VARCHAR
        : name == "length" || name == "added_snapshot_id"
            ? facebook::velox::TypeKind::BIGINT
            : name == "partition_summaries"
                ? facebook::velox::TypeKind::ARRAY
                : facebook::velox::TypeKind::INTEGER;
    if (!kColumns.contains(name) || !names.insert(name).second || kind != expected) {
      throw std::runtime_error("Invalid Iceberg $manifests output column: " + name);
    }
    if (name == "partition_summaries") {
      const auto element = output_type_->childAt(index)->asArray().elementType();
      if (element->kind() != facebook::velox::TypeKind::ROW ||
          element->asRow().size() != 4 ||
          element->asRow().nameOf(0) != "contains_null" ||
          element->asRow().nameOf(1) != "contains_nan" ||
          element->asRow().nameOf(2) != "lower_bound" ||
          element->asRow().nameOf(3) != "upper_bound" ||
          element->asRow().childAt(0)->kind() != facebook::velox::TypeKind::BOOLEAN ||
          element->asRow().childAt(1)->kind() != facebook::velox::TypeKind::BOOLEAN ||
          element->asRow().childAt(2)->kind() != facebook::velox::TypeKind::VARCHAR ||
          element->asRow().childAt(3)->kind() != facebook::velox::TypeKind::VARCHAR) {
        throw std::runtime_error("Invalid Iceberg partition_summaries output type");
      }
    }
  }
}

facebook::velox::VectorPtr IcebergManifestListReader::summaries(
    const std::vector<folly::dynamic>& rows,
    const facebook::velox::TypePtr& arrayType) const {
  using namespace facebook::velox;
  auto offsets = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool_);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool_);
  auto nulls = AlignedBuffer::allocate<bool>(rows.size(), pool_, bits::kNotNull);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  auto* rawNulls = nulls->asMutable<uint64_t>();
  std::vector<Summary> flat;
  for (size_t row = 0; row < rows.size(); ++row) {
    rawOffsets[row] = flat.size();
    const auto& partitions = rows[row]["partitions"];
    if (partitions.isNull()) {
      bits::setNull(rawNulls, row, true);
      rawSizes[row] = 0;
      continue;
    }
    if (!partitions.isArray() || partitions.size() > kMaxSummaryFields) {
      throw std::runtime_error("Invalid Iceberg manifest partition summaries");
    }
    const auto specId = requiredInt32(rows[row], "partition_spec_id");
    auto spec = specs_.find(specId);
    if (spec == specs_.end() || spec->second.size() != partitions.size()) {
      throw std::runtime_error("Iceberg manifest partition summary spec differs");
    }
    rawSizes[row] = partitions.size();
    for (size_t index = 0; index < partitions.size(); ++index) {
      const auto& source = partitions[index];
      if (!source.isObject() || !source["contains_null"].isBool()) {
        throw std::runtime_error("Invalid Iceberg manifest partition summary");
      }
      std::optional<bool> containsNan;
      if (!source["contains_nan"].isNull()) {
        if (!source["contains_nan"].isBool()) {
          throw std::runtime_error("Invalid Iceberg manifest contains_nan");
        }
        containsNan = source["contains_nan"].asBool();
      }
      flat.push_back({source["contains_null"].asBool(), containsNan,
          humanBound(spec->second[index], source["lower_bound"]),
          humanBound(spec->second[index], source["upper_bound"])});
    }
  }
  const auto elementType = arrayType->asArray().elementType();
  auto elements = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(elementType, flat.size(), pool_));
  auto containsNull = elements->childAt(0)->asFlatVector<bool>();
  auto containsNan = elements->childAt(1)->asFlatVector<bool>();
  auto lower = elements->childAt(2)->asFlatVector<StringView>();
  auto upper = elements->childAt(3)->asFlatVector<StringView>();
  for (size_t index = 0; index < flat.size(); ++index) {
    containsNull->set(index, flat[index].containsNull);
    if (flat[index].containsNan) containsNan->set(index, *flat[index].containsNan);
    else containsNan->setNull(index, true);
    lower->set(index, StringView(flat[index].lower));
    upper->set(index, StringView(flat[index].upper));
  }
  return std::make_shared<ArrayVector>(pool_, arrayType, nulls, rows.size(),
      offsets, sizes, elements);
}

facebook::velox::RowVectorPtr IcebergManifestListReader::next(uint64_t maxRows) {
  if (maxRows == 0) throw std::invalid_argument("Metadata scan batch size must be positive");
  std::vector<folly::dynamic> rows;
  rows.reserve(std::min(maxRows, kMaxBatchRows));
  while (rows.size() < maxRows && rows.size() < kMaxBatchRows) {
    auto record = container_->nextRecord();
    if (!record) break;
    if (!record->isObject()) throw std::runtime_error("Invalid Iceberg manifest record");
    rows.push_back(std::move(*record));
  }
  if (rows.empty()) return nullptr;
  std::vector<facebook::velox::VectorPtr> columns;
  columns.reserve(output_type_->size());
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    const auto type = output_type_->childAt(column);
    if (name == "partition_summaries") {
      columns.push_back(summaries(rows, type));
      continue;
    }
    auto vector = facebook::velox::BaseVector::create(type, rows.size(), pool_);
    for (size_t row = 0; row < rows.size(); ++row) {
      if (name == "path") {
        const auto path = requiredString(rows[row], "manifest_path");
        vector->asFlatVector<facebook::velox::StringView>()->set(
            row, facebook::velox::StringView(path));
      } else if (name == "length" || name == "added_snapshot_id") {
        const auto value = requiredInteger(rows[row], name == "length"
            ? "manifest_length" : "added_snapshot_id");
        vector->asFlatVector<int64_t>()->set(row, value);
      } else {
        int32_t value = 0;
        if (name == "content") {
          value = requiredInt32(rows[row], "content");
          if (value != 0 && value != 1) {
            throw std::runtime_error("Invalid Iceberg manifest content kind");
          }
        } else if (name == "partition_spec_id") {
          value = requiredInt32(rows[row], "partition_spec_id");
        } else {
          const auto content = requiredInt32(rows[row], "content");
          if (content != 0 && content != 1) {
            throw std::runtime_error("Invalid Iceberg manifest content kind");
          }
          if ((content == 0 && isDataFileCount(name)) ||
              (content == 1 && isDeleteFileCount(name))) {
            value = requiredInt32(rows[row], countSource(name));
          }
        }
        vector->asFlatVector<int32_t>()->set(row, value);
      }
    }
    columns.push_back(std::move(vector));
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool_, output_type_, nullptr, rows.size(), std::move(columns));
}

}  // namespace pangrova::velox
