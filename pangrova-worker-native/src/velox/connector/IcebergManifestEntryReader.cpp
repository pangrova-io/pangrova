#include "velox/connector/IcebergManifestEntryReader.h"
#include "velox/connector/IcebergPartitionProjection.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "split/PathUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kMaxBatchRows = 1024;
constexpr size_t kMaxBatchArrayElements = 1'000'000;

std::unique_ptr<IcebergAvroContainerReader> openPinned(
    const std::string& uri, uint64_t length, const std::string& sha256) {
  const auto path = split::metadataObjectPath(uri);
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!filesystem) throw std::runtime_error("No Native filesystem for Iceberg manifest");
  return std::make_unique<IcebergAvroContainerReader>(
      filesystem->openFileForRead(path), length, sha256);
}

int64_t headerLong(const IcebergAvroContainerReader& container, const char* name) {
  const auto value = container.metadataValue(name);
  int64_t parsed = 0;
  const auto [end, error] = std::from_chars(
      value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::runtime_error(std::string("Invalid Iceberg manifest header: ") + name);
  }
  return parsed;
}

int64_t requiredLong(const folly::dynamic& object, const char* name) {
  if (!object.isObject() || !object.count(name) || !object[name].isInt()) {
    throw std::runtime_error(std::string("Invalid Iceberg file integer: ") + name);
  }
  return object[name].asInt();
}

int32_t requiredInt(const folly::dynamic& object, const char* name) {
  const auto value = requiredLong(object, name);
  if (value < std::numeric_limits<int32_t>::min() ||
      value > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error(std::string("Iceberg file int32 overflow: ") + name);
  }
  return static_cast<int32_t>(value);
}

std::string requiredText(const folly::dynamic& object, const char* name) {
  if (!object.isObject() || !object.count(name) || !object[name].isString()) {
    throw std::runtime_error(std::string("Invalid Iceberg file string: ") + name);
  }
  const auto& text = object[name].asString();
  if (text.empty()) {
    throw std::runtime_error(std::string("Empty Iceberg file string: ") + name);
  }
  return text;
}

bool avroKind(const folly::dynamic& schema, std::string_view kind) {
  if (schema.isString()) {
    const auto& value = schema.asString();
    return std::string_view(value.data(), value.size()) == kind;
  }
  if (!schema.isObject() || !schema.count("type") || !schema["type"].isString()) {
    return false;
  }
  const auto& value = schema["type"].asString();
  return std::string_view(value.data(), value.size()) == kind;
}

const folly::dynamic& avroField(const folly::dynamic& record,
    std::string_view name, int64_t fieldId) {
  if (!avroKind(record, "record") || !record.count("fields") ||
      !record["fields"].isArray()) {
    throw std::runtime_error("Invalid Iceberg $files Avro record schema");
  }
  const folly::dynamic* found = nullptr;
  for (const auto& field : record["fields"]) {
    if (!field.isObject() || !field.count("name") ||
        !field["name"].isString() || !field.count("type")) {
      throw std::runtime_error("Invalid Iceberg $files Avro field schema");
    }
    const auto& fieldName = field["name"].asString();
    if (std::string_view(fieldName.data(), fieldName.size()) != name) continue;
    if (found || !field.count("field-id") || !field["field-id"].isInt() ||
        field["field-id"].asInt() != fieldId) {
      throw std::runtime_error("Iceberg $files Avro field ID differs");
    }
    found = &field["type"];
  }
  if (!found) throw std::runtime_error("Iceberg $files Avro field is missing");
  return *found;
}

const folly::dynamic& nonNullAvroSchema(const folly::dynamic& schema) {
  if (!schema.isArray()) return schema;
  if (schema.size() != 2) {
    throw std::runtime_error("Unsupported Iceberg $files Avro union");
  }
  if (avroKind(schema[0], "null") && !avroKind(schema[1], "null")) {
    return schema[1];
  }
  if (avroKind(schema[1], "null") && !avroKind(schema[0], "null")) {
    return schema[0];
  }
  throw std::runtime_error("Unsupported Iceberg $files Avro union");
}

void validateColumnSizesSchema(const folly::dynamic& writerSchema) {
  const auto& dataFile = nonNullAvroSchema(avroField(writerSchema, "data_file", 2));
  const auto& map = nonNullAvroSchema(avroField(dataFile, "column_sizes", 108));
  if (!avroKind(map, "array") || !map.count("logicalType") ||
      !map["logicalType"].isString() || map["logicalType"].asString() != "map" ||
      !map.count("items")) {
    throw std::runtime_error("Unsupported Iceberg column_sizes Avro logical map");
  }
  const auto& item = map["items"];
  if (!avroKind(avroField(item, "key", 117), "int") ||
      !avroKind(avroField(item, "value", 118), "long")) {
    throw std::runtime_error("Unsupported Iceberg column_sizes Avro key/value type");
  }
}

using MetricMap = std::vector<std::pair<int32_t, int64_t>>;

std::optional<MetricMap> optionalColumnSizes(const folly::dynamic& file) {
  if (!file.count("column_sizes") || file["column_sizes"].isNull()) {
    return std::nullopt;
  }
  const auto& encoded = file["column_sizes"];
  if (!encoded.isArray() || encoded.size() > kMaxBatchArrayElements) {
    throw std::runtime_error("Invalid Iceberg column_sizes logical map");
  }
  MetricMap values;
  std::unordered_set<int32_t> keys;
  values.reserve(encoded.size());
  for (const auto& entry : encoded) {
    const auto key = requiredInt(entry, "key");
    const auto value = requiredLong(entry, "value");
    if (key <= 0 || value < 0 || !keys.insert(key).second) {
      throw std::runtime_error("Invalid Iceberg column_sizes key/value");
    }
    values.emplace_back(key, value);
  }
  return values;
}

template <typename T>
std::optional<std::vector<T>> optionalIntegerArray(
    const folly::dynamic& object, const char* name) {
  if (!object.count(name) || object[name].isNull()) return std::nullopt;
  const auto& array = object[name];
  if (!array.isArray() || array.size() > kMaxBatchArrayElements) {
    throw std::runtime_error(std::string("Invalid Iceberg file array: ") + name);
  }
  std::vector<T> values;
  values.reserve(array.size());
  for (const auto& item : array) {
    if (!item.isInt() || item.asInt() < 0 ||
        (std::is_same_v<T, int32_t> &&
            item.asInt() > std::numeric_limits<int32_t>::max())) {
      throw std::runtime_error(std::string("Invalid Iceberg file array value: ") + name);
    }
    values.push_back(static_cast<T>(item.asInt()));
  }
  return values;
}

std::optional<int32_t> optionalSortOrderId(const folly::dynamic& file) {
  if (!file.count("sort_order_id") || file["sort_order_id"].isNull()) {
    return std::nullopt;
  }
  const auto value = requiredInt(file, "sort_order_id");
  if (value < 0) throw std::runtime_error("Invalid Iceberg sort order ID");
  return value;
}

struct ParentManifestIdentity {
  int64_t addedSnapshotId = 0;
  int64_t sequenceNumber = 0;
  std::optional<int64_t> firstRowId;
};

ParentManifestIdentity verifyParentMembership(IcebergAvroContainerReader& list,
    const std::string& childUri, uint64_t childLength,
    uint64_t snapshotId, uint64_t sequenceNumber, int32_t formatVersion,
    int32_t specId, int32_t content, bool requireEntryMetadata) {
  if (snapshotId == 0 || headerLong(list, "snapshot-id") !=
          static_cast<int64_t>(snapshotId) ||
      headerLong(list, "sequence-number") != static_cast<int64_t>(sequenceNumber) ||
      headerLong(list, "format-version") != formatVersion) {
    throw std::runtime_error("Pinned Iceberg manifest-list header differs");
  }
  int matches = 0;
  ParentManifestIdentity identity;
  while (auto record = list.nextRecord()) {
    if (!record->isObject()) {
      throw std::runtime_error("Invalid Iceberg manifest-list record");
    }
    if (requiredText(*record, "manifest_path") != childUri) continue;
    if (requiredLong(*record, "manifest_length") !=
            static_cast<int64_t>(childLength) ||
        requiredInt(*record, "partition_spec_id") != specId ||
        requiredInt(*record, "content") != content) {
      throw std::runtime_error("Pinned Iceberg child differs from manifest list");
    }
    if (requireEntryMetadata) {
      identity.addedSnapshotId = requiredLong(*record, "added_snapshot_id");
      identity.sequenceNumber = requiredLong(*record, "sequence_number");
      if (identity.addedSnapshotId <= 0 || identity.sequenceNumber < 0 ||
          identity.sequenceNumber > static_cast<int64_t>(sequenceNumber)) {
        throw std::runtime_error("Invalid Iceberg manifest entry inheritance");
      }
      if (formatVersion == 3 && record->count("first_row_id") &&
          !(*record)["first_row_id"].isNull()) {
        const auto first = requiredLong(*record, "first_row_id");
        if (first < 0) throw std::runtime_error("Negative Iceberg manifest first row ID");
        identity.firstRowId = first;
      }
    }
    ++matches;
  }
  if (matches != 1) {
    throw std::runtime_error("Pinned Iceberg child is missing or duplicated in manifest list");
  }
  return identity;
}

struct FileRow {
  int32_t content;
  std::string path;
  std::string format;
  int32_t specId;
  int64_t recordCount;
  int64_t fileSize;
  std::optional<MetricMap> columnSizes;
  std::optional<std::vector<int64_t>> splitOffsets;
  std::optional<std::vector<int32_t>> equalityIds;
  std::optional<int32_t> sortOrderId;
  int64_t snapshotId = 0;
  int64_t sequenceNumber = 0;
  int64_t fileSequenceNumber = 0;
  int32_t status = 0;
  std::optional<int64_t> firstRowId;
  folly::dynamic partition{nullptr};
};

template <typename T>
facebook::velox::VectorPtr arrayColumn(const std::vector<FileRow>& rows,
    const facebook::velox::TypePtr& type,
    std::optional<std::vector<T>> FileRow::* member,
    facebook::velox::memory::MemoryPool* pool) {
  using namespace facebook::velox;
  auto offsets = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool);
  auto nulls = AlignedBuffer::allocate<bool>(rows.size(), pool, bits::kNotNull);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  auto* rawNulls = nulls->asMutable<uint64_t>();
  std::vector<T> flat;
  for (size_t row = 0; row < rows.size(); ++row) {
    rawOffsets[row] = flat.size();
    const auto& values = rows[row].*member;
    if (!values) {
      bits::setNull(rawNulls, row, true);
      rawSizes[row] = 0;
      continue;
    }
    if (values->size() > kMaxBatchArrayElements - flat.size()) {
      throw std::runtime_error("Iceberg file arrays exceed batch element limit");
    }
    rawSizes[row] = values->size();
    flat.insert(flat.end(), values->begin(), values->end());
  }
  auto elements = BaseVector::create(type->asArray().elementType(), flat.size(), pool);
  auto* typed = elements->template asFlatVector<T>();
  for (size_t index = 0; index < flat.size(); ++index) {
    typed->set(index, flat[index]);
  }
  return std::make_shared<ArrayVector>(pool, type, nulls, rows.size(),
      offsets, sizes, elements);
}

facebook::velox::VectorPtr mapColumn(const std::vector<FileRow>& rows,
    const facebook::velox::TypePtr& type,
    facebook::velox::memory::MemoryPool* pool) {
  using namespace facebook::velox;
  auto offsets = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool);
  auto sizes = AlignedBuffer::allocate<vector_size_t>(rows.size(), pool);
  auto nulls = AlignedBuffer::allocate<bool>(rows.size(), pool, bits::kNotNull);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  auto* rawNulls = nulls->asMutable<uint64_t>();
  MetricMap flat;
  for (size_t row = 0; row < rows.size(); ++row) {
    rawOffsets[row] = flat.size();
    const auto& values = rows[row].columnSizes;
    if (!values) {
      bits::setNull(rawNulls, row, true);
      rawSizes[row] = 0;
      continue;
    }
    if (values->size() > kMaxBatchArrayElements - flat.size()) {
      throw std::runtime_error("Iceberg column_sizes exceeds batch element limit");
    }
    rawSizes[row] = values->size();
    flat.insert(flat.end(), values->begin(), values->end());
  }
  auto keys = BaseVector::create(type->asMap().keyType(), flat.size(), pool);
  auto values = BaseVector::create(type->asMap().valueType(), flat.size(), pool);
  for (size_t index = 0; index < flat.size(); ++index) {
    keys->asFlatVector<int32_t>()->set(index, flat[index].first);
    values->asFlatVector<int64_t>()->set(index, flat[index].second);
  }
  return std::make_shared<MapVector>(pool, type, nulls, rows.size(),
      offsets, sizes, keys, values);
}

FileRow decodeLiveEntry(const folly::dynamic& entry,
    int32_t specId, int32_t manifestContent, bool readPartition) {
  if (!entry.isObject() || !entry.count("data_file") ||
      !entry["data_file"].isObject()) {
    throw std::runtime_error("Invalid Iceberg manifest entry");
  }
  const auto& file = entry["data_file"];
  const auto content = requiredInt(file, "content");
  if ((manifestContent == 0 && content != 0) ||
      (manifestContent == 1 && content != 1 && content != 2)) {
    throw std::runtime_error("Iceberg file content disagrees with manifest kind");
  }
  if (file.count("spec_id") && !file["spec_id"].isNull() &&
      requiredInt(file, "spec_id") != specId) {
    throw std::runtime_error("Iceberg file spec differs from manifest spec");
  }
  const auto recordCount = requiredLong(file, "record_count");
  const auto fileSize = requiredLong(file, "file_size_in_bytes");
  if (recordCount < 0 || fileSize < 0) {
    throw std::runtime_error("Negative Iceberg file count or size");
  }
  auto splitOffsets = optionalIntegerArray<int64_t>(file, "split_offsets");
  if (splitOffsets && !std::is_sorted(splitOffsets->begin(), splitOffsets->end())) {
    throw std::runtime_error("Iceberg split offsets are not sorted");
  }
  auto equalityIds = optionalIntegerArray<int32_t>(file, "equality_ids");
  if ((content == 2 && (!equalityIds || equalityIds->empty())) ||
      (content != 2 && equalityIds)) {
    throw std::runtime_error("Iceberg equality IDs disagree with file content");
  }
  FileRow result{content, requiredText(file, "file_path"),
      requiredText(file, "file_format"), specId, recordCount, fileSize,
      optionalColumnSizes(file), std::move(splitOffsets),
      std::move(equalityIds), optionalSortOrderId(file),
      0, 0, 0, 0, std::nullopt};
  if (readPartition && file.count("partition")) result.partition = file["partition"];
  return result;
}

facebook::velox::RowVectorPtr projectFileRows(const std::vector<FileRow>& rows,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::memory::MemoryPool* pool,
    const IcebergPartitionProjection* partitionProjection) {
  std::vector<facebook::velox::VectorPtr> columns;
  columns.reserve(outputType->size());
  for (size_t column = 0; column < outputType->size(); ++column) {
    const auto& name = outputType->nameOf(column);
    const auto type = outputType->childAt(column);
    if (name == "partition") {
      std::vector<const folly::dynamic*> partitions;
      partitions.reserve(rows.size());
      for (const auto& row : rows) partitions.push_back(&row.partition);
      columns.push_back(partitionProjection->project(partitions, pool));
      continue;
    }
    if (name == "split_offsets") {
      columns.push_back(arrayColumn(rows, type, &FileRow::splitOffsets, pool));
      continue;
    }
    if (name == "column_sizes") {
      columns.push_back(mapColumn(rows, type, pool));
      continue;
    }
    if (name == "equality_ids") {
      columns.push_back(arrayColumn(rows, type, &FileRow::equalityIds, pool));
      continue;
    }
    auto vector = facebook::velox::BaseVector::create(
        type, rows.size(), pool);
    for (size_t row = 0; row < rows.size(); ++row) {
      const auto& file = rows[row];
      if (name == "first_row_id") {
        if (file.firstRowId) vector->asFlatVector<int64_t>()->set(row, *file.firstRowId);
        else vector->setNull(row, true);
      } else if (name == "sort_order_id") {
        if (file.sortOrderId) {
          vector->asFlatVector<int32_t>()->set(row, *file.sortOrderId);
        } else {
          vector->setNull(row, true);
        }
      } else if (name == "content" || name == "spec_id") {
        vector->asFlatVector<int32_t>()->set(row,
            name == "content" ? file.content : file.specId);
      } else if (name == "file_path" || name == "file_format") {
        vector->asFlatVector<facebook::velox::StringView>()->set(row,
            facebook::velox::StringView(
                name == "file_path" ? file.path : file.format));
      } else {
        vector->asFlatVector<int64_t>()->set(row,
            name == "record_count" ? file.recordCount : file.fileSize);
      }
    }
    columns.push_back(std::move(vector));
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool, outputType, nullptr, rows.size(), std::move(columns));
}

void validateFileOutput(const facebook::velox::RowTypePtr& outputType,
    const folly::dynamic& writerSchema) {
  using facebook::velox::TypeKind;
  std::unordered_set<std::string> names;
  for (size_t index = 0; index < outputType->size(); ++index) {
    const auto& name = outputType->nameOf(index);
    const auto expected = name == "partition" ? TypeKind::ROW
        : name == "column_sizes" ? TypeKind::MAP
        : name == "split_offsets" || name == "equality_ids"
        ? TypeKind::ARRAY : name == "content" || name == "spec_id" ||
            name == "sort_order_id"
        ? TypeKind::INTEGER
        : name == "file_path" || name == "file_format"
            ? TypeKind::VARCHAR : TypeKind::BIGINT;
    if ((name != "content" && name != "file_path" &&
            name != "file_format" && name != "spec_id" &&
            name != "record_count" && name != "file_size_in_bytes" &&
            name != "column_sizes" && name != "split_offsets" && name != "equality_ids" &&
            name != "sort_order_id" && name != "partition" && name != "first_row_id") ||
        !names.insert(name).second || outputType->childAt(index)->kind() != expected) {
      throw std::runtime_error("Unsupported Iceberg $files output column: " + name);
    }
    if (expected == TypeKind::ARRAY && outputType->childAt(index)->asArray()
            .elementType()->kind() != (name == "split_offsets"
                ? TypeKind::BIGINT : TypeKind::INTEGER)) {
      throw std::runtime_error("Unsupported Iceberg $files array element type: " + name);
    }
    if (expected == TypeKind::MAP &&
        (outputType->childAt(index)->asMap().keyType()->kind() != TypeKind::INTEGER ||
         outputType->childAt(index)->asMap().valueType()->kind() != TypeKind::BIGINT)) {
      throw std::runtime_error("Unsupported Iceberg $files map key/value type: " + name);
    }
  }
  if (names.contains("column_sizes")) {
    validateColumnSizesSchema(writerSchema);
  }
}

}  // namespace

IcebergManifestEntryReader::IcebergManifestEntryReader(
    const std::string& childUri, uint64_t childLength,
    const std::string& childSha256, const std::string& listUri,
    uint64_t listLength, const std::string& listSha256,
    uint64_t snapshotId, uint64_t sequenceNumber, int32_t formatVersion,
    int32_t specId, int32_t manifestContent,
    facebook::velox::RowTypePtr outputType,
    facebook::velox::memory::MemoryPool* pool, bool entries,
    const std::vector<split::IcebergManifestPartitionSpec>& partitionSpecs)
    : spec_id_(specId), manifest_content_(manifestContent),
      format_version_(formatVersion), entries_(entries),
      snapshot_sequence_number_(static_cast<int64_t>(sequenceNumber)),
      output_type_(std::move(outputType)), pool_(pool) {
  if (!output_type_ || !pool_ || childUri.empty() || listUri.empty() ||
      childUri == listUri || childLength == 0 || listLength == 0 ||
      childSha256.size() != 32 || listSha256.size() != 32 ||
      (formatVersion != 2 && formatVersion != 3) || specId < 0 ||
      (manifestContent != 0 && manifestContent != 1)) {
    throw std::invalid_argument("Invalid Iceberg files reader selection");
  }
  auto list = openPinned(listUri, listLength, listSha256);
  const auto parent = verifyParentMembership(*list, childUri, childLength,
      snapshotId, sequenceNumber, formatVersion, specId, manifestContent, entries_);
  inherited_snapshot_id_ = parent.addedSnapshotId;
  inherited_sequence_number_ = parent.sequenceNumber;
  inherited_first_row_id_ = parent.firstRowId;
  child_ = openPinned(childUri, childLength, childSha256);
  if (headerLong(*child_, "format-version") != formatVersion ||
      headerLong(*child_, "partition-spec-id") != specId ||
      child_->metadataValue("content") !=
          (manifestContent == 0 ? "data" : "deletes")) {
    throw std::runtime_error("Pinned Iceberg child manifest header differs");
  }
  validateOutputSchema();
  auto fileOutput = entries_
      ? (output_type_->containsChild("data_file")
          ? std::dynamic_pointer_cast<const facebook::velox::RowType>(output_type_->findChild("data_file"))
          : nullptr)
      : output_type_;
  if (fileOutput && fileOutput->containsChild("partition")) {
    const auto& fileSchema = nonNullAvroSchema(avroField(child_->writerSchema(), "data_file", 2));
    const auto& partitionSchema = nonNullAvroSchema(avroField(fileSchema, "partition", 102));
    partition_projection_ = std::make_unique<IcebergPartitionProjection>(
        partitionSchema, specId, partitionSpecs,
        std::dynamic_pointer_cast<const facebook::velox::RowType>(fileOutput->findChild("partition")));
  }
}

IcebergManifestEntryReader::~IcebergManifestEntryReader() = default;

void IcebergManifestEntryReader::validateOutputSchema() const {
  if (!entries_) {
    if (output_type_->containsChild("first_row_id")) {
      throw std::runtime_error("Iceberg $files does not declare first_row_id");
    }
    validateFileOutput(output_type_, child_->writerSchema());
    return;
  }
  using facebook::velox::TypeKind;
  std::unordered_set<std::string> names;
  for (size_t index = 0; index < output_type_->size(); ++index) {
    const auto& name = output_type_->nameOf(index);
    const auto& type = output_type_->childAt(index);
    if (!names.insert(name).second) throw std::runtime_error("Duplicate Iceberg entry output");
    if (name == "data_file" && type->kind() == TypeKind::ROW) {
      validateFileOutput(std::dynamic_pointer_cast<const facebook::velox::RowType>(type),
          child_->writerSchema());
    } else if ((name == "status" && type->kind() == TypeKind::INTEGER) ||
        ((name == "snapshot_id" || name == "sequence_number" || name == "file_sequence_number") &&
         type->kind() == TypeKind::BIGINT)) {
      continue;
    } else {
      throw std::runtime_error("Unsupported Iceberg $entries output column: " + name);
    }
  }
}

facebook::velox::RowVectorPtr IcebergManifestEntryReader::next(uint64_t maxRows) {
  if (maxRows == 0) throw std::invalid_argument("Metadata scan batch size must be positive");
  std::vector<FileRow> rows;
  rows.reserve(std::min(maxRows, kMaxBatchRows));
  while (rows.size() < maxRows && rows.size() < kMaxBatchRows) {
    auto record = child_->nextRecord();
    if (!record) break;
    const auto status = requiredInt(*record, "status");
    if (status < 0 || status > 2) {
      throw std::runtime_error("Invalid Iceberg manifest entry status");
    }
    if (status != 2 || entries_) {
      auto file = decodeLiveEntry(*record, spec_id_, manifest_content_, partition_projection_ != nullptr);
      if (entries_) {
        file.status = status;
        if (!record->count("snapshot_id") || !record->count("sequence_number") ||
            !record->count("file_sequence_number")) {
          throw std::runtime_error("Iceberg manifest entry lineage fields are missing");
        }
        const auto& snapshot = (*record)["snapshot_id"];
        const auto& sequence = (*record)["sequence_number"];
        const auto& fileSequence = (*record)["file_sequence_number"];
        if ((!snapshot.isNull() && !snapshot.isInt()) ||
            (!sequence.isNull() && !sequence.isInt()) ||
            (!fileSequence.isNull() && !fileSequence.isInt()) ||
            (snapshot.isNull() && status != 1) ||
            (sequence.isNull() && status != 1) ||
            (fileSequence.isNull() && status != 1)) {
          throw std::runtime_error("Invalid Iceberg manifest entry lineage");
        }
        file.snapshotId = snapshot.isNull()
            ? inherited_snapshot_id_ : snapshot.asInt();
        file.sequenceNumber = sequence.isNull()
            ? inherited_sequence_number_ : sequence.asInt();
        file.fileSequenceNumber = fileSequence.isNull()
            ? inherited_sequence_number_ : fileSequence.asInt();
        if (file.snapshotId <= 0 || file.sequenceNumber < 0 ||
            file.fileSequenceNumber < 0 ||
            file.sequenceNumber > snapshot_sequence_number_ ||
            file.fileSequenceNumber > snapshot_sequence_number_) {
          throw std::runtime_error("Invalid Iceberg manifest entry lineage value");
        }
        if (format_version_ == 3) {
          const auto& dataFile = (*record)["data_file"];
          if (dataFile.count("first_row_id") && !dataFile["first_row_id"].isNull()) {
            const auto first = requiredLong(dataFile, "first_row_id");
            if (first < 0 || file.content != 0) {
              throw std::runtime_error("Invalid Iceberg data file first row ID");
            }
            file.firstRowId = first;
          } else if (file.content == 0 && inherited_first_row_id_) {
            if (*inherited_first_row_id_ >
                    std::numeric_limits<int64_t>::max() - inherited_row_offset_ ||
                file.recordCount >
                    std::numeric_limits<int64_t>::max() - inherited_row_offset_) {
              throw std::runtime_error("Iceberg inherited first row ID overflow");
            }
            file.firstRowId = *inherited_first_row_id_ + inherited_row_offset_;
            inherited_row_offset_ += file.recordCount;
          }
        }
      }
      rows.push_back(std::move(file));
    }
  }
  if (rows.empty()) return nullptr;
  if (!entries_) return projectFileRows(rows, output_type_, pool_, partition_projection_.get());
  std::vector<facebook::velox::VectorPtr> columns;
  for (size_t index = 0; index < output_type_->size(); ++index) {
    const auto& name = output_type_->nameOf(index);
    const auto& type = output_type_->childAt(index);
    if (name == "data_file") {
      columns.push_back(projectFileRows(rows,
          std::dynamic_pointer_cast<const facebook::velox::RowType>(type),
          pool_, partition_projection_.get()));
      continue;
    }
    auto vector = facebook::velox::BaseVector::create(type, rows.size(), pool_);
    for (size_t row = 0; row < rows.size(); ++row) {
      const auto& file = rows[row];
      if (name == "status") vector->asFlatVector<int32_t>()->set(row, file.status);
      else vector->asFlatVector<int64_t>()->set(row,
          name == "snapshot_id" ? file.snapshotId
          : name == "file_sequence_number" ? file.fileSequenceNumber : file.sequenceNumber);
    }
    columns.push_back(std::move(vector));
  }
  return std::make_shared<facebook::velox::RowVector>(
      pool_, output_type_, nullptr, rows.size(), std::move(columns));
}

}  // namespace pangrova::velox
