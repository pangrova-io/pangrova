#include "velox/connector/IcebergManifestEntryReader.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "split/PathUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kMaxBatchRows = 1024;

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

void verifyParentMembership(IcebergAvroContainerReader& list,
    const std::string& childUri, uint64_t childLength,
    uint64_t snapshotId, uint64_t sequenceNumber, int32_t formatVersion,
    int32_t specId, int32_t content) {
  if (snapshotId == 0 || headerLong(list, "snapshot-id") !=
          static_cast<int64_t>(snapshotId) ||
      headerLong(list, "sequence-number") != static_cast<int64_t>(sequenceNumber) ||
      headerLong(list, "format-version") != formatVersion) {
    throw std::runtime_error("Pinned Iceberg manifest-list header differs");
  }
  int matches = 0;
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
    ++matches;
  }
  if (matches != 1) {
    throw std::runtime_error("Pinned Iceberg child is missing or duplicated in manifest list");
  }
}

struct FileRow {
  int32_t content;
  std::string path;
  std::string format;
  int32_t specId;
  int64_t recordCount;
  int64_t fileSize;
};

FileRow decodeLiveEntry(const folly::dynamic& entry,
    int32_t specId, int32_t manifestContent) {
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
  return {content, requiredText(file, "file_path"),
      requiredText(file, "file_format"), specId, recordCount, fileSize};
}

}  // namespace

IcebergManifestEntryReader::IcebergManifestEntryReader(
    const std::string& childUri, uint64_t childLength,
    const std::string& childSha256, const std::string& listUri,
    uint64_t listLength, const std::string& listSha256,
    uint64_t snapshotId, uint64_t sequenceNumber, int32_t formatVersion,
    int32_t specId, int32_t manifestContent,
    facebook::velox::RowTypePtr outputType,
    facebook::velox::memory::MemoryPool* pool)
    : spec_id_(specId), manifest_content_(manifestContent),
      output_type_(std::move(outputType)), pool_(pool) {
  if (!output_type_ || !pool_ || childUri.empty() || listUri.empty() ||
      childUri == listUri || childLength == 0 || listLength == 0 ||
      childSha256.size() != 32 || listSha256.size() != 32 ||
      (formatVersion != 2 && formatVersion != 3) || specId < 0 ||
      (manifestContent != 0 && manifestContent != 1)) {
    throw std::invalid_argument("Invalid Iceberg files reader selection");
  }
  auto list = openPinned(listUri, listLength, listSha256);
  verifyParentMembership(*list, childUri, childLength, snapshotId,
      sequenceNumber, formatVersion, specId, manifestContent);
  child_ = openPinned(childUri, childLength, childSha256);
  if (headerLong(*child_, "format-version") != formatVersion ||
      headerLong(*child_, "partition-spec-id") != specId ||
      child_->metadataValue("content") !=
          (manifestContent == 0 ? "data" : "deletes")) {
    throw std::runtime_error("Pinned Iceberg child manifest header differs");
  }
  validateOutputSchema();
}

void IcebergManifestEntryReader::validateOutputSchema() const {
  using facebook::velox::TypeKind;
  std::unordered_set<std::string> names;
  for (size_t index = 0; index < output_type_->size(); ++index) {
    const auto& name = output_type_->nameOf(index);
    const auto expected = name == "content" || name == "spec_id"
        ? TypeKind::INTEGER
        : name == "file_path" || name == "file_format"
            ? TypeKind::VARCHAR : TypeKind::BIGINT;
    if ((name != "content" && name != "file_path" &&
            name != "file_format" && name != "spec_id" &&
            name != "record_count" && name != "file_size_in_bytes") ||
        !names.insert(name).second || output_type_->childAt(index)->kind() != expected) {
      throw std::runtime_error("Unsupported Iceberg $files output column: " + name);
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
    if (status != 2) {
      rows.push_back(decodeLiveEntry(*record, spec_id_, manifest_content_));
    }
  }
  if (rows.empty()) return nullptr;
  std::vector<facebook::velox::VectorPtr> columns;
  columns.reserve(output_type_->size());
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    auto vector = facebook::velox::BaseVector::create(
        output_type_->childAt(column), rows.size(), pool_);
    for (size_t row = 0; row < rows.size(); ++row) {
      const auto& file = rows[row];
      if (name == "content" || name == "spec_id") {
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
      pool_, output_type_, nullptr, rows.size(), std::move(columns));
}

}  // namespace pangrova::velox
