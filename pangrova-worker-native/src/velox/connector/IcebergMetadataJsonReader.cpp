#include "velox/connector/IcebergMetadataJsonReader.h"

#include <algorithm>
#include <charconv>
#include <stdexcept>
#include <string_view>

#include "simdjson.h"
#include "split/PathUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox {
namespace {

constexpr uint64_t kReadChunkBytes = 64 * 1024;
constexpr size_t kMaxJsonObjectBytes = 64 * 1024 * 1024;

int64_t parseInteger(std::string_view value) {
  int64_t result = 0;
  const auto [end, error] = std::from_chars(
      value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::runtime_error("Invalid Iceberg metadata integer");
  }
  return result;
}

std::optional<int64_t> optionalLong(
    const simdjson::dom::element& object, const char* key) {
  auto field = object[key];
  if (field.error() == simdjson::NO_SUCH_FIELD) {
    return std::nullopt;
  }
  if (field.error()) {
    throw std::runtime_error("Invalid Iceberg metadata field");
  }
  if (field.value().is_null()) {
    return std::nullopt;
  }
  return field.value().get_int64().value();
}

int64_t requiredLong(const simdjson::dom::element& object, const char* key) {
  auto value = optionalLong(object, key);
  if (!value.has_value()) {
    throw std::runtime_error(std::string("Missing Iceberg metadata field: ") + key);
  }
  return *value;
}

simdjson::dom::element parseObject(
    const std::string& json, simdjson::dom::parser& parser,
    simdjson::padded_string& padded) {
  padded = simdjson::padded_string(json);
  auto result = parser.parse(padded);
  if (result.error() || !result.value().is_object()) {
    throw std::runtime_error("Invalid Iceberg metadata JSON object");
  }
  return result.value();
}

}  // namespace

// Bounded random-access cursor over a Velox filesystem object. Metadata URIs
// may be local, S3 or any other filesystem registered with the Native Worker.
class MetadataJsonCursor final {
 public:
  explicit MetadataJsonCursor(std::shared_ptr<facebook::velox::ReadFile> file)
      : file_(std::move(file)), size_(file_->size()),
        buffer_(kReadChunkBytes, '\0') {}

  uint64_t size() const { return size_; }
  uint64_t position() const { return position_; }
  void seek(uint64_t position) {
    if (position > size_) {
      throw std::runtime_error("Iceberg metadata cursor out of range");
    }
    position_ = position;
  }

  std::optional<char> get() {
    if (position_ == size_) {
      return std::nullopt;
    }
    if (position_ < buffer_begin_ || position_ >= buffer_begin_ + valid_) {
      buffer_begin_ = position_;
      valid_ = std::min<uint64_t>(buffer_.size(), size_ - position_);
      file_->pread(buffer_begin_, valid_, buffer_.data());
    }
    return buffer_[position_++ - buffer_begin_];
  }

  std::optional<std::string> nextObject(uint64_t arrayEnd) {
    while (position_ < arrayEnd) {
      const char next = *get();
      if (next == ',' || next == ' ' || next == '\n' || next == '\r' ||
          next == '\t') {
        continue;
      }
      if (next != '{') {
        throw std::runtime_error("Iceberg metadata array contains non-object");
      }
      std::string object(1, next);
      int depth = 1;
      bool inString = false;
      bool escaped = false;
      while (depth > 0 && position_ <= arrayEnd) {
        auto character = get();
        if (!character.has_value() || object.size() >= kMaxJsonObjectBytes) {
          throw std::runtime_error("Iceberg metadata JSON object is truncated or too large");
        }
        const char c = *character;
        object.push_back(c);
        if (inString) {
          if (escaped) escaped = false;
          else if (c == '\\') escaped = true;
          else if (c == '"') inString = false;
        } else if (c == '"') {
          inString = true;
        } else if (c == '{') {
          ++depth;
        } else if (c == '}') {
          --depth;
        }
      }
      if (depth != 0) {
        throw std::runtime_error("Unterminated Iceberg metadata JSON object");
      }
      return object;
    }
    return std::nullopt;
  }

 private:
  std::shared_ptr<facebook::velox::ReadFile> file_;
  uint64_t size_;
  uint64_t position_ = 0;
  uint64_t buffer_begin_ = 0;
  uint64_t valid_ = 0;
  std::string buffer_;
};

IcebergMetadataJsonReader::IcebergMetadataJsonReader(
    const std::string& uri, uint64_t expectedLength,
    split::SystemTableKind kind, facebook::velox::RowTypePtr outputType,
    facebook::velox::memory::MemoryPool* pool)
    : kind_(kind), output_type_(std::move(outputType)), pool_(pool) {
  const auto path = split::metadataObjectPath(uri);
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!filesystem) {
    throw std::runtime_error("No Native filesystem for Iceberg metadata URI");
  }
  file_ = filesystem->openFileForRead(path);
  cursor_ = std::make_unique<MetadataJsonCursor>(file_);
  if (expectedLength == 0 || cursor_->size() != expectedLength) {
    throw std::runtime_error("Pinned Iceberg metadata object length changed");
  }
  validateOutputSchema();
  indexDocument();
  if (kind_ == split::SystemTableKind::ICEBERG_HISTORY) {
    loadAncestors();
  }
  cursor_->seek(kind_ == split::SystemTableKind::ICEBERG_HISTORY
                    ? history_.begin : snapshots_.begin);
}

IcebergMetadataJsonReader::~IcebergMetadataJsonReader() = default;

void IcebergMetadataJsonReader::validateOutputSchema() const {
  for (size_t i = 0; i < output_type_->size(); ++i) {
    const auto& name = output_type_->nameOf(i);
    auto type = output_type_->childAt(i)->kind();
    const bool common = (name == "snapshot_id" || name == "parent_id");
    const bool snapshots = kind_ == split::SystemTableKind::ICEBERG_SNAPSHOTS &&
        ((name == "timestamp_ms" && type == facebook::velox::TypeKind::BIGINT) ||
         ((name == "operation" || name == "summary") &&
          type == facebook::velox::TypeKind::VARCHAR));
    const bool history = kind_ == split::SystemTableKind::ICEBERG_HISTORY &&
        ((name == "made_current_at" && type == facebook::velox::TypeKind::BIGINT) ||
         (name == "is_current_ancestor" &&
          type == facebook::velox::TypeKind::BOOLEAN));
    if (!(common && type == facebook::velox::TypeKind::BIGINT) &&
        !snapshots && !history) {
      throw std::runtime_error("Invalid Iceberg system table output schema: " + name);
    }
  }
}

void IcebergMetadataJsonReader::indexDocument() {
  int objectDepth = 0;
  int arrayDepth = 0;
  bool inString = false;
  bool escaped = false;
  bool captureKey = false;
  bool expectingKey = false;
  bool currentNumber = false;
  std::string key;
  std::string token;
  std::string number;
  std::string activeArray;
  cursor_->seek(0);
  while (auto next = cursor_->get()) {
    const char c = *next;
    if (currentNumber) {
      if ((c >= '0' && c <= '9') || c == '-') {
        number.push_back(c);
        continue;
      }
      if (c != ' ' && c != '\n' && c != '\r' && c != '\t') {
        current_snapshot_id_ = parseInteger(number);
        currentNumber = false;
        number.clear();
      } else if (!number.empty()) {
        continue;
      }
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (c == '\\') escaped = true;
      else if (c == '"') {
        inString = false;
        if (captureKey) {
          key = std::move(token);
          token.clear();
          expectingKey = false;
        }
      } else if (captureKey) token.push_back(c);
      continue;
    }
    if (c == '"') {
      inString = true;
      captureKey = objectDepth == 1 && arrayDepth == 0 && expectingKey;
    } else if (c == '{') {
      ++objectDepth;
      if (objectDepth == 1) expectingKey = true;
    } else if (c == '}') {
      --objectDepth;
    } else if (c == '[') {
      if (objectDepth == 1 && arrayDepth == 0) {
        activeArray = key;
        if (key == "snapshots") snapshots_.begin = cursor_->position();
        if (key == "snapshot-log") history_.begin = cursor_->position();
      }
      ++arrayDepth;
    } else if (c == ']') {
      --arrayDepth;
      if (arrayDepth == 0) {
        if (activeArray == "snapshots") {
          snapshots_.end = cursor_->position() - 1;
          snapshots_.found = true;
        } else if (activeArray == "snapshot-log") {
          history_.end = cursor_->position() - 1;
          history_.found = true;
        }
        activeArray.clear();
      }
    } else if (c == ':' && objectDepth == 1 && arrayDepth == 0 &&
               key == "current-snapshot-id") {
      currentNumber = true;
    } else if (c == ',' && objectDepth == 1 && arrayDepth == 0) {
      expectingKey = true;
      key.clear();
    }
    if (objectDepth < 0 || arrayDepth < 0) {
      throw std::runtime_error("Malformed Iceberg metadata JSON structure");
    }
  }
  if (objectDepth != 0 || arrayDepth != 0) {
    throw std::runtime_error("Malformed Iceberg metadata JSON structure");
  }
  // Fresh Iceberg tables may omit empty snapshot arrays altogether. They have
  // no current snapshot and must scan as zero rows, not as a missing table.
  if (current_snapshot_id_ == -1) {
    if (!snapshots_.found) snapshots_.found = true;
    if (!history_.found) history_.found = true;
  }
  if (!snapshots_.found ||
      (kind_ == split::SystemTableKind::ICEBERG_HISTORY && !history_.found)) {
    throw std::runtime_error("Iceberg metadata lacks required snapshot entries");
  }
}

void IcebergMetadataJsonReader::loadAncestors() {
  cursor_->seek(snapshots_.begin);
  simdjson::dom::parser parser;
  simdjson::padded_string padded;
  while (auto object = cursor_->nextObject(snapshots_.end)) {
    auto snapshot = parseObject(*object, parser, padded);
    auto id = requiredLong(snapshot, "snapshot-id");
    auto parent = optionalLong(snapshot, "parent-snapshot-id");
    if (!parents_.emplace(id, parent).second) {
      throw std::runtime_error("Duplicate Iceberg snapshot ID in metadata");
    }
  }
  for (int64_t id = current_snapshot_id_; id != -1;) {
    if (!ancestors_.insert(id).second) {
      throw std::runtime_error("Cycle in Iceberg snapshot ancestry");
    }
    auto found = parents_.find(id);
    if (found == parents_.end()) {
      throw std::runtime_error("Current Iceberg snapshot is missing from metadata");
    }
    id = found->second.value_or(-1);
  }
}

IcebergMetadataJsonReader::Row IcebergMetadataJsonReader::parseRow(
    const std::string& object) const {
  simdjson::dom::parser parser;
  simdjson::padded_string padded;
  auto value = parseObject(object, parser, padded);
  Row row;
  row.snapshotId = requiredLong(value, "snapshot-id");
  if (kind_ == split::SystemTableKind::ICEBERG_SNAPSHOTS) {
    row.parentId = optionalLong(value, "parent-snapshot-id");
    row.timestampMs = requiredLong(value, "timestamp-ms");
    auto summary = value["summary"];
    if (!summary.error() && !summary.value().is_null()) {
      row.summary = simdjson::minify(summary.value());
      auto operation = summary.value()["operation"];
      if (!operation.error() && !operation.value().is_null()) {
        row.operation = std::string(operation.value().get_string().value());
      }
    }
  } else {
    row.timestampMs = requiredLong(value, "timestamp-ms");
    auto found = parents_.find(*row.snapshotId);
    if (found == parents_.end()) {
      throw std::runtime_error("Iceberg history references missing snapshot");
    }
    row.parentId = found->second;
    row.currentAncestor = ancestors_.contains(*row.snapshotId);
  }
  return row;
}

facebook::velox::RowVectorPtr IcebergMetadataJsonReader::materialize(
    const std::vector<Row>& rows) const {
  auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(output_type_, rows.size(), pool_));
  for (size_t column = 0; column < output_type_->size(); ++column) {
    const auto& name = output_type_->nameOf(column);
    auto child = vector->childAt(column);
    for (size_t index = 0; index < rows.size(); ++index) {
      const auto& row = rows[index];
      if (name == "is_current_ancestor") {
        child->asFlatVector<bool>()->set(index, row.currentAncestor.value());
      } else if (name == "operation" || name == "summary") {
        const auto& text = name == "operation" ? row.operation : row.summary;
        if (text) child->asFlatVector<facebook::velox::StringView>()->set(
            index, facebook::velox::StringView(*text));
        else child->setNull(index, true);
      } else {
        const auto& value = name == "snapshot_id" ? row.snapshotId :
            name == "parent_id" ? row.parentId : row.timestampMs;
        if (value) child->asFlatVector<int64_t>()->set(index, *value);
        else child->setNull(index, true);
      }
    }
  }
  return vector;
}

facebook::velox::RowVectorPtr IcebergMetadataJsonReader::next(uint64_t maxRows) {
  if (maxRows == 0) {
    throw std::invalid_argument("Metadata scan batch size must be positive");
  }
  const auto end = kind_ == split::SystemTableKind::ICEBERG_HISTORY
      ? history_.end : snapshots_.end;
  std::vector<Row> rows;
  rows.reserve(std::min<uint64_t>(maxRows, 4096));
  while (rows.size() < maxRows) {
    auto object = cursor_->nextObject(end);
    if (!object) break;
    rows.push_back(parseRow(*object));
  }
  return rows.empty() ? nullptr : materialize(rows);
}

}  // namespace pangrova::velox
