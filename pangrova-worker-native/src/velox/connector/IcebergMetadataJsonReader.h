#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "split/SplitConfig.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {
class ReadFile;
namespace memory { class MemoryPool; }
}  // namespace facebook::velox

namespace pangrova::velox {

class MetadataJsonCursor;

// Streams a pinned Iceberg metadata object from the worker's shared storage.
// Only one JSON object is buffered at a time; Velox operators process its
// bounded RowVector batches like any other scan source.
class IcebergMetadataJsonReader final {
 public:
  IcebergMetadataJsonReader(
      const std::string& uri,
      uint64_t expectedLength,
      split::SystemTableKind kind,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::memory::MemoryPool* pool);
  ~IcebergMetadataJsonReader();

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  struct ArrayRange {
    uint64_t begin = 0;
    uint64_t end = 0;
    bool found = false;
  };
  struct Row {
    std::optional<int64_t> snapshotId;
    std::optional<int64_t> parentId;
    std::optional<int64_t> timestampMs;
    std::optional<std::string> operation;
    std::optional<std::string> summary;
    std::optional<bool> currentAncestor;
  };

  void indexDocument();
  void validateOutputSchema() const;
  void loadAncestors();
  Row parseRow(const std::string& object) const;
  facebook::velox::RowVectorPtr materialize(const std::vector<Row>& rows) const;

  std::shared_ptr<facebook::velox::ReadFile> file_;
  std::unique_ptr<MetadataJsonCursor> cursor_;
  split::SystemTableKind kind_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
  ArrayRange snapshots_;
  ArrayRange history_;
  int64_t current_snapshot_id_ = -1;
  std::unordered_map<int64_t, std::optional<int64_t>> parents_;
  std::unordered_set<int64_t> ancestors_;
};

}  // namespace pangrova::velox
