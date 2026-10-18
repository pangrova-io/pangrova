#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "velox/connector/IcebergAvroContainerReader.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace pangrova::velox {

// Scans live entries from one child manifest pinned by its parent list.
// Only explicitly verified fixed scalar and array fields are supported initially.
class IcebergManifestEntryReader final {
 public:
  IcebergManifestEntryReader(
      const std::string& childUri, uint64_t childLength,
      const std::string& childSha256, const std::string& listUri,
      uint64_t listLength, const std::string& listSha256,
      uint64_t snapshotId, uint64_t sequenceNumber, int32_t formatVersion,
      int32_t specId, int32_t manifestContent,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::memory::MemoryPool* pool,
      bool entries = false);

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  void validateOutputSchema() const;

  std::unique_ptr<IcebergAvroContainerReader> child_;
  int32_t spec_id_;
  int32_t manifest_content_;
  int32_t format_version_;
  bool entries_;
  int64_t inherited_snapshot_id_ = 0;
  int64_t inherited_sequence_number_ = 0;
  std::optional<int64_t> inherited_first_row_id_;
  int64_t inherited_row_offset_ = 0;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
};

}  // namespace pangrova::velox
