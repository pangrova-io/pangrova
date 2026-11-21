#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/split/SplitConfig.h"
#include "engine/velox/connector/AvroContainerReader.h"
#include "velox/vector/ComplexVector.h"
#include "engine/velox/connector/MetadataReadContext.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace pangrova::velox {
class IcebergPartitionProjection;

// Scans live files or all entries from one child manifest pinned by its parent list.
// Entry lineage surrounds a typed data_file row; file maps, arrays and
// field-ID-based partition projections share the ordinary file scan renderer.
struct IcebergManifestEntryReadUnit {
  MetadataObjectReference child;
  MetadataObjectReference list;
  uint64_t snapshotId;
  uint64_t sequenceNumber;
  int32_t formatVersion;
  int32_t specId;
  int32_t manifestContent;
  bool entries = false;
  std::vector<split::IcebergManifestPartitionSpec> partitionSpecs{};
};

class IcebergManifestEntryReader final {
 public:
  IcebergManifestEntryReader(const IcebergManifestEntryReadUnit& unit,
      facebook::velox::RowTypePtr outputType, facebook::velox::memory::MemoryPool* pool,
      const MetadataReadContext& reads = MetadataReadContext::unscoped());

  ~IcebergManifestEntryReader();

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  void validateOutputSchema() const;

  std::unique_ptr<AvroContainerReader> child_;
  std::unique_ptr<IcebergPartitionProjection> partition_projection_;
  int32_t spec_id_;
  int32_t manifest_content_;
  int32_t format_version_;
  bool entries_;
  int64_t snapshot_sequence_number_;
  int64_t inherited_snapshot_id_ = 0;
  int64_t inherited_sequence_number_ = 0;
  std::optional<int64_t> inherited_first_row_id_;
  int64_t inherited_row_offset_ = 0;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
};

}  // namespace pangrova::velox
