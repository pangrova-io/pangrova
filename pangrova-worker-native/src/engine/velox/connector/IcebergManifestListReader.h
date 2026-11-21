#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "engine/split/SplitConfig.h"
#include "engine/velox/connector/AvroContainerReader.h"
#include "velox/vector/ComplexVector.h"
#include "engine/velox/connector/MetadataReadContext.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace pangrova::velox {

// Converts one SHA-pinned Iceberg v2/v3 manifest list into the SDK's typed
// $manifests rows. Unsupported bound representations fail before output.
struct IcebergManifestListReadUnit {
  MetadataObjectReference object;
  uint64_t snapshotId;
  uint64_t sequenceNumber;
  int32_t formatVersion;
  std::vector<split::IcebergManifestPartitionSpec> specs{};
};

class IcebergManifestListReader final {
 public:
  IcebergManifestListReader(const IcebergManifestListReadUnit& unit,
      facebook::velox::RowTypePtr outputType, facebook::velox::memory::MemoryPool* pool,
      const MetadataReadContext& reads = MetadataReadContext::unscoped());

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  facebook::velox::VectorPtr summaries(
      const std::vector<folly::dynamic>& rows,
      const facebook::velox::TypePtr& arrayType) const;
  void validateOutputSchema() const;

  std::unique_ptr<AvroContainerReader> container_;
  std::unordered_map<int32_t, std::vector<split::IcebergManifestPartitionField>> specs_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
};

}  // namespace pangrova::velox
