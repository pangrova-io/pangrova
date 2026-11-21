#pragma once

#include <cstdint>
#include <string>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::memory {
class MemoryPool;
}

namespace pangrova::velox {

/** Reads one pinned local Paimon branch directory status on the Native Worker. */
class PaimonBranchDirectoryReader final {
 public:
  PaimonBranchDirectoryReader(std::string uri, std::string name,
      int64_t expectedMtimeMs, uint64_t expectedDevice, uint64_t expectedInode,
      int32_t zoneOffsetSeconds,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::memory::MemoryPool* pool);

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  std::string uri_;
  std::string name_;
  int64_t expected_mtime_ms_;
  uint64_t expected_device_;
  uint64_t expected_inode_;
  int32_t zone_offset_seconds_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
  bool emitted_ = false;
};

}  // namespace pangrova::velox
