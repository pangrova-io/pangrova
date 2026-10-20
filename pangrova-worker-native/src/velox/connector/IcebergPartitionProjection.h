#pragma once

#include <string>
#include <vector>

#include "folly/dynamic.h"
#include "split/SplitConfig.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Projects an Avro partition tuple into the table's unified partition type by
// stable Iceberg field ID. Fields absent from an older spec become null.
class IcebergPartitionProjection final {
 public:
  IcebergPartitionProjection(const folly::dynamic& writerSchema, int32_t specId,
      const std::vector<split::IcebergManifestPartitionSpec>& specs,
      facebook::velox::RowTypePtr outputType);

  facebook::velox::RowVectorPtr project(
      const std::vector<const folly::dynamic*>& rows,
      facebook::velox::memory::MemoryPool* pool) const;

 private:
  struct Field {
    std::string writerName;
    folly::dynamic schema;
  };
  std::vector<Field> fields_;
  facebook::velox::RowTypePtr output_type_;
};

}  // namespace pangrova::velox
