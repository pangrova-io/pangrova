#pragma once

#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/memory/ColumnarMemoryPool.h"
#include "engine/result/ResultColumnType.h"
#include "engine/result/ResultPayloadQueue.h"
#include "substrait/type.pb.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::result {

class ResultPayloadProducer final {
 public:
  ResultPayloadProducer(const substrait::NamedStruct& canonical,
      std::shared_ptr<arrow::Schema> wireSchema,
      std::vector<ResultColumnVectorDimension> vectorColumns,
      std::shared_ptr<memory::ColumnarMemoryPool> memory,
      std::shared_ptr<ResultOwnershipState> ownership,
      int64_t maximumFrameBytes = columnar::kMaximumBatchStreamBytes);

  arrow::Result<std::vector<ResultPayloadQueue::Frame>> prepare(
      const facebook::velox::RowVectorPtr& rows, std::shared_ptr<void> source) const;

 private:
  facebook::velox::RowTypePtr canonical_type_;
  std::shared_ptr<arrow::Schema> wire_schema_;
  std::vector<ResultColumnVectorDimension> vector_columns_;
  std::shared_ptr<memory::ColumnarMemoryPool> memory_;
  std::shared_ptr<ResultOwnershipState> ownership_;
  int64_t maximum_frame_bytes_;
};

}  // namespace pangrova::result
