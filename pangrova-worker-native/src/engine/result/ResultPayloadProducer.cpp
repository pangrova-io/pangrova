#include "engine/result/ResultPayloadProducer.h"

#include <stdexcept>

#include "capabilities/capability/LogicalTypeContract.h"
#include "engine/columnar/CanonicalArrowValidation.h"
#include "engine/velox/serde/CanonicalArrowBatchBridge.h"
#include "engine/velox/serde/CanonicalArrowSchema.h"
#include "engine/velox/serde/CanonicalResultNullability.h"
#include "engine/velox/serde/CanonicalVectorNames.h"

namespace pangrova::result {
namespace {

arrow::Status prepareFrames(const arrow::RecordBatch& batch,
    const arrow::ipc::IpcWriteOptions& options,
    const std::shared_ptr<memory::ColumnarMemoryPool>& memory,
    const std::shared_ptr<ResultOwnershipState>& ownership, const std::shared_ptr<void>& source,
    int64_t maximumFrameBytes, std::vector<ResultPayloadQueue::Frame>& frames) {
  {
    arrow::ipc::IpcPayload payload;
    ARROW_RETURN_NOT_OK(arrow::ipc::GetRecordBatchPayload(batch, options, &payload));
    const auto bytes = arrow::ipc::GetPayloadSize(payload, options);
    if (bytes <= 0) return arrow::Status::Invalid("Result payload has no positive wire size");
    if (bytes <= maximumFrameBytes) {
      frames.emplace_back(std::move(payload), bytes, memory, ownership, source);
      return arrow::Status::OK();
    }
  }
  if (batch.num_rows() <= 1) {
    return arrow::Status::CapacityError("A single result row exceeds the Arrow Flight wire ceiling");
  }
  // The whole exported batch is validated before splitting. Only the sliced
  // messages whose exact wire lengths fit reach the queue.
  const auto middle = batch.num_rows() / 2;
  ARROW_RETURN_NOT_OK(prepareFrames(*batch.Slice(0, middle), options, memory, ownership, source, maximumFrameBytes, frames));
  return prepareFrames(*batch.Slice(middle), options, memory, ownership, source, maximumFrameBytes, frames);
}

}  // namespace

ResultPayloadProducer::ResultPayloadProducer(const substrait::NamedStruct& canonical,
    std::shared_ptr<arrow::Schema> wireSchema,
    std::vector<ResultColumnVectorDimension> vectorColumns,
    std::shared_ptr<memory::ColumnarMemoryPool> memory,
    std::shared_ptr<ResultOwnershipState> ownership, int64_t maximumFrameBytes)
    : canonical_type_(velox::admittedCanonicalResultType(canonical)),
      wire_schema_(std::move(wireSchema)), vector_columns_(std::move(vectorColumns)),
      memory_(std::move(memory)), ownership_(std::move(ownership)),
      maximum_frame_bytes_(maximumFrameBytes) {
  if (maximum_frame_bytes_ <= 0 || maximum_frame_bytes_ > columnar::kMaximumBatchStreamBytes) {
    throw std::invalid_argument("Result payload ceiling is outside the supported wire range");
  }
}

arrow::Result<std::vector<ResultPayloadQueue::Frame>> ResultPayloadProducer::prepare(
    const facebook::velox::RowVectorPtr& rows, std::shared_ptr<void> source) const {
  const auto canonical = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      velox::bindCanonicalVectorNames(rows, canonical_type_));
  const auto batch = velox::bindCanonicalArrowSchema(
      velox::exportArrowRecordBatch(canonical, memory_->source().get()), wire_schema_);
  ARROW_RETURN_NOT_OK(columnar::validateCanonicalArrowBatch(*batch, *wire_schema_));
  for (const auto& column : vector_columns_) {
    capability::validateVectorResultArray(column.logical_type, *batch->column(column.column_index));
  }
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.memory_pool = memory_.get();
  std::vector<ResultPayloadQueue::Frame> frames;
  ARROW_RETURN_NOT_OK(prepareFrames(*batch, options, memory_, ownership_, source, maximum_frame_bytes_, frames));
  return frames;
}

}  // namespace pangrova::result
