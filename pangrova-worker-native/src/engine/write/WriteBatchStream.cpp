#include "engine/write/WriteBatchStream.h"

#include <arrow/array.h>
#include <arrow/type.h>
#include <limits>

namespace pangrova::write {
namespace {

arrow::Status validateBatchShape(const arrow::RecordBatch& batch) {
  const auto& schema = batch.schema();
  if (!schema) return arrow::Status::Invalid("Write batch schema is missing");
  if (batch.num_columns() != schema->num_fields()) {
    return arrow::Status::Invalid("Write batch column count differs from schema field count");
  }
  for (int index = 0; index < batch.num_columns(); ++index) {
    const auto& column = batch.column(index);
    const auto& field = schema->field(index);
    if (!column || !field) return arrow::Status::Invalid("Write batch contains a null column or field");
    if (column->length() != batch.num_rows()) {
      return arrow::Status::Invalid("Write batch column length differs from batch row count");
    }
    if (!column->type()->Equals(*field->type())) {
      return arrow::Status::Invalid("Write batch column type differs from schema field type");
    }
  }
  return arrow::Status::OK();
}

}  // namespace

arrow::Result<std::shared_ptr<arrow::Buffer>> encodeWriteBatch(
    const arrow::RecordBatch& batch,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool) {
  if (batch.num_rows() < 0 || batch.num_rows() > std::numeric_limits<int32_t>::max()) {
    return arrow::Status::Invalid("Write batch row count outside JNI int32 range");
  }
  ARROW_RETURN_NOT_OK(validateBatchShape(batch));
  // Rebuild only RecordBatch metadata; column buffers retain their actual owners.
  auto record = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
  return columnar::encodeBatchStream({std::move(record)}, pool);
}

}  // namespace pangrova::write
