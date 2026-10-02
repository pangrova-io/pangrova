// WriteOperator: Velox sink operator implementation that writes batches to
// Java connectors via NativeWriteExecutor JNI wrapper.
//
// Key design decisions:
//   - beginWrite is called in the constructor so the write session is open
//     before any data arrives. This matches the Velox operator lifecycle
//     where the operator is fully initialized before addInput is called.
//   - The destructor calls abortWrite if commitWrite was never called,
//     ensuring the try-finally lifecycle guarantee from the design doc.
//   - Arrow IPC serialization is used for data transfer across the JNI
//     boundary, consistent with the read path. Dictionary and constant
//     encodings are flattened before serialization to ensure Java sees
//     plain vectors.

#include "write/WriteOperator.h"
#include "write/WriteArrowExport.h"

#include "observability/PangrovaLog.h"

#include "velox/vector/LazyVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <folly/ScopeGuard.h>

#include <limits>
#include <stdexcept>

namespace pangrova::write {
namespace {

arrow::Status validatePlainType(const arrow::DataType& type) {
  if (type.id() == arrow::Type::DICTIONARY) {
    return arrow::Status::Invalid(
        "WriteOperator: dictionaries must be flattened before serialization");
  }
  for (const auto& field : type.fields()) {
    ARROW_RETURN_NOT_OK(validatePlainType(*field->type()));
  }
  return arrow::Status::OK();
}

arrow::Status validateBatchShape(const arrow::RecordBatch& batch) {
  const auto& schema = batch.schema();
  if (!schema) {
    return arrow::Status::Invalid("WriteOperator: batch schema is missing");
  }
  if (batch.num_columns() != schema->num_fields()) {
    return arrow::Status::Invalid(
        "WriteOperator: column count differs from schema field count");
  }
  for (int index = 0; index < batch.num_columns(); ++index) {
    const auto& column = batch.column(index);
    const auto& field = schema->field(index);
    if (!column || !field) {
      return arrow::Status::Invalid(
          "WriteOperator: batch contains a null column or field");
    }
    if (column->length() != batch.num_rows()) {
      return arrow::Status::Invalid(
          "WriteOperator: column length differs from batch row count");
    }
    if (!column->type()->Equals(*field->type())) {
      return arrow::Status::Invalid(
          "WriteOperator: column type differs from schema field type");
    }
  }
  return arrow::Status::OK();
}

}  // namespace

WriteOperator::WriteOperator(
    int32_t operatorId,
    facebook::velox::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
    NativeWriteExecutor* writeExecutor,
    const WriteOperatorConfig& config)
    : Operator(
          driverCtx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "WriteOperator"),
      writeExecutor_(writeExecutor),
      writeHandle_(-1),
      committed_(false),
      totalRows_(0),
      totalBytes_(0) {
  if (!writeExecutor_) {
    throw std::runtime_error("WriteOperator: null NativeWriteExecutor");
  }
  writeHandle_ = writeExecutor_->beginWrite(config.descriptor);
  PANGROVA_LOG_INFO << "WriteOperator: session opened"
      << " handle=" << writeHandle_
      << " table=" << config.descriptor.catalogName
      << "." << config.descriptor.schemaName
      << "." << config.descriptor.tableName;
}

WriteOperator::~WriteOperator() {
  if (!committed_ && writeHandle_ >= 0) {
    writeExecutor_->abortWrite(writeHandle_);
    PANGROVA_LOG_WARN << "WriteOperator: aborted uncommitted session"
        << " handle=" << writeHandle_
        << " rows=" << totalRows_;
  }
}

bool WriteOperator::needsInput() const {
  return !noMoreInput_;
}

void WriteOperator::addInput(facebook::velox::RowVectorPtr input) {
  if (!input || input->size() == 0) {
    return;
  }

  // Serialize RowVector to Arrow IPC bytes in C++, consistent with the read
  // path. Flatten dictionary and constant encodings so Java sees plain vectors.
  ArrowArray arrowArray{};
  ArrowSchema arrowSchema{};
  // Import consumes the exports and clears their release callbacks. Until then,
  // this guard releases any completed export even if the next export throws.
  auto releaseExports = folly::makeGuard([&]() {
    if (arrowArray.release) {
      arrowArray.release(&arrowArray);
    }
    if (arrowSchema.release) {
      arrowSchema.release(&arrowSchema);
    }
  });
  ArrowOptions arrowOpts;
  arrowOpts.flattenDictionary = true;
  arrowOpts.flattenConstant = true;
  // Load any LazyVector columns before Arrow export. Bridge.cpp's schema
  // export does not handle LAZY encoding — an unloaded lazy child (e.g. an
  // identity-projected complex column) would be asUnchecked-cast and read
  // as garbage, crashing or over-allocating. Same contract as the
  // drainOutputToWriter path in PlanExecutionDriver.
  facebook::velox::LazyVector::ensureLoadedRows(
      input, facebook::velox::SelectivityVector(input->size()));
  auto exportVector = prepareWriteArrowExport(input);
  facebook::velox::exportToArrow(exportVector, arrowArray, operatorCtx_->pool(), arrowOpts);
  facebook::velox::exportToArrow(exportVector, arrowSchema, arrowOpts);

  auto importResult = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  if (!importResult.ok()) {
    throw std::runtime_error(
        "WriteOperator::addInput: Arrow import failed: "
        + importResult.status().ToString());
  }
  auto recordBatch = *importResult;

  auto buffer_result = serializeBatch(*recordBatch);
  if (!buffer_result.ok()) {
    throw std::runtime_error(
        "WriteOperator::addInput: serialization failed: "
        + buffer_result.status().ToString());
  }
  const auto buffer = *buffer_result;

  const int32_t rowCount = static_cast<int32_t>(recordBatch->num_rows());
  const int32_t colCount = static_cast<int32_t>(recordBatch->num_columns());
  // Java borrows these bytes only during this synchronous call. Keep the owning
  // buffer local and alive until appendBatchData returns; never queue its pointer.
  writeExecutor_->appendBatchData(
      writeHandle_, buffer->data(), buffer->size(), rowCount, colCount);

  totalRows_ += input->size();
  totalBytes_ += input->estimateFlatSize();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteOperator::serializeBatch(
    const arrow::RecordBatch& batch) {
  if (batch.num_rows() < 0 ||
      batch.num_rows() > std::numeric_limits<int32_t>::max()) {
    return arrow::Status::Invalid("WriteOperator: row count outside JNI int32 range");
  }
  ARROW_RETURN_NOT_OK(validateBatchShape(batch));
  ARROW_RETURN_NOT_OK(batch.ValidateFull());
  for (const auto& field : batch.schema()->fields()) {
    ARROW_RETURN_NOT_OK(validatePlainType(*field->type()));
  }

  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.codec = nullptr;
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(
      auto writer, arrow::ipc::MakeStreamWriter(sink, batch.schema(), options));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

facebook::velox::RowVectorPtr WriteOperator::getOutput() {
  // Sink operator — no output rows produced. All data flows to the Java
  // connector via appendBatchData(). Returning nullptr signals to the Velox
  // driver that this operator has no output to propagate downstream.
  return nullptr;
}

void WriteOperator::noMoreInput() {
  Operator::noMoreInput();

  WriteCommitResult result = writeExecutor_->commitWrite(writeHandle_);
  committed_ = true;

  PANGROVA_LOG_INFO << "WriteOperator: committed"
      << " handle=" << writeHandle_
      << " rowsWritten=" << result.rowsWritten
      << " bytesWritten=" << result.bytesWritten
      << " filesCreated=" << result.filePathsCreated.size();
}

bool WriteOperator::isFinished() {
  return noMoreInput_ && committed_;
}

facebook::velox::exec::BlockingReason WriteOperator::isBlocked(
    facebook::velox::ContinueFuture* future) {
  // JNI calls are synchronous — the operator is never blocked waiting
  // for an async event. Return kNotBlocked so the Velox driver proceeds
  // immediately.
  return facebook::velox::exec::BlockingReason::kNotBlocked;
}

}  // namespace pangrova::write
