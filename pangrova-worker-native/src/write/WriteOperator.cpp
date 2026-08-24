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

#include <stdexcept>

#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include "velox/vector/LazyVector.h"
#include "velox/vector/arrow/Bridge.h"

#include "observability/PangrovaLog.h"

namespace pangrova::write {

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
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
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
  facebook::velox::exportToArrow(input, arrowArray, operatorCtx_->pool(), arrowOpts);
  facebook::velox::exportToArrow(input, arrowSchema, arrowOpts);

  auto importResult = arrow::ImportRecordBatch(&arrowArray, &arrowSchema);
  if (!importResult.ok()) {
    throw std::runtime_error(
        "WriteOperator::addInput: Arrow import failed: "
        + importResult.status().ToString());
  }
  auto recordBatch = *importResult;

  // F-NATIVE-WRITE-08: Use SerializeRecordBatch directly instead of
  // creating a full IPC stream writer per batch. Avoids stream-header
  // + EOS framing overhead and the intermediate BufferOutputStream copy.
  auto buffer_result = arrow::ipc::SerializeRecordBatch(
      *recordBatch, arrow::ipc::IpcWriteOptions::Defaults());
  if (!buffer_result.ok()) {
    throw std::runtime_error(
        "WriteOperator::addInput: serialization failed: "
        + buffer_result.status().ToString());
  }
  auto buffer = *buffer_result;

  int32_t rowCount = static_cast<int32_t>(recordBatch->num_rows());
  int32_t colCount = static_cast<int32_t>(recordBatch->num_columns());
  writeExecutor_->appendBatchData(
      writeHandle_, buffer->data(), buffer->size(), rowCount, colCount);

  totalRows_ += input->size();
  totalBytes_ += input->estimateFlatSize();
}



facebook::velox::RowVectorPtr WriteOperator::getOutput() {
  // Sink operator — no output rows produced. All data flows to the Java
  // connector via appendBatch(). Returning nullptr signals to the Velox
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
