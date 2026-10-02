// WriteOperator: Velox operator that writes input batches to a Java connector
// via the NativeWriteExecutor JNI wrapper.
//
// Integrates with the Velox execution framework as a sink operator:
//   - addInput: serializes each RowVector to Arrow IPC format and calls
//     bridge_->appendBatchData() to push data to Java ConnectorPageSink.
//   - noMoreInput: calls bridge_->commitWrite() to finalize the transaction.
//
// Lifecycle guarantee: every beginWrite is paired with exactly one commitWrite
// or abortWrite via RAII (WriteGuard). If addInput or noMoreInput throws,
// the destructor ensures abortWrite is called.
//
// Thread model: single Velox driver thread owns this operator. No concurrent
// access. The JNI calls execute on the same thread (pull mode).
//
// Placement: src/ — top-level write operator component.
#pragma once

#include "write/NativeWriteExecutor.h"

#include "velox/exec/Operator.h"

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include <cstdint>
#include <memory>

namespace pangrova::write {

// Configuration for WriteOperator, extracted from the Substrait WriteRel.
struct WriteOperatorConfig {
  WriteDescriptor descriptor;
};

// Velox sink operator that writes batches to Java connectors via JNI.
//
// Execution flow:
//   1. Constructor calls bridge_->beginWrite() to open a write session.
//   2. Each addInput() serializes the RowVector to Arrow IPC format
//      and calls bridge_->appendBatchData().
//   3. noMoreInput() calls bridge_->commitWrite() to finalize.
//   4. If an exception occurs at any point, the destructor calls
//      bridge_->abortWrite() to roll back partial writes.
class WriteOperator : public facebook::velox::exec::Operator {
 public:
  WriteOperator(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
      NativeWriteExecutor* writeExecutor,
      const WriteOperatorConfig& config);

  ~WriteOperator() override;

  // Non-copyable, non-movable.
  WriteOperator(const WriteOperator&) = delete;
  WriteOperator& operator=(const WriteOperator&) = delete;

  // Returns true when the operator can accept input (always true until
  // noMoreInput is called).
  bool needsInput() const override;

  // Serializes the input RowVector to Arrow IPC format and calls
  // bridge_->appendBatchData(). Increments rows/bytes counters.
  void addInput(facebook::velox::RowVectorPtr input) override;

  /// Serializes one batch as an uncompressed IPC stream: schema, batch, EOS.
  /// Preserves source column order and dimensions, including zero-row batches.
  /// @param batch Valid batch with dictionaries already flattened during export.
  /// @return Owning buffer, or an error for invalid data, dictionary fields
  /// (including nested fields), dimensions outside JNI int32 range, or IPC I/O.
  /// The caller must retain the buffer until synchronous appendBatchData returns.
  static arrow::Result<std::shared_ptr<arrow::Buffer>> serializeBatch(
      const arrow::RecordBatch& batch);

  // Returns nullptr — this is a sink operator with no output.
  facebook::velox::RowVectorPtr getOutput() override;

  // Signals end of input. Commits the write transaction via the bridge.
  void noMoreInput() override;

  // Returns true after noMoreInput() has been called and commit succeeded.
  bool isFinished() override;

  // Blocks until the operator is ready (always ready — JNI calls are sync).
  facebook::velox::exec::BlockingReason isBlocked(
      facebook::velox::ContinueFuture* future) override;

 private:
  NativeWriteExecutor* writeExecutor_;  // Not owned — lifetime managed by task executor.
  int64_t writeHandle_;
  bool committed_;
  int64_t totalRows_;
  int64_t totalBytes_;
};

}  // namespace pangrova::write
