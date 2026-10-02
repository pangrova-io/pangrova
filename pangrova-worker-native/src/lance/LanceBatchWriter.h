// LanceBatchWriter: C++ writer for Lance file format via Lance C FFI.
//
// Accepts Velox RowVector batches, exports them to Arrow C Data Interface
// structs, and writes to Lance format via the Lance C FFI static library.
// Produces task-attempt-private uncommitted Lance fragments. finish() never
// publishes a dataset version; the connector owns the one atomic aggregate
// commit and recovery decision.
//
// Thread model: single-threaded. One writer per isolated dataset URI.
// The caller must not invoke methods concurrently.
//
// Lifecycle: open → writeBatch (0..N) → finish(metadata) → explicit close.
// Failed cleanup retains the writer for task-owned retry and observation.
//
// Placement: lance/ — Lance file format integration layer.
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::memory {
class MemoryPool;
}  // namespace facebook::velox::memory

namespace pangrova::lance {

// Statistics collected during write for manifest metadata reporting.
struct LanceWriteStats {
  int64_t rowsWritten = 0;
  int64_t bytesWritten = 0;
  int32_t batchesWritten = 0;
};

enum class LanceWriteOperation : int32_t {
  kCreate = 1,
  kAppend = 2,
  kOverwrite = 3,
};

// Configuration for LanceBatchWriter construction.
struct LanceWriterConfig {
  // Output URI for the Lance dataset (local path or object store URI).
  std::string outputPath;

  LanceWriteOperation operation = LanceWriteOperation::kCreate;

  // Exact Lance snapshot used to create the transaction. CREATE requires 0;
  // APPEND and OVERWRITE require a non-zero version.
  uint64_t expectedReadVersion = 0;

  // Connector-owned schema fence and idempotency identity. The worker echoes
  // these into typed commit metadata; the connector validates them again.
  std::array<uint8_t, 32> schemaFenceSha256{};
  std::string writeId;
  std::string tenantId;
  std::string generation;
  std::string transactionStagingPrefix;
  // Deterministically serialized canonical v2 TaskAttemptId.
  std::vector<uint8_t> taskAttempt;
  std::string storageBinding;
};

// Writes Velox RowVector batches to Lance file format via Lance C FFI.
//
// @NotThreadSafe — must be used on a single thread.
class LanceBatchWriter {
 public:
  // Constructs a writer for the given output schema and configuration.
  //
  // Opens the Lance C FFI writer handle immediately. Throws std::runtime_error
  // if the writer cannot be opened (invalid path, permission denied, etc.).
  //
  // @param pool       Memory pool for Arrow export operations.
  // @param outputType Schema of the data to be written.
  // @param config     Writer configuration (output path, append mode).
  LanceBatchWriter(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      const facebook::velox::RowTypePtr& outputType,
      const LanceWriterConfig& config);

  // Closes the writer handle. If finish() was not called, buffered data
  // is discarded (no implicit flush on destruction).
  ~LanceBatchWriter();

  // Non-copyable, non-movable (owns opaque FFI handle).
  LanceBatchWriter(const LanceBatchWriter&) = delete;
  LanceBatchWriter& operator=(const LanceBatchWriter&) = delete;
  LanceBatchWriter(LanceBatchWriter&&) = delete;
  LanceBatchWriter& operator=(LanceBatchWriter&&) = delete;

  // Writes a batch of rows to the Lance dataset.
  //
  // Exports the RowVector to Arrow C Data Interface format and passes
  // ownership of the ArrowArray to the Lance C FFI writer.
  //
  // @param batch  Non-null RowVector with schema matching outputType.
  // @throws std::runtime_error on write failure.
  void writeBatch(const facebook::velox::RowVectorPtr& batch);

  // Flushes buffered data files and returns typed uncommitted metadata.
  //
  // This never publishes a live manifest. Connector-owned commit must collect
  // the returned fragment and call the aggregate commit API exactly once.
  //
  // @throws std::runtime_error on flush/finalize failure.
  std::vector<uint8_t> finish();

  // Stops new input and explicitly joins SDK work before releasing the handle.
  // Errors retain the full owner and must be reflected in the task outcome.
  void close();
  // Releases verified local owners after an already-observed operation error.
  // Does not delete or authorize deletion of Dataset objects or sidecars.
  void releaseDrained();
  bool isDrainedAndCleaned() const { return encoders_joined_ && private_stage_cleaned_; }

  // Returns cumulative write statistics.
  const LanceWriteStats& stats() const { return stats_; }

  // Returns the output file path.
  const std::string& outputPath() const { return config_.outputPath; }

 private:
  // Exports the output type to an ArrowSchema for writer initialization.
  void exportSchemaToArrow();

  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  facebook::velox::RowTypePtr output_type_;
  LanceWriterConfig config_;
  LanceWriteStats stats_;

  // Opaque Lance C FFI writer handle. Non-null between construction and
  // destruction. Set to nullptr after close.
  void* writer_handle_;

  // Whether finish() has been called successfully.
  bool finished_ = false;
  bool encoders_joined_ = false;
  bool private_stage_cleaned_ = false;
};

}  // namespace pangrova::lance
