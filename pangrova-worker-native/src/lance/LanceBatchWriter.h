// LanceBatchWriter: C++ writer for Lance file format via Lance C FFI.
//
// Accepts Velox RowVector batches, exports them to Arrow C Data Interface
// structs, and writes to Lance format via the Lance C FFI static library.
// Produces .lance files compatible with the table schema and partition spec.
//
// Used by two paths:
//   - C++ hot path (INSERT SELECT, UPDATE, DELETE WHERE): called directly
//     by Velox write operators.
//   - Java cold path (INSERT VALUES): called via NativeBridge JNI from Java.
//
// Thread model: single-threaded. One writer per partition output file.
// The caller must not invoke methods concurrently.
//
// Lifecycle: open → writeBatch (0..N) → finish → destructor.
// If finish() is not called before destruction, buffered data is discarded.
//
// Compile guard: only available when PANGROVA_LANCE_AVAILABLE=1 (Lance C FFI
// linked into libpangrova_native). Callers must check the macro before use.
//
// Placement: lance/ — Lance file format integration layer.
#pragma once

#if PANGROVA_LANCE_AVAILABLE

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

// Configuration for LanceBatchWriter construction.
struct LanceWriterConfig {
  // Output path for the .lance file (local path or object store URI).
  std::string outputPath;

  // Whether to append to an existing dataset (true) or create new (false).
  bool append = false;
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
      facebook::velox::memory::MemoryPool* pool,
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

  // Flushes all buffered data and finalizes the .lance file on storage.
  //
  // Must be called exactly once before destruction to produce a valid
  // .lance file. Calling writeBatch() after finish() is undefined behavior.
  //
  // @throws std::runtime_error on flush/finalize failure.
  void finish();

  // Returns cumulative write statistics.
  const LanceWriteStats& stats() const { return stats_; }

  // Returns the output file path.
  const std::string& outputPath() const { return config_.outputPath; }

 private:
  // Exports the output type to an ArrowSchema for writer initialization.
  void exportSchemaToArrow();

  facebook::velox::memory::MemoryPool* pool_;
  facebook::velox::RowTypePtr output_type_;
  LanceWriterConfig config_;
  LanceWriteStats stats_;

  // Opaque Lance C FFI writer handle. Non-null between construction and
  // destruction. Set to nullptr after close.
  void* writer_handle_;

  // Whether finish() has been called successfully.
  bool finished_ = false;
};

}  // namespace pangrova::lance

#endif  // PANGROVA_LANCE_AVAILABLE
