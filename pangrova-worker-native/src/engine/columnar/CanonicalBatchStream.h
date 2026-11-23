#pragma once

#include <arrow/buffer.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/util/type_fwd.h>
#include "engine/memory/ColumnarMemoryPool.h"

namespace pangrova::columnar {
// The wire ceiling is not a substitute for the actual task allocator cap.
inline constexpr int64_t kMaximumBatchStreamBytes = 256LL * 1024 * 1024;
struct BatchStreamOptions {
  int64_t maximum_bytes = kMaximumBatchStreamBytes;
  arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
  double minimum_space_savings = 0.2;
};
// The Arrow payloads hold their backing buffers until the entire prepared page is written.
class PreparedBatchStream {
 public:
  static arrow::Result<std::unique_ptr<PreparedBatchStream>> prepare(
      const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
      const std::shared_ptr<memory::ColumnarMemoryPool>& pool, const BatchStreamOptions& options);

  int64_t size() const { return size_; }
  arrow::Status writeTo(arrow::io::OutputStream& sink) const;

 private:
  PreparedBatchStream(std::shared_ptr<memory::ColumnarMemoryPool> pool,
      arrow::ipc::IpcWriteOptions options, std::vector<arrow::ipc::IpcPayload> payloads, int64_t size);

  // Arrow buffers borrow their allocator. Declare it before all governed payloads.
  const std::shared_ptr<memory::ColumnarMemoryPool> pool_;
  const arrow::ipc::IpcWriteOptions options_;
  const std::vector<arrow::ipc::IpcPayload> payloads_;
  const int64_t size_;
};

arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    const BatchStreamOptions& options = {});
// Checks Arrow buffers once per decoded batch. An admitted schema also checks
// wire identity and required values before any batch is returned.
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool,
    int64_t maximumBytes = kMaximumBatchStreamBytes,
    const arrow::Schema* admittedSchema = nullptr);
}  // namespace pangrova::columnar
