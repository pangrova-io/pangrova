#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/columnar/CanonicalArrowValidation.h"
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>
#include <cmath>

namespace pangrova::columnar {
namespace {
// Rejects each write before it grows the governed output allocation.
class BoundedBatchOutput final : public arrow::io::OutputStream {
 public:
  BoundedBatchOutput(std::shared_ptr<arrow::io::BufferOutputStream> sink, int64_t maximum)
      : sink_(std::move(sink)), maximum_(maximum) {}
  arrow::Status Close() override { return sink_->Close(); }
  bool closed() const override { return sink_->closed(); }
  arrow::Result<int64_t> Tell() const override { return sink_->Tell(); }
  arrow::Status Write(const void* data, int64_t bytes) override {
    ARROW_ASSIGN_OR_RAISE(auto position, sink_->Tell());
    if (bytes < 0 || bytes > maximum_ - position) {
      return arrow::Status::CapacityError("Canonical batch stream exceeds wire limit");
    }
    return sink_->Write(data, bytes);
  }
 private:
  std::shared_ptr<arrow::io::BufferOutputStream> sink_;
  const int64_t maximum_;
};

arrow::Result<arrow::ipc::IpcWriteOptions> writeOptions(
    const BatchStreamOptions& config, arrow::MemoryPool* pool) {
  if (!std::isfinite(config.minimum_space_savings) || config.minimum_space_savings < 0 ||
      config.minimum_space_savings > 1) {
    return arrow::Status::Invalid("Canonical batch compression savings must be within [0, 1]");
  }
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.memory_pool = pool;
  // Work stays on the owning task driver, not Arrow's process-global CPU pool.
  options.use_threads = false;
  if (config.compression != arrow::Compression::UNCOMPRESSED) {
    if (config.compression != arrow::Compression::LZ4_FRAME && config.compression != arrow::Compression::ZSTD) {
      return arrow::Status::Invalid("Canonical batch compression requires none, LZ4_FRAME or ZSTD");
    }
    ARROW_ASSIGN_OR_RAISE(auto codec, arrow::util::Codec::Create(config.compression));
    options.codec = std::shared_ptr<arrow::util::Codec>(std::move(codec));
    options.min_space_savings = config.minimum_space_savings;
  }
  return options;
}
}  // namespace

arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, const BatchStreamOptions& config) {
  const auto maximumBytes = config.maximum_bytes;
  if (!pool || batches.empty() || maximumBytes <= 0 || maximumBytes > kMaximumBatchStreamBytes) {
    return arrow::Status::Invalid("Canonical batch stream requires allocator, batches and bounded wire size");
  }
  if (!batches.front()) return arrow::Status::Invalid("Canonical batch stream requires non-null batches");
  for (const auto& batch : batches) {
    if (!batch || !batch->schema()->Equals(*batches.front()->schema(), true)) {
      return arrow::Status::TypeError("Canonical batch stream changed its declared schema");
    }
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
  }
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create(0, pool.get()));
  auto bounded = std::make_shared<BoundedBatchOutput>(sink, maximumBytes);
  ARROW_ASSIGN_OR_RAISE(auto options, writeOptions(config, pool.get()));
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(bounded, batches.front()->schema(), options));
  for (const auto& batch : batches) {
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());
  if (buffer->size() > maximumBytes) return arrow::Status::CapacityError("Canonical batch stream exceeds wire limit");
  return memory::retainColumnarBuffer(std::move(buffer), pool);
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, int64_t maximumBytes,
    const arrow::Schema* admittedSchema) {
  if (!buffer || !pool || maximumBytes <= 0 || maximumBytes > kMaximumBatchStreamBytes ||
      buffer->size() < 8 || buffer->size() > maximumBytes) {
    return arrow::Status::Invalid("Canonical batch stream has invalid ownership or wire size");
  }
  const auto* end = buffer->data() + buffer->size() - 8;
  if (end[0] != 0xff || end[1] != 0xff || end[2] != 0xff || end[3] != 0xff ||
      end[4] != 0 || end[5] != 0 || end[6] != 0 || end[7] != 0) {
    return arrow::Status::SerializationError("Canonical batch stream is missing its committed Arrow EOF marker");
  }
  auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = pool.get();
  options.use_threads = false;
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(input, options));
  std::vector<std::shared_ptr<arrow::RecordBatch>> result;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    if (!batch) break;
    if (batch->schema() != reader->schema() && !batch->schema()->Equals(*reader->schema(), true)) {
      return arrow::Status::TypeError("Canonical batch stream changed schema during decoding");
    }
    if (admittedSchema) {
      ARROW_RETURN_NOT_OK(validateCanonicalArrowBatch(*batch, *admittedSchema));
    } else {
      ARROW_RETURN_NOT_OK(batch->ValidateFull());
    }
    result.push_back(memory::retainColumnarBatch(batch, pool));
  }
  ARROW_ASSIGN_OR_RAISE(auto consumed, input->Tell());
  ARROW_ASSIGN_OR_RAISE(auto size, input->GetSize());
  if (consumed != size || result.empty()) {
    return arrow::Status::SerializationError("Canonical batch stream has trailing bytes or no record batches");
  }
  return result;
}
}  // namespace pangrova::columnar
