#include "columnar/CanonicalBatchStream.h"
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace pangrova::columnar {
arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, int64_t maximumBytes) {
  if (!pool || batches.empty() || maximumBytes <= 0 || maximumBytes > kMaximumBatchStreamBytes) {
    return arrow::Status::Invalid("Canonical batch stream requires allocator, batches and bounded wire size");
  }
  for (const auto& batch : batches) {
    if (!batch || !batch->schema()->Equals(*batches.front()->schema(), true)) {
      return arrow::Status::TypeError("Canonical batch stream changed its declared schema");
    }
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
  }
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create(0, pool.get()));
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.memory_pool = pool.get();
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, batches.front()->schema(), options));
  for (const auto& batch : batches) {
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_ASSIGN_OR_RAISE(auto size, sink->Tell());
    if (size > maximumBytes) return arrow::Status::CapacityError("Canonical batch stream exceeds wire limit");
  }
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());
  if (buffer->size() > maximumBytes) return arrow::Status::CapacityError("Canonical batch stream exceeds wire limit");
  return memory::retainColumnarBuffer(std::move(buffer), pool);
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, int64_t maximumBytes) {
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
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(input, options));
  std::vector<std::shared_ptr<arrow::RecordBatch>> result;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_RETURN_NOT_OK(reader->ReadNext(&batch));
    if (!batch) break;
    if (!batch->schema()->Equals(*reader->schema(), true)) {
      return arrow::Status::TypeError("Canonical batch stream changed schema during decoding");
    }
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
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
