#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/columnar/CanonicalArrowValidation.h"
#include <arrow/io/memory.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>
#include <algorithm>
#include <array>
#include <cmath>

namespace pangrova::columnar {
namespace {
// The continuation token followed by a zero-length message terminates each IPC stream.
constexpr std::array<uint8_t, 8> kStreamEnd{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0};

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

PreparedBatchStream::PreparedBatchStream(std::shared_ptr<memory::ColumnarMemoryPool> pool,
    arrow::ipc::IpcWriteOptions options, std::vector<arrow::ipc::IpcPayload> payloads, int64_t size)
    : pool_(std::move(pool)), options_(std::move(options)), payloads_(std::move(payloads)), size_(size) {}

arrow::Result<std::unique_ptr<PreparedBatchStream>> PreparedBatchStream::prepare(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, const BatchStreamOptions& config) {
  const auto maximumBytes = config.maximum_bytes;
  if (!pool || batches.empty() || !batches.front() || maximumBytes <= 0 ||
      maximumBytes > kMaximumBatchStreamBytes) {
    return arrow::Status::Invalid("Canonical batch stream requires allocator, batches and bounded wire size");
  }
  const auto& schema = *batches.front()->schema();
  const arrow::ipc::DictionaryFieldMapper dictionaries(schema);
  if (dictionaries.num_fields() != 0) {
    return arrow::Status::Invalid("Canonical batch stream requires flattened dictionaries");
  }
  ARROW_ASSIGN_OR_RAISE(auto options, writeOptions(config, pool.get()));

  std::vector<arrow::ipc::IpcPayload> payloads;
  int64_t size = static_cast<int64_t>(kStreamEnd.size());
  auto append = [&](arrow::ipc::IpcPayload payload) -> arrow::Status {
    const auto bytes = arrow::ipc::GetPayloadSize(payload, options);
    if (bytes <= 0 || bytes > maximumBytes - size) {
      return arrow::Status::CapacityError("Canonical batch stream exceeds wire limit");
    }
    size += bytes;
    payloads.push_back(std::move(payload));
    return arrow::Status::OK();
  };
  arrow::ipc::IpcPayload schemaPayload;
  ARROW_RETURN_NOT_OK(arrow::ipc::GetSchemaPayload(schema, options, dictionaries, &schemaPayload));
  ARROW_RETURN_NOT_OK(append(std::move(schemaPayload)));
  for (const auto& batch : batches) {
    if (!batch || (batch->schema().get() != &schema && !batch->schema()->Equals(schema, true))) {
      return arrow::Status::TypeError("Canonical batch stream changed its declared schema");
    }
    ARROW_RETURN_NOT_OK(batch->ValidateFull());
    arrow::ipc::IpcPayload payload;
    ARROW_RETURN_NOT_OK(arrow::ipc::GetRecordBatchPayload(*batch, options, &payload));
    ARROW_RETURN_NOT_OK(append(std::move(payload)));
  }
  return std::unique_ptr<PreparedBatchStream>(
      new PreparedBatchStream(pool, std::move(options), std::move(payloads), size));
}

arrow::Status PreparedBatchStream::writeTo(arrow::io::OutputStream& sink) const {
  for (const auto& payload : payloads_) {
    int32_t metadataBytes = 0;
    ARROW_RETURN_NOT_OK(arrow::ipc::WriteIpcPayload(payload, options_, &sink, &metadataBytes));
  }
  return sink.Write(kStreamEnd.data(), kStreamEnd.size());
}

arrow::Result<std::shared_ptr<arrow::Buffer>> encodeBatchStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, const BatchStreamOptions& config) {
  ARROW_ASSIGN_OR_RAISE(auto prepared, PreparedBatchStream::prepare(batches, pool, config));
  ARROW_ASSIGN_OR_RAISE(auto storage, arrow::AllocateBuffer(prepared->size(), pool.get()));
  auto buffer = std::shared_ptr<arrow::Buffer>(std::move(storage));
  arrow::io::FixedSizeBufferWriter sink(buffer);
  ARROW_RETURN_NOT_OK(prepared->writeTo(sink));
  ARROW_RETURN_NOT_OK(sink.Close());
  return memory::retainColumnarBuffer(std::move(buffer), pool);
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> decodeBatchStream(
    std::shared_ptr<arrow::Buffer> buffer,
    const std::shared_ptr<memory::ColumnarMemoryPool>& pool, int64_t maximumBytes,
    const arrow::Schema* admittedSchema) {
  const auto endBytes = static_cast<int64_t>(kStreamEnd.size());
  if (!buffer || !pool || maximumBytes <= 0 || maximumBytes > kMaximumBatchStreamBytes ||
      buffer->size() < endBytes || buffer->size() > maximumBytes) {
    return arrow::Status::Invalid("Canonical batch stream has invalid ownership or wire size");
  }
  const auto* end = buffer->data() + buffer->size() - endBytes;
  if (!std::equal(kStreamEnd.begin(), kStreamEnd.end(), end)) {
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
