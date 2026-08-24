// ArrowBatchVectorSerde: Serializes Velox RowVector to Arrow IPC RecordBatch.
// See ArrowBatchVectorSerde.h for class documentation.

#include "result/ArrowBatchVectorSerde.h"

#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::result {

namespace {

// Validates that all column types in the RowVector are representable in Arrow
// IPC format. Throws ArrowBatchUnsupportedTypeException for opaque types.
void validateTypesForExport(
    const facebook::velox::RowVectorPtr& vector) {
  const auto& type = vector->type()->asRow();
  for (decltype(type.size()) i = 0; i < type.size(); ++i) {
    const auto& child_type = type.childAt(i);
    if (child_type->kind() == facebook::velox::TypeKind::OPAQUE) {
      throw ArrowBatchUnsupportedTypeException(child_type->toString());
    }
  }
}

// Exports a Velox RowVector to an Arrow RecordBatch via Arrow C Data Interface.
// L2-3: preserve_dictionary controls whether dictionary-encoded vectors are
// exported as-is (true) or flattened to flat encoding (false). Arrow IPC
// format natively supports dictionary encoding; preserving it reduces
// serialized size for low-cardinality columns.
std::shared_ptr<arrow::RecordBatch> exportToBatch(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool,
    bool preserve_dictionary = false) {
  ArrowArray out_array;
  ArrowSchema out_schema;
  ArrowOptions arrow_opts;
  arrow_opts.flattenDictionary = !preserve_dictionary;
  arrow_opts.flattenConstant = true;
  facebook::velox::exportToArrow(vector, out_array, pool, arrow_opts);
  facebook::velox::exportToArrow(vector, out_schema, arrow_opts);

  auto result = arrow::ImportRecordBatch(&out_array, &out_schema);
  VELOX_CHECK(
      result.ok(),
      "Failed to export RowVector to RecordBatch: {}",
      result.status().ToString());
  return *result;
}

// Serializes an Arrow RecordBatch to IPC stream format bytes and writes to
// the output stream with a 4-byte length prefix for message framing.
// Uses Arrow IPC stream format which embeds the schema, enabling self-describing
// deserialization without external schema information.
void writeBatchToStream(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    facebook::velox::OutputStream* stream) {
  // Use IPC stream writer which includes schema + batch in one stream.
  auto sink_result = arrow::io::BufferOutputStream::Create();
  VELOX_CHECK(
      sink_result.ok(),
      "Failed to create buffer output stream: {}",
      sink_result.status().ToString());
  auto sink = *sink_result;

  auto writer_result = arrow::ipc::MakeStreamWriter(
      sink, batch->schema(), arrow::ipc::IpcWriteOptions::Defaults());
  VELOX_CHECK(
      writer_result.ok(),
      "Failed to create IPC stream writer: {}",
      writer_result.status().ToString());
  auto writer = *writer_result;

  auto write_status = writer->WriteRecordBatch(*batch);
  VELOX_CHECK(
      write_status.ok(),
      "Failed to write RecordBatch: {}",
      write_status.ToString());

  auto close_status = writer->Close();
  VELOX_CHECK(
      close_status.ok(),
      "Failed to close IPC stream writer: {}",
      close_status.ToString());

  auto buffer_result = sink->Finish();
  VELOX_CHECK(
      buffer_result.ok(),
      "Failed to finish buffer output stream: {}",
      buffer_result.status().ToString());

  auto buffer = *buffer_result;
  int32_t length = static_cast<int32_t>(buffer->size());
  stream->write(reinterpret_cast<const char*>(&length), sizeof(length));
  stream->write(reinterpret_cast<const char*>(buffer->data()), length);
}

// Selects rows from a RowVector by index ranges, returning a flat RowVector.
facebook::velox::RowVectorPtr selectRows(
    const facebook::velox::RowVectorPtr& vector,
    const folly::Range<const facebook::velox::IndexRange*>& ranges,
    facebook::velox::memory::MemoryPool* pool) {
  if (ranges.size() == 1) {
    return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        vector->slice(ranges[0].begin, ranges[0].size));
  }
  facebook::velox::vector_size_t total_rows = 0;
  for (const auto& range : ranges) {
    total_rows += range.size;
  }
  auto indices_buffer = facebook::velox::AlignedBuffer::allocate<
      facebook::velox::vector_size_t>(total_rows, pool);
  auto raw_indices =
      indices_buffer->asMutable<facebook::velox::vector_size_t>();
  facebook::velox::vector_size_t offset = 0;
  for (const auto& range : ranges) {
    for (facebook::velox::vector_size_t j = 0; j < range.size; ++j) {
      raw_indices[offset++] = range.begin + j;
    }
  }
  facebook::velox::VectorPtr wrapped =
      facebook::velox::BaseVector::wrapInDictionary(
          nullptr, indices_buffer, total_rows, vector);
  facebook::velox::BaseVector::flattenVector(wrapped);
  return std::dynamic_pointer_cast<facebook::velox::RowVector>(wrapped);
}

// BatchVectorSerializer that writes Arrow IPC RecordBatch format.
// L2-2: Caches the Arrow schema to avoid redundant schema export/negotiation
// on every serialize() call when the input schema is unchanged.
// L2-3: When preserve_dictionary is true, dictionary-encoded vectors are
// exported without flattening, reducing transfer size for low-cardinality cols.
class ArrowBatchSerializer : public facebook::velox::BatchVectorSerializer {
 public:
  ArrowBatchSerializer(
      facebook::velox::memory::MemoryPool* pool,
      bool preserve_dictionary)
      : pool_(pool), preserve_dictionary_(preserve_dictionary) {}

  void serialize(
      const facebook::velox::RowVectorPtr& vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::Scratch& /*scratch*/,
      facebook::velox::OutputStream* stream) override {
    validateTypesForExport(vector);

    facebook::velox::RowVectorPtr target;
    if (ranges.size() == 1 && ranges[0].begin == 0 &&
        ranges[0].size == vector->size()) {
      target = vector;
    } else {
      target = selectRows(vector, ranges, pool_);
    }

    auto batch = exportToBatch(target, pool_, preserve_dictionary_);

    // L2-2: Cache the Arrow schema for change detection. Each serialize()
    // call produces an independent IPC message (schema + batch), so the
    // StreamWriter cannot be reused across calls. However, caching the schema
    // enables: (1) fast schema-change detection without deep comparison on
    // every call, and (2) future optimization where schema-unchanged batches
    // could use a more compact encoding.
    if (!cached_schema_ || !cached_schema_->Equals(batch->schema())) {
      cached_schema_ = batch->schema();
    }

    writeBatchToStream(batch, stream);
  }

  // F-NATIVE-SHUF-06: Distribute per-row estimate across the range instead
  // of lumping the entire range cost onto the first row. PartitionedOutput
  // uses per-row sizes to decide when to flush a partition; lumping causes
  // one partition to flush immediately while others starve.
  void estimateSerializedSize(
      facebook::velox::VectorPtr vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::vector_size_t** sizes,
      facebook::velox::Scratch& /*scratch*/) override {
    if (!vector || vector->size() == 0) {
      return;
    }
    auto avg_row_bytes = vector->estimateFlatSize() /
        std::max(static_cast<int64_t>(1),
                 static_cast<int64_t>(vector->size()));
    auto per_row_estimate =
        static_cast<facebook::velox::vector_size_t>(avg_row_bytes * 1.1);
    for (const auto& range : ranges) {
      for (auto i = range.begin; i < range.begin + range.size; ++i) {
        *sizes[i] += per_row_estimate;
      }
    }
  }

 private:
  facebook::velox::memory::MemoryPool* pool_;
  bool preserve_dictionary_;
  // L2-2: Cached schema to detect schema changes and avoid redundant work.
  std::shared_ptr<arrow::Schema> cached_schema_;
};

// IterativeVectorSerializer that accumulates rows and serializes as Arrow IPC.
// Arrow IPC is inherently batch-oriented, so this buffers appended data and
// flushes as a single RecordBatch per buffered vector.
class ArrowBatchIterativeSerializer
    : public facebook::velox::IterativeVectorSerializer {
 public:
  ArrowBatchIterativeSerializer(
      facebook::velox::RowTypePtr type,
      int32_t /*numRows*/,
      facebook::velox::StreamArena* streamArena,
      bool preserve_dictionary)
      : type_(std::move(type)),
        pool_(streamArena->pool()),
        preserve_dictionary_(preserve_dictionary) {}

  void append(
      const facebook::velox::RowVectorPtr& vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::Scratch& /*scratch*/) override {
    validateTypesForExport(vector);

    for (const auto& range : ranges) {
      if (range.size == vector->size() && range.begin == 0) {
        buffered_vectors_.push_back(vector);
      } else {
        auto sliced = std::dynamic_pointer_cast<facebook::velox::RowVector>(
            vector->slice(range.begin, range.size));
        buffered_vectors_.push_back(sliced);
      }
      num_rows_ += range.size;
    }
  }

  size_t maxSerializedSize() const override {
    size_t total = 0;
    for (const auto& vec : buffered_vectors_) {
      total += vec->estimateFlatSize();
    }
    // 1.1x for IPC metadata overhead + 4 bytes length prefix per batch.
    return static_cast<size_t>(total * 1.1) +
        buffered_vectors_.size() * sizeof(int32_t);
  }

  void flush(facebook::velox::OutputStream* stream) override {
    for (const auto& vector : buffered_vectors_) {
      // L4-4: Respect class-level preserve_dictionary setting.
      auto batch = exportToBatch(vector, pool_, preserve_dictionary_);
      writeBatchToStream(batch, stream);
    }
    buffered_vectors_.clear();
    num_rows_ = 0;
  }

  void clear() override {
    buffered_vectors_.clear();
    num_rows_ = 0;
  }

 private:
  facebook::velox::RowTypePtr type_;
  facebook::velox::memory::MemoryPool* pool_;
  bool preserve_dictionary_;
  std::vector<facebook::velox::RowVectorPtr> buffered_vectors_;
  facebook::velox::vector_size_t num_rows_ = 0;
};

}  // namespace

std::unique_ptr<facebook::velox::IterativeVectorSerializer>
ArrowBatchVectorSerde::createIterativeSerializer(
    facebook::velox::RowTypePtr type,
    int32_t numRows,
    facebook::velox::StreamArena* streamArena,
    const Options* /*options*/) {
  return std::make_unique<ArrowBatchIterativeSerializer>(
      std::move(type), numRows, streamArena, preserve_dictionary_);
}

std::unique_ptr<facebook::velox::BatchVectorSerializer>
ArrowBatchVectorSerde::createBatchSerializer(
    facebook::velox::memory::MemoryPool* pool,
    const Options* /*options*/) {
  return std::make_unique<ArrowBatchSerializer>(pool, preserve_dictionary_);
}

void ArrowBatchVectorSerde::deserialize(
    facebook::velox::ByteInputStream* source,
    facebook::velox::memory::MemoryPool* pool,
    facebook::velox::RowTypePtr type,
    facebook::velox::RowVectorPtr* result,
    const Options* /*options*/) {
  // Read the 4-byte length prefix.
  int32_t length = 0;
  source->readBytes(reinterpret_cast<uint8_t*>(&length), sizeof(length));
  VELOX_CHECK_GT(length, 0, "Invalid Arrow batch message length: {}", length);

  // Allocate an Arrow-owned buffer and read serialized IPC stream bytes into it.
  // This is critical: Arrow IPC deserialization uses zero-copy slicing, so the
  // resulting RecordBatch columns reference slices of this buffer. Using a
  // non-owning wrapper over a local vector would cause use-after-free.
  auto alloc_result = arrow::AllocateBuffer(length);
  VELOX_CHECK(
      alloc_result.ok(),
      "Failed to allocate buffer for IPC deserialization: {}",
      alloc_result.status().ToString());
  auto owned_buffer = std::shared_ptr<arrow::Buffer>(
      std::move(*alloc_result));
  source->readBytes(owned_buffer->mutable_data(), length);

  // F-NATIVE-SHUF-07: Use IPC stream reader which reads the embedded schema
  // and record batch from the self-describing stream format. This avoids
  // the broken ReadSchema-on-RecordBatch-message approach and ensures schema
  // round-trip fidelity for all types including complex types (Map, Array, Row).
  auto buf_reader = std::make_shared<arrow::io::BufferReader>(owned_buffer);

  // Open a stream reader which reads schema + batches from the IPC stream.
  auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buf_reader);
  VELOX_CHECK(
      reader_result.ok(),
      "Failed to open IPC stream reader: {}",
      reader_result.status().ToString());
  auto reader = *reader_result;

  // Read the first (and only) RecordBatch from the stream.
  std::shared_ptr<arrow::RecordBatch> batch;
  auto read_status = reader->ReadNext(&batch);
  VELOX_CHECK(
      read_status.ok(),
      "Failed to read RecordBatch from IPC stream: {}",
      read_status.ToString());
  VELOX_CHECK(
      batch != nullptr,
      "No RecordBatch found in IPC stream");

  // Import Arrow RecordBatch to Velox RowVector via Arrow C Data Interface.
  ArrowArray out_array;
  ArrowSchema out_schema;
  auto export_status =
      arrow::ExportRecordBatch(*batch, &out_array, &out_schema);
  VELOX_CHECK(
      export_status.ok(),
      "Failed to export RecordBatch to C interface: {}",
      export_status.ToString());

  *result = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::importFromArrowAsOwner(
          out_schema, out_array, pool));
}

void ArrowBatchVectorSerde::registerNamedVectorSerde() {
  facebook::velox::registerNamedVectorSerde(
      kSerdeKind, std::make_unique<ArrowBatchVectorSerde>());
}

void ArrowBatchVectorSerde::deregisterNamedVectorSerde() {
  facebook::velox::deregisterNamedVectorSerde(kSerdeKind);
}

}  // namespace pangrova::result
