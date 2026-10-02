// ArrowBatchVectorSerde: Serializes Velox RowVector to Arrow IPC RecordBatch.
// See ArrowBatchVectorSerde.h for class documentation.

#include "result/ArrowBatchVectorSerde.h"
#include "columnar/CanonicalBatchStream.h"
#include <limits>

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

struct ExportedColumnarData {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ExportedColumnarData() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

// Validates that all column types in the RowVector are representable in Arrow
// IPC format. Throws ArrowBatchUnsupportedTypeException for opaque types.
void validateExportType(const facebook::velox::TypePtr& type) {
  if (type->kind() == facebook::velox::TypeKind::OPAQUE) {
    throw ArrowBatchUnsupportedTypeException(type->toString());
  }
  for (uint32_t i = 0; i < type->size(); ++i) validateExportType(type->childAt(i));
}
void validateTypesForExport(const facebook::velox::RowVectorPtr& vector) {
  validateExportType(vector->type());
}

// Exports a Velox RowVector to an Arrow RecordBatch via Arrow C Data Interface.
// Physical encodings are normalized so every batch has one logical wire schema.
std::shared_ptr<arrow::RecordBatch> exportToBatch(
    const facebook::velox::RowVectorPtr& vector,
    facebook::velox::memory::MemoryPool* pool) {
  ExportedColumnarData data;
  ArrowOptions arrow_opts;
  // Transport schema is fixed by the logical types, not per-batch encodings.
  arrow_opts.flattenDictionary = true;
  arrow_opts.flattenConstant = true;
  facebook::velox::exportToArrow(vector, data.array, pool, arrow_opts);
  facebook::velox::exportToArrow(vector, data.schema, arrow_opts);

  auto result = arrow::ImportRecordBatch(&data.array, &data.schema);
  VELOX_CHECK(
      result.ok(),
      "Failed to export RowVector to RecordBatch: {}",
      result.status().ToString());
  return *result;
}

// One plain Arrow stream per OutputBuffer payload, without private framing.
void writeBatchesToStream(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    facebook::velox::OutputStream* stream,
    const std::shared_ptr<pangrova::memory::ColumnarMemoryPool>& pool) {
  const auto encoded = columnar::encodeBatchStream(batches, pool);
  VELOX_CHECK(encoded.ok(), "Arrow stream serialization failed: {}", encoded.status().ToString());
  const auto& buffer = *encoded;
  stream->write(reinterpret_cast<const char*>(buffer->data()), buffer->size());
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
class ArrowBatchSerializer : public facebook::velox::BatchVectorSerializer {
 public:
  ArrowBatchSerializer(
      facebook::velox::memory::MemoryPool* pool)
      : pool_(pool),
        columnar_pool_(std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool->shared_from_this())) {}

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

    auto batch = exportToBatch(target, pool_);

    writeBatchesToStream({batch}, stream, columnar_pool_);
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
  std::shared_ptr<pangrova::memory::ColumnarMemoryPool> columnar_pool_;
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
      facebook::velox::StreamArena* streamArena)
      : type_(std::move(type)),
        pool_(streamArena->pool()),
        columnar_pool_(std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_->shared_from_this())) {}

  void append(
      const facebook::velox::RowVectorPtr& vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::Scratch& /*scratch*/) override {
    validateTypesForExport(vector);
    VELOX_USER_CHECK(vector->type()->equivalent(*type_),
        "Arrow stream input type differs from its declared output type");
    encoded_.reset();

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
    prepareEncoded();
    return encoded_ ? static_cast<size_t>(encoded_->size()) : 0;
  }

  void flush(facebook::velox::OutputStream* stream) override {
    prepareEncoded();
    if (encoded_) stream->write(reinterpret_cast<const char*>(encoded_->data()), encoded_->size());
    clear();
  }

  void clear() override {
    buffered_vectors_.clear();
    encoded_.reset();
    num_rows_ = 0;
  }

 private:
  void prepareEncoded() const {
    if (encoded_ || buffered_vectors_.empty()) return;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (const auto& vector : buffered_vectors_) batches.push_back(exportToBatch(vector, pool_));
    auto encoded = columnar::encodeBatchStream(batches, columnar_pool_);
    VELOX_CHECK(encoded.ok(), "Arrow stream serialization failed: {}", encoded.status().ToString());
    encoded_ = *encoded;
  }

  facebook::velox::RowTypePtr type_;
  facebook::velox::memory::MemoryPool* pool_;
  std::shared_ptr<pangrova::memory::ColumnarMemoryPool> columnar_pool_;
  std::vector<facebook::velox::RowVectorPtr> buffered_vectors_;
  facebook::velox::vector_size_t num_rows_ = 0;
  mutable std::shared_ptr<arrow::Buffer> encoded_;
};

}  // namespace

std::unique_ptr<facebook::velox::IterativeVectorSerializer>
ArrowBatchVectorSerde::createIterativeSerializer(
    facebook::velox::RowTypePtr type,
    int32_t numRows,
    facebook::velox::StreamArena* streamArena,
    const Options* /*options*/) {
  return std::make_unique<ArrowBatchIterativeSerializer>(
      std::move(type), numRows, streamArena);
}

std::unique_ptr<facebook::velox::BatchVectorSerializer>
ArrowBatchVectorSerde::createBatchSerializer(
    facebook::velox::memory::MemoryPool* pool,
    const Options* /*options*/) {
  return std::make_unique<ArrowBatchSerializer>(pool);
}

void ArrowBatchVectorSerde::deserialize(
    facebook::velox::ByteInputStream* source,
    facebook::velox::memory::MemoryPool* pool,
    facebook::velox::RowTypePtr type,
    facebook::velox::RowVectorPtr* result,
    const Options* /*options*/) {
  VELOX_CHECK_NOT_NULL(source);
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK_NOT_NULL(result);
  const auto length = source->remainingSize();
  VELOX_CHECK_LE(length, static_cast<size_t>(columnar::kMaximumBatchStreamBytes));
  auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool->shared_from_this());
  auto allocation = arrow::AllocateBuffer(length, allocator.get());
  VELOX_CHECK(allocation.ok(), "Arrow stream allocation failed: {}", allocation.status().ToString());
  auto buffer = pangrova::memory::retainColumnarBuffer(
      std::shared_ptr<arrow::Buffer>(std::move(*allocation)), allocator);
  source->readBytes(buffer->mutable_data(), length);
  auto decoded = columnar::decodeBatchStream(std::move(buffer), allocator);
  VELOX_CHECK(decoded.ok(), "Arrow stream decode failed: {}", decoded.status().ToString());
  std::vector<facebook::velox::RowVectorPtr> vectors;
  int64_t rows = 0;
  for (const auto& batch : *decoded) {
    ExportedColumnarData data;
    const auto exported = arrow::ExportRecordBatch(*batch, &data.array, &data.schema);
    VELOX_CHECK(exported.ok(), "Arrow batch export failed: {}", exported.ToString());
    auto ownedArray = data.array;
    auto ownedSchema = data.schema;
    data.array.release = nullptr;
    data.schema.release = nullptr;
    auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::importFromArrowAsOwner(ownedSchema, ownedArray, pool));
    VELOX_CHECK_NOT_NULL(vector);
    VELOX_CHECK(vector->type()->equivalent(*type) && vector->type()->asRow().names() == type->names(),
        "Arrow stream schema differs from the exact task output schema");
    rows += vector->size();
    VELOX_CHECK_LE(rows, std::numeric_limits<facebook::velox::vector_size_t>::max());
    vectors.push_back(std::move(vector));
  }
  if (vectors.size() == 1) {
    *result = std::move(vectors.front());
    return;
  }
  *result = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(type, rows, pool));
  facebook::velox::vector_size_t offset = 0;
  for (const auto& vector : vectors) {
    (*result)->copy(vector.get(), offset, 0, vector->size());
    offset += vector->size();
  }
}

void ArrowBatchVectorSerde::registerNamedVectorSerde() {
  facebook::velox::registerNamedVectorSerde(
      kSerdeKind, std::make_unique<ArrowBatchVectorSerde>());
}

void ArrowBatchVectorSerde::deregisterNamedVectorSerde() {
  facebook::velox::deregisterNamedVectorSerde(kSerdeKind);
}

}  // namespace pangrova::result
