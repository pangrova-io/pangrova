// ArrowBatchVectorSerde: Serializes Velox RowVector to Arrow IPC RecordBatch.
// See ArrowBatchVectorSerde.h for class documentation.

#include "velox/serde/ArrowBatchVectorSerde.h"
#include "velox/serde/CanonicalResultNullability.h"
#include "velox/serde/CanonicalVectorNames.h"
#include "velox/serde/CanonicalArrowSchema.h"
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
#include <arrow/util/bitmap_ops.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::velox {

namespace {

using RowCount = facebook::velox::vector_size_t;

void validateRange(const facebook::velox::BaseVector* vector, const facebook::velox::IndexRange& range) {
  VELOX_CHECK_NOT_NULL(vector);
  VELOX_USER_CHECK(range.begin >= 0 && range.size >= 0 && range.begin <= vector->size() &&
      range.size <= vector->size() - range.begin, "Arrow row selection exceeds input bounds");
}

void addEstimate(RowCount* size, uint64_t average, uint64_t count) {
  VELOX_CHECK_NOT_NULL(size);
  VELOX_USER_CHECK_GE(*size, 0, "Arrow size estimate cannot be negative");
  const auto remaining = std::numeric_limits<RowCount>::max() - *size;
  *size += count > static_cast<uint64_t>(remaining) / average
      ? remaining : static_cast<RowCount>(count * average);
}

void estimateRanges(const facebook::velox::BaseVector* vector,
    const folly::Range<const facebook::velox::IndexRange*>& ranges, RowCount** sizes) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto average = vector->estimateFlatSize() / std::max<RowCount>(1, vector->size()) + 16;
  for (size_t i = 0; i < ranges.size(); ++i) {
    validateRange(vector, ranges[i]);
    // Estimates are hints, not wire-size proofs; saturate rather than overflow.
    addEstimate(sizes[i], average, ranges[i].size);
  }
}

columnar::BatchStreamOptions streamOptions(const facebook::velox::VectorSerde::Options* options) {
  columnar::BatchStreamOptions config;
  if (!options) return config;
  using Kind = facebook::velox::common::CompressionKind;
  switch (options->compressionKind) {
    case Kind::CompressionKind_NONE: break;
    case Kind::CompressionKind_LZ4: config.compression = arrow::Compression::LZ4_FRAME; break;
    case Kind::CompressionKind_ZSTD: config.compression = arrow::Compression::ZSTD; break;
    default: VELOX_USER_FAIL("Arrow stream supports only none, lz4 and zstd compression");
  }
  config.minimum_space_savings = 1 - options->minCompressionRatio;
  if (const auto* stream = dynamic_cast<const ArrowBatchVectorSerde::StreamOptions*>(options)) {
    config.maximum_bytes = stream->maximum_bytes;
  }
  return config;
}

const ArrowBatchVectorSerde::StreamOptions& canonicalOptions(
    const facebook::velox::VectorSerde::Options* options) {
  const auto* stream = dynamic_cast<const ArrowBatchVectorSerde::StreamOptions*>(options);
  VELOX_USER_CHECK(stream && stream->canonical_schema,
      "Arrow batch serde requires its admitted canonical schema options");
  VELOX_USER_CHECK(stream->maximum_bytes > 0 &&
      stream->maximum_bytes <= columnar::kMaximumBatchStreamBytes,
      "Arrow batch serde requires a bounded positive wire size");
  return *stream;
}

struct ExportedColumnarData {
  ArrowArray array{};
  ArrowSchema schema{};
  ~ExportedColumnarData() {
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
  }
};

// Velox may retain an old cached null count after an expression clears its
// nulls. Arrow's C Data Interface requires the count to describe the exported
// validity bitmap, so derive it from that bitmap at the transport boundary.
void reconcileExportedNullCounts(ArrowArray& array) {
  VELOX_CHECK_GE(array.length, 0);
  VELOX_CHECK_GE(array.offset, 0);
  VELOX_CHECK_GT(array.n_buffers, 0);
  VELOX_CHECK_NOT_NULL(array.buffers);
  if (const auto* validity = static_cast<const uint8_t*>(array.buffers[0])) {
    array.null_count = array.length -
        arrow::internal::CountSetBits(validity, array.offset, array.length);
  } else {
    VELOX_CHECK_EQ(array.null_count, 0,
        "Exported Arrow array has nulls but no validity bitmap");
  }
  VELOX_CHECK_GE(array.n_children, 0);
  for (int64_t i = 0; i < array.n_children; ++i) {
    VELOX_CHECK_NOT_NULL(array.children);
    VELOX_CHECK_NOT_NULL(array.children[i]);
    reconcileExportedNullCounts(*array.children[i]);
  }
  if (array.dictionary) reconcileExportedNullCounts(*array.dictionary);
}

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
  reconcileExportedNullCounts(data.array);

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
    const std::shared_ptr<pangrova::memory::ColumnarMemoryPool>& pool,
    const columnar::BatchStreamOptions& options) {
  const auto encoded = columnar::encodeBatchStream(batches, pool, options);
  if (encoded.status().IsCapacityError()) throw ArrowBatchWireLimitException(encoded.status().ToString());
  VELOX_CHECK(encoded.ok(), "Arrow stream serialization failed: {}", encoded.status().ToString());
  const auto& buffer = *encoded;
  stream->write(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

// Selects rows from a RowVector by index ranges, returning a flat RowVector.
facebook::velox::RowVectorPtr selectRows(
    const facebook::velox::RowVectorPtr& vector,
    const folly::Range<const facebook::velox::IndexRange*>& ranges,
    facebook::velox::memory::MemoryPool* pool) {
  facebook::velox::vector_size_t total_rows = 0;
  for (const auto& range : ranges) {
    validateRange(vector.get(), range);
    VELOX_USER_CHECK_LE(range.size, std::numeric_limits<RowCount>::max() - total_rows,
        "Arrow selected rows exceed vector row capacity");
    total_rows += range.size;
  }
  if (ranges.size() == 1) {
    return std::dynamic_pointer_cast<facebook::velox::RowVector>(
        vector->slice(ranges[0].begin, ranges[0].size));
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
      facebook::velox::memory::MemoryPool* pool, columnar::BatchStreamOptions options,
      std::shared_ptr<const substrait::NamedStruct> schema)
      : pool_(pool),
        columnar_pool_(std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool->shared_from_this())),
        options_(std::move(options)), canonical_schema_(std::move(schema)),
        wire_schema_(canonicalArrowSchema(*canonical_schema_, pool)) {}

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

    target = std::dynamic_pointer_cast<facebook::velox::RowVector>(bindCanonicalVectorNames(
        target, admittedCanonicalResultType(*canonical_schema_)));
    validateCanonicalResultNullability(*canonical_schema_, target);
    auto batch = bindCanonicalArrowSchema(exportToBatch(target, pool_), wire_schema_);

    writeBatchesToStream({batch}, stream, columnar_pool_, options_);
  }

  void estimateSerializedSize(
      facebook::velox::VectorPtr vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::vector_size_t** sizes,
      facebook::velox::Scratch& /*scratch*/) override {
    estimateRanges(vector.get(), ranges, sizes);
  }

 private:
  facebook::velox::memory::MemoryPool* pool_;
  std::shared_ptr<pangrova::memory::ColumnarMemoryPool> columnar_pool_;
  const columnar::BatchStreamOptions options_;
  const std::shared_ptr<const substrait::NamedStruct> canonical_schema_;
  const std::shared_ptr<arrow::Schema> wire_schema_;
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
      facebook::velox::StreamArena* streamArena, columnar::BatchStreamOptions options,
      std::shared_ptr<const substrait::NamedStruct> schema)
      : type_(std::move(type)),
        pool_(streamArena->pool()),
        columnar_pool_(std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool_->shared_from_this())),
        options_(std::move(options)), canonical_schema_(std::move(schema)),
        wire_schema_(canonicalArrowSchema(*canonical_schema_, pool_)) {}

  void append(
      const facebook::velox::RowVectorPtr& vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::Scratch& /*scratch*/) override {
    validateTypesForExport(vector);
    VELOX_USER_CHECK(*vector->type() == *type_,
        "Arrow stream input type differs from its declared output type");
    // Stage every selected owner before publishing any of this append. A late
    // invalid range or allocation failure must leave the buffered prefix intact.
    std::vector<facebook::velox::RowVectorPtr> selected;
    selected.reserve(ranges.size());
    for (const auto& range : ranges) {
      validateRange(vector.get(), range);
      if (range.size == vector->size() && range.begin == 0) {
        selected.push_back(vector);
      } else {
        auto sliced = std::dynamic_pointer_cast<facebook::velox::RowVector>(
            vector->slice(range.begin, range.size));
        selected.push_back(std::move(sliced));
      }
    }
    const auto declared = admittedCanonicalResultType(*canonical_schema_);
    for (auto& target : selected) {
      target = std::dynamic_pointer_cast<facebook::velox::RowVector>(bindCanonicalVectorNames(target, declared));
      validateCanonicalResultNullability(*canonical_schema_, target);
    }
    buffered_vectors_.insert(buffered_vectors_.end(), selected.begin(), selected.end());
    encoded_.reset();
  }

  void append(const facebook::velox::RowVectorPtr& vector,
      const folly::Range<const RowCount*>& rows, facebook::velox::Scratch& scratch) override {
    std::vector<facebook::velox::IndexRange> ranges;
    ranges.reserve(rows.size());
    for (const auto row : rows) {
      validateRange(vector.get(), {row, 1});
      if (!ranges.empty() && ranges.back().begin + ranges.back().size == row) {
        ++ranges.back().size;
      } else {
        ranges.push_back({row, 1});
      }
    }
    append(vector, folly::Range<const facebook::velox::IndexRange*>(ranges.data(), ranges.size()), scratch);
  }

  bool supportsAppendRows() const override { return true; }

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
  }

 private:
  void prepareEncoded() const {
    if (encoded_ || buffered_vectors_.empty()) return;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (const auto& vector : buffered_vectors_) {
      batches.push_back(bindCanonicalArrowSchema(exportToBatch(vector, pool_), wire_schema_));
    }
    auto encoded = columnar::encodeBatchStream(batches, columnar_pool_, options_);
    if (encoded.status().IsCapacityError()) throw ArrowBatchWireLimitException(encoded.status().ToString());
    VELOX_CHECK(encoded.ok(), "Arrow stream serialization failed: {}", encoded.status().ToString());
    encoded_ = *encoded;
  }

  facebook::velox::RowTypePtr type_;
  facebook::velox::memory::MemoryPool* pool_;
  std::shared_ptr<pangrova::memory::ColumnarMemoryPool> columnar_pool_;
  const columnar::BatchStreamOptions options_;
  const std::shared_ptr<const substrait::NamedStruct> canonical_schema_;
  const std::shared_ptr<arrow::Schema> wire_schema_;
  std::vector<facebook::velox::RowVectorPtr> buffered_vectors_;
  mutable std::shared_ptr<arrow::Buffer> encoded_;
};

}  // namespace

void ArrowBatchVectorSerde::estimateSerializedSize(const facebook::velox::BaseVector* vector,
    const folly::Range<const RowCount*>& rows, RowCount** sizes, facebook::velox::Scratch& /*scratch*/) {
  VELOX_CHECK_NOT_NULL(vector);
  const auto average = vector->estimateFlatSize() / std::max<RowCount>(1, vector->size()) + 16;
  for (size_t i = 0; i < rows.size(); ++i) {
    const facebook::velox::IndexRange range{rows[i], 1};
    validateRange(vector, range);
    addEstimate(sizes[i], average, 1);
  }
}

void ArrowBatchVectorSerde::estimateSerializedSize(const facebook::velox::BaseVector* vector,
    const folly::Range<const facebook::velox::IndexRange*>& ranges,
    RowCount** sizes, facebook::velox::Scratch& /*scratch*/) {
  estimateRanges(vector, ranges, sizes);
}

std::unique_ptr<facebook::velox::IterativeVectorSerializer>
ArrowBatchVectorSerde::createIterativeSerializer(
    facebook::velox::RowTypePtr type,
    int32_t numRows,
    facebook::velox::StreamArena* streamArena,
    const Options* options) {
  return std::make_unique<ArrowBatchIterativeSerializer>(
      std::move(type), numRows, streamArena, streamOptions(options),
      canonicalOptions(options).canonical_schema);
}

std::unique_ptr<facebook::velox::BatchVectorSerializer>
ArrowBatchVectorSerde::createBatchSerializer(
    facebook::velox::memory::MemoryPool* pool,
    const Options* options) {
  return std::make_unique<ArrowBatchSerializer>(pool, streamOptions(options),
      canonicalOptions(options).canonical_schema);
}

void ArrowBatchVectorSerde::deserialize(
    facebook::velox::ByteInputStream* source,
    facebook::velox::memory::MemoryPool* pool,
    facebook::velox::RowTypePtr type,
    facebook::velox::RowVectorPtr* result,
    const Options* options) {
  deserialize(source, pool, std::move(type), result, 0, options);
}

void ArrowBatchVectorSerde::deserialize(
    facebook::velox::ByteInputStream* source,
    facebook::velox::memory::MemoryPool* pool,
    facebook::velox::RowTypePtr type,
    facebook::velox::RowVectorPtr* result,
    facebook::velox::vector_size_t resultOffset,
    const Options* options) {
  VELOX_CHECK_NOT_NULL(source);
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK_NOT_NULL(type);
  VELOX_CHECK_NOT_NULL(result);
  const auto& admitted = canonicalOptions(options);
  const auto wireSchema = canonicalArrowSchema(*admitted.canonical_schema, pool);
  const auto taskSchema = physicalArrowSchema(wireSchema, *admitted.canonical_schema, type);
  VELOX_USER_CHECK_GE(resultOffset, 0, "Arrow append result offset must be non-negative");
  if (resultOffset > 0) {
    VELOX_USER_CHECK(*result, "Arrow append nonzero offset requires an existing output");
    VELOX_USER_CHECK_LE(resultOffset, (*result)->size(), "Arrow append result offset exceeds output rows");
    VELOX_USER_CHECK(*(*result)->type() == *type, "Arrow append output schema differs from task schema");
  }
  const auto length = source->remainingSize();
  VELOX_USER_CHECK_LE(length, static_cast<size_t>(admitted.maximum_bytes),
      "Arrow stream exceeds its admitted wire size");
  auto allocator = std::make_shared<pangrova::memory::ColumnarMemoryPool>(pool->shared_from_this());
  auto allocation = arrow::AllocateBuffer(length, allocator.get());
  VELOX_CHECK(allocation.ok(), "Arrow stream allocation failed: {}", allocation.status().ToString());
  auto buffer = pangrova::memory::retainColumnarBuffer(
      std::shared_ptr<arrow::Buffer>(std::move(*allocation)), allocator);
  source->readBytes(buffer->mutable_data(), length);
  auto decoded = columnar::decodeBatchStream(std::move(buffer), allocator, admitted.maximum_bytes);
  VELOX_CHECK(decoded.ok(), "Arrow stream decode failed: {}", decoded.status().ToString());
  std::vector<facebook::velox::RowVectorPtr> vectors;
  int64_t rows = resultOffset;
  for (const auto& batch : *decoded) {
    VELOX_USER_CHECK(batch->schema()->Equals(*wireSchema, true),
        "Arrow wire schema differs from its admitted canonical schema; actual: {}; admitted: {}",
        batch->schema()->ToString(), wireSchema->ToString());
    // Arrow cardinality is int64; validate before the SDK narrows to vector_size_t.
    VELOX_USER_CHECK_LE(batch->num_rows(),
        std::numeric_limits<facebook::velox::vector_size_t>::max() - rows,
        "Arrow append total rows exceed vector row capacity");
    ExportedColumnarData data;
    const auto physical = bindCanonicalArrowSchema(batch, taskSchema);
    const auto exported = arrow::ExportRecordBatch(*physical, &data.array, &data.schema);
    VELOX_CHECK(exported.ok(), "Arrow batch export failed: {}", exported.ToString());
    auto vector = std::dynamic_pointer_cast<facebook::velox::RowVector>(
        facebook::velox::importFromArrowAsOwner(data.schema, data.array, pool));
    VELOX_CHECK_NOT_NULL(vector);
    VELOX_CHECK(*vector->type() == *type,
        "Arrow stream schema differs from the exact task output schema");
    validateCanonicalResultNullability(*admitted.canonical_schema, vector);
    rows += vector->size();
    VELOX_USER_CHECK_LE(rows, std::numeric_limits<facebook::velox::vector_size_t>::max(),
        "Arrow append total rows exceed vector row capacity");
    vectors.push_back(std::move(vector));
  }
  if (resultOffset == 0 && vectors.size() == 1) {
    *result = std::move(vectors.front());
    return;
  }
  auto combined = std::dynamic_pointer_cast<facebook::velox::RowVector>(
      facebook::velox::BaseVector::create(type, rows, pool));
  if (resultOffset > 0) combined->copy(result->get(), 0, 0, resultOffset);
  facebook::velox::vector_size_t offset = resultOffset;
  for (const auto& vector : vectors) {
    combined->copy(vector.get(), offset, 0, vector->size());
    offset += vector->size();
  }
  *result = std::move(combined);
}

void ArrowBatchVectorSerde::registerNamedVectorSerde() {
  facebook::velox::registerNamedVectorSerde(
      kSerdeKind, std::make_unique<ArrowBatchVectorSerde>());
}

void ArrowBatchVectorSerde::deregisterNamedVectorSerde() {
  facebook::velox::deregisterNamedVectorSerde(kSerdeKind);
}

}  // namespace pangrova::velox
