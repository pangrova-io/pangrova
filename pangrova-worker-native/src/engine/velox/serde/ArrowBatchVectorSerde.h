// ArrowBatchVectorSerde: Velox VectorSerde implementation that serializes
// RowVector to Arrow IPC RecordBatch format. Registered in Velox's named
// serde registry as "ArrowBatch" for use by PartitionedOutputNode.
//
// When PartitionedOutputNode uses this serde, output is pre-serialized in
// Arrow IPC format. FlightResultServer can then stream these buffers directly
// to the authorized client-facing data-plane consumer.
//
// Unsupported types (Velox-specific opaque types not representable in Arrow
// IPC) fail with ArrowBatchUnsupportedTypeException; no query rerun occurs.
//
// Shared by result and shuffle at the Velox VectorSerde boundary.
#pragma once

#include <stdexcept>
#include <memory>
#include <string>

#include "velox/vector/VectorStream.h"
#include "substrait/type.pb.h"

namespace pangrova::velox {

// Exception thrown when ArrowBatchVectorSerde encounters a Velox type that
// cannot be represented in the canonical Arrow wire format.
class ArrowBatchUnsupportedTypeException : public std::runtime_error {
 public:
  explicit ArrowBatchUnsupportedTypeException(const std::string& type_name)
      : std::runtime_error(
            "ArrowBatch serde does not support type: " + type_name),
        type_name_(type_name) {}

  const std::string& typeName() const { return type_name_; }

 private:
  std::string type_name_;
};

class ArrowBatchWireLimitException : public std::runtime_error {
 public:
  explicit ArrowBatchWireLimitException(const std::string& message) : std::runtime_error(message) {}
};

// Velox VectorSerde that serializes RowVector to Arrow IPC RecordBatch format.
//
// Supports all standard Arrow types (integers, floats, strings, binary,
// decimal, timestamp, date, arrays, maps, structs). Throws
// ArrowBatchUnsupportedTypeException for Velox-specific opaque types.
class ArrowBatchVectorSerde : public facebook::velox::VectorSerde {
 public:
  static constexpr const char* kSerdeKind = "ArrowBatch";

  struct StreamOptions : public Options {
    int64_t maximum_bytes = 256LL * 1024 * 1024;
    std::shared_ptr<const substrait::NamedStruct> canonical_schema;
  };

  ArrowBatchVectorSerde() : VectorSerde(kSerdeKind) {}

  void estimateSerializedSize(const facebook::velox::BaseVector* vector,
      const folly::Range<const facebook::velox::vector_size_t*>& rows,
      facebook::velox::vector_size_t** sizes, facebook::velox::Scratch& scratch) override;

  void estimateSerializedSize(const facebook::velox::BaseVector* vector,
      const folly::Range<const facebook::velox::IndexRange*>& ranges,
      facebook::velox::vector_size_t** sizes, facebook::velox::Scratch& scratch) override;

  // Creates an iterative serializer. Arrow IPC is batch-oriented, so this
  // delegates to a batch-based approach internally.
  std::unique_ptr<facebook::velox::IterativeVectorSerializer>
  createIterativeSerializer(
      facebook::velox::RowTypePtr type,
      int32_t numRows,
      facebook::velox::StreamArena* streamArena,
      const Options* options = nullptr) override;

  // Creates a batch serializer that writes Arrow IPC RecordBatch to the
  // output stream.
  std::unique_ptr<facebook::velox::BatchVectorSerializer>
  createBatchSerializer(
      facebook::velox::memory::MemoryPool* pool,
      const Options* options = nullptr) override;

  // Deserialize Arrow IPC bytes back to RowVector.
  void deserialize(
      facebook::velox::ByteInputStream* source,
      facebook::velox::memory::MemoryPool* pool,
      facebook::velox::RowTypePtr type,
      facebook::velox::RowVectorPtr* result,
      const Options* options = nullptr) override;

  bool supportsAppendInDeserialize() const override { return true; }

  // SDK columnar Exchange API. Publishes a new result only after the complete
  // stream, exact schema and checked row count have been validated.
  void deserialize(
      facebook::velox::ByteInputStream* source,
      facebook::velox::memory::MemoryPool* pool,
      facebook::velox::RowTypePtr type,
      facebook::velox::RowVectorPtr* result,
      facebook::velox::vector_size_t resultOffset,
      const Options* options = nullptr) override;

  // Register this serde in Velox's named serde registry as "ArrowBatch".
  static void registerNamedVectorSerde();

  // Deregister from Velox's named serde registry.
  static void deregisterNamedVectorSerde();

};

}  // namespace pangrova::velox
