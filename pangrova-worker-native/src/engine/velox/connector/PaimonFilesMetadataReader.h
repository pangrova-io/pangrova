#pragma once

#include <array>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "folly/dynamic.h"
#include "pangrova/protocol/v2/connector.pb.h"
#include "engine/velox/connector/AvroContainerReader.h"
#include "engine/velox/connector/MetadataReadContext.h"
#include "engine/velox/connector/PaimonBinaryRow.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Streams exact, SDK-selected physical metadata; no projected rows arrive from Java.
class PaimonFilesMetadataReader final {
 public:
  PaimonFilesMetadataReader(const protocol::v2::PinnedMetadataObject& manifest,
      uint64_t snapshotId, const std::string& context,
      facebook::velox::RowTypePtr outputType, facebook::velox::memory::MemoryPool* pool,
      const MetadataReadContext& reads = MetadataReadContext::unscoped());
  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  struct Field { int64_t id; std::string name; PaimonScalarType type; };
  struct Schema {
    std::vector<Field> fields;
    std::vector<int> partitions;
    std::vector<int> keys;
    std::vector<int> sorted;
  };
  struct Selection { std::string uri; bool seen = false; };
  static Schema schema(const std::string& json, uint64_t id);
  static std::string identifier(const folly::dynamic& entry);
  std::string partition(const folly::dynamic& entry) const;
  std::string key(const folly::dynamic& data, bool lower) const;
  std::array<std::string, 3> stats(const folly::dynamic& data) const;
  void validateOutput() const;
  void emit(const folly::dynamic& entry, const std::string& uri,
      const facebook::velox::RowVectorPtr& output, int row) const;
  std::unique_ptr<AvroContainerReader> input_;
  std::unordered_map<std::string, Selection> selected_;
  std::unordered_map<uint64_t, Schema> schemas_;
  uint64_t read_schema_id_;
  uint64_t remaining_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
};

}  // namespace pangrova::velox
