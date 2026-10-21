#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "folly/dynamic.h"

namespace facebook::velox {
class ReadFile;
}

namespace pangrova::velox {

// Reads records from one pinned Avro object-container file. The container is
// streamed by block; only a bounded decompressed block is retained at a time.
// Unsupported codecs and schemas fail closed before producing a row.
class AvroContainerReader final {
 public:
  AvroContainerReader(
      std::shared_ptr<facebook::velox::ReadFile> file,
      uint64_t expectedLength,
      std::string expectedSha256);

  std::optional<folly::dynamic> nextRecord();
  uint64_t lastRecordSize() const { return last_record_size_; }
  std::string metadataValue(const std::string& key) const;
  const folly::dynamic& writerSchema() const { return writer_schema_; }

 private:
  int64_t readFileLong();
  std::string readFileBytes();
  std::string readExact(uint64_t size);
  void parseHeader();
  bool readNextBlock();

  std::shared_ptr<facebook::velox::ReadFile> file_;
  uint64_t file_size_ = 0;
  uint64_t file_position_ = 0;
  std::string codec_;
  std::unordered_map<std::string, std::string> metadata_;
  std::string sync_marker_;
  folly::dynamic writer_schema_;
  std::string block_;
  uint64_t block_position_ = 0;
  uint64_t remaining_rows_ = 0;
  uint64_t last_record_size_ = 0;
};

}  // namespace pangrova::velox
