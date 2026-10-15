#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox {
class ReadFile;
namespace memory { class MemoryPool; }
}  // namespace facebook::velox

namespace pangrova::velox {

// One pinned Paimon metadata JSON object produces bounded Native row batches.
class PaimonSnapshotJsonReader final {
 public:
  PaimonSnapshotJsonReader(
      const std::string& uri,
      uint64_t expectedLength,
      uint64_t expectedSnapshotId,
      std::string expectedSha256,
      facebook::velox::RowTypePtr outputType,
      facebook::velox::memory::MemoryPool* pool,
      std::string tagName = {},
      std::optional<uint64_t> expectedSchemaId = std::nullopt,
      bool schemaHistory = false);

  facebook::velox::RowVectorPtr next(uint64_t maxRows);

 private:
  void validateOutputSchema() const;
  facebook::velox::RowVectorPtr nextOptions(uint64_t maxRows);

  std::shared_ptr<facebook::velox::ReadFile> file_;
  facebook::velox::RowTypePtr output_type_;
  facebook::velox::memory::MemoryPool* pool_;
  uint64_t expected_length_;
  uint64_t expected_snapshot_id_;
  std::string expected_sha256_;
  std::string tag_name_;
  std::optional<uint64_t> expected_schema_id_;
  bool schema_history_;
  std::vector<std::pair<std::string, std::string>> options_;
  size_t option_cursor_ = 0;
  bool options_loaded_ = false;
  bool emitted_ = false;
};

}  // namespace pangrova::velox
