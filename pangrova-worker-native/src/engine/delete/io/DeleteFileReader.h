#pragma once

#include <roaring.hh>
#include <string>
#include <unordered_set>
#include <vector>

// Velox DWIO reader for Parquet delete files.
#if __has_include("velox/dwio/common/ReaderFactory.h") && \
    __has_include("velox/common/file/File.h")
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#define PANGROVA_HAS_VELOX_DELETE_READER 1
#else
#define PANGROVA_HAS_VELOX_DELETE_READER 0
#endif

namespace pangrova::velox {
class MetadataReadContext;
}

namespace pangrova::delete_filter {

// Reads Iceberg v2/v3 delete files (Parquet format) to extract
// position, equality key, or deletion vector data for delete filtering.
//
// Primary implementation uses Velox native DWIO readers (ParquetReader)
// for zero-conversion integration with the Velox execution pipeline.
// Falls back to Arrow parquet::arrow::FileReader when Velox DWIO is
// not available at compile time.
class DeleteFileReader {
 public:
  // Read position delete file, return positions for the given data file.
  // Position delete files have schema: (file_path: STRING, pos: BIGINT).
  // Filters rows where file_path matches dataFilePath.
  static std::vector<int64_t> readPositions(
      const std::string& deleteFilePath,
      const std::string& dataFilePath,
      const pangrova::velox::MetadataReadContext* reads = nullptr);

  // Read equality delete file, return serialized key strings.
  // Equality delete files contain columns matching the equality field IDs.
  // Each row is serialized as "col1|col2|..." for set-based lookup.
  static std::unordered_set<std::string> readEqualityKeys(
      const std::string& deleteFilePath,
      const std::vector<std::string>& equalityColumns);

  // Read Iceberg V3 deletion vector (Puffin file Roaring Bitmap).
  // Returns a Roaring bitmap of deleted row positions.
  static roaring::Roaring readDeletionVector(
      const std::string& dvFilePath);
};

}  // namespace pangrova::delete_filter
