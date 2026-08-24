// RowDeleteFilterFactory: Creates delete filters from Iceberg split config.

#include "delete/RowDeleteFilterFactory.h"

#include "delete/DeletionVectorRowFilter.h"
#include "delete/io/DeleteFileReader.h"
#include "delete/io/DeletionVectorReader.h"

namespace pangrova::delete_filter {

std::vector<std::unique_ptr<RowDeleteFilter>> RowDeleteFilterFactory::create(
    const split::IcebergConfig& iceberg,
    const std::string& dataFilePath) {
  std::vector<std::unique_ptr<RowDeleteFilter>> filters;

  // Phase 1: Deletion vector filters (applied first — O(1) bitmap lookup).
  for (const auto& df : iceberg.delete_files) {
    if (df.delete_type == "deletion_vector") {
      auto bitmap = io::DeletionVectorReader::readIcebergDV(df.dv_file_path);
      filters.push_back(
          std::make_unique<DeletionVectorRowFilter>(std::move(bitmap)));
    }
  }

  // Phase 2: Position deletes (applied second — O(log n) sorted lookup).
  std::vector<int64_t> positions;
  for (const auto& df : iceberg.delete_files) {
    if (df.delete_type == "position") {
      auto loaded = DeleteFileReader::readPositions(
            df.file_path, dataFilePath);
        positions.insert(positions.end(), loaded.begin(), loaded.end());
    }
  }
  if (!positions.empty()) {
    filters.push_back(
        std::make_unique<PositionDeleteFilter>(std::move(positions)));
  }

  // Phase 3: Equality deletes (applied last — hash set lookup).
  for (const auto& df : iceberg.delete_files) {
    if (df.delete_type == "equality") {
      auto deleted_keys = DeleteFileReader::readEqualityKeys(
            df.file_path, df.equality_fields);
        std::unordered_set<std::string> key_set(
            deleted_keys.begin(), deleted_keys.end());
        filters.push_back(
            std::make_unique<EqualityDeleteFilter>(
                df.equality_fields, std::move(key_set)));
    }
  }

  return filters;
}

}  // namespace pangrova::delete_filter
