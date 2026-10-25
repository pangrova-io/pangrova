// RowDeleteFilterFactory: Creates delete filters from Iceberg split config.

#include "delete/RowDeleteFilterFactory.h"

#include <stdexcept>

#include "delete/DeletionVectorRowFilter.h"
#include "delete/io/DeleteFileReader.h"
#include "delete/io/DeletionVectorReader.h"

namespace pangrova::delete_filter {

std::vector<std::unique_ptr<RowDeleteFilter>> RowDeleteFilterFactory::create(
    const split::IcebergConfig& iceberg,
    const std::string& dataFilePath) {
  for (const auto& df : iceberg.delete_files) {
    if (df.delete_type == "equality") {
      throw std::runtime_error(
          "Iceberg equality deletes are not supported until cross-engine read qualification passes");
    }
    if (df.delete_type != "position" && df.delete_type != "deletion_vector") {
      throw std::runtime_error("Unsupported Iceberg delete type: " + df.delete_type);
    }
  }
  std::vector<std::unique_ptr<RowDeleteFilter>> filters;

  // Phase 1: Deletion vector filters (applied first — O(1) bitmap lookup).
  for (const auto& df : iceberg.delete_files) {
    if (df.delete_type == "deletion_vector") {
      auto bitmap = io::DeletionVectorReader::readIcebergDV(df.dv_file_path, dataFilePath);
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

  return filters;
}

}  // namespace pangrova::delete_filter
