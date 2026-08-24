// RowDeleteFilterFactory: Creates delete filters from split config.
// Returns filters that operate directly on RowVector without Arrow conversion.
//
// Filter chain ordering: DV → Position → Equality.
// DV filters are applied first because bitmap lookup is O(1) per row and
// can eliminate large row ranges before more expensive position/equality checks.
//
// Placement: delete/ — delete filter abstractions.
#pragma once

#include <memory>
#include <vector>

#include "split/SplitConfig.h"
#include "delete/RowDeleteFilter.h"

namespace pangrova::delete_filter {

class RowDeleteFilterFactory {
 public:
  // Creates delete filters from IcebergConfig.
  // Returns an empty vector if no delete files are present.
  // The returned filter chain is ordered: DV first, then position, then equality.
  // This ordering maximizes pruning efficiency — DV bitmap O(1) lookup
  // eliminates rows before position O(log n) and equality O(1) hash checks.
  //
  // @param iceberg Iceberg split configuration with delete file metadata.
  // @param dataFilePath Path of the data file being scanned. Used to filter
  //        position deletes by target file (position delete files contain
  //        (file_path, pos) tuples for multiple data files).
  static std::vector<std::unique_ptr<RowDeleteFilter>> create(
      const split::IcebergConfig& iceberg,
      const std::string& dataFilePath = "");
};

}  // namespace pangrova::delete_filter
