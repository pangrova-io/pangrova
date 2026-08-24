// MergeReaderFactory: Factory for creating the appropriate MergeReader
// based on PaimonConfig merge engine type.
//
// Placement: merge/ — merge reader factory.
// Validates: Requirements R8.1, R10.4, R22.2
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "split/SplitConfig.h"
#include "merge/AggregationMergeReader.h"
#include "merge/MergeReader.h"
#include "velox/type/Type.h"

namespace pangrova::merge {

// Factory that creates the appropriate MergeReader implementation
// based on PaimonConfig settings. All methods are stateless static
// functions — safe to call from any thread.
class MergeReaderFactory {
 public:
  MergeReaderFactory() = delete;

  // Create a MergeReader for the given Paimon config.
  // Returns nullptr when merge is not needed.
  static std::unique_ptr<MergeReader> create(
      const split::PaimonConfig& config,
      const facebook::velox::RowTypePtr& output_type);

 private:
  static std::vector<facebook::velox::column_index_t> resolveKeyIndices(
      const std::vector<std::string>& primary_keys,
      const facebook::velox::RowTypePtr& output_type);

  static facebook::velox::column_index_t resolveColumnIndex(
      const std::string& column_name,
      const facebook::velox::RowTypePtr& output_type);

  static std::unordered_map<facebook::velox::column_index_t, AggFunction>
  resolveAggSpecs(
      const std::unordered_map<std::string, std::string>& agg_specs,
      const facebook::velox::RowTypePtr& output_type);
};

}  // namespace pangrova::merge
