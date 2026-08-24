// LanceVectorIndex: ANN index operations over Lance datasets via C FFI.
//
// Wraps the Lance C FFI to provide vector column reading from Lance datasets,
// supporting the native scan path for brute-force vector search over Lance
// files. Index creation and ANN search use the Java LanceFfiBridge (JNI)
// since the Lance Rust library exposes those operations at a higher level
// than the current C FFI.

#ifndef PANGROVA_LANCE_VECTOR_INDEX_H
#define PANGROVA_LANCE_VECTOR_INDEX_H

#include "vector/VectorScanOperator.h"

#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::lance {

/// Pre-filter strategy for ANN search (mirrors proto PreFilterStrategy).
enum class PreFilterStrategy {
  NONE = 0,             // No pre-filter; search all rows.
  BEFORE_SEARCH = 1,    // Apply bitset before graph traversal.
  AFTER_SEARCH = 2,     // Apply bitset after candidate generation.
  ADAPTIVE = 3,         // Engine decides based on selectivity.
};

struct LanceVectorSearchConfig {
  int topK = 10;
  std::string columnName;
  std::string filter;
  pangrova::vector::DistanceMetric metric =
      pangrova::vector::DistanceMetric::L2;

  // L4-11D: Pre-filter bitset (roaring bitmap serialized bytes).
  // When non-empty, only rows set in the bitset are searched.
  std::vector<uint8_t> preFilterBitset;
  PreFilterStrategy preFilterStrategy = PreFilterStrategy::NONE;

  // L4-11B: Shard IDs assigned to this search.
  std::vector<std::string> assignedShardIds;
};

struct LanceSearchResult {
  int64_t rowId;
  float distance;
};

class LanceVectorIndex {
 public:
  LanceVectorIndex() = default;

  std::vector<LanceSearchResult> bruteForceSearch(
      const std::string& datasetUri,
      const float* queryVector,
      int64_t dimension,
      const LanceVectorSearchConfig& config) const;

  int64_t countVectors(
      const std::string& datasetUri,
      const std::string& columnName) const;

 private:
  LanceVectorIndex(const LanceVectorIndex&) = delete;
  LanceVectorIndex& operator=(const LanceVectorIndex&) = delete;
};

}  // namespace pangrova::lance

#endif  // PANGROVA_LANCE_VECTOR_INDEX_H
