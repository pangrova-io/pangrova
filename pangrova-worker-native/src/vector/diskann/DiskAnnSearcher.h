// DiskAnnSearcher: High-level search interface for DiskANN indexes.
//
// Wraps VamanaIndex with full-precision reranking from SSD.
// Beam search uses PQ-compressed vectors in memory, then reads
// full-precision vectors from SSD for the top candidates.
//
// Thread-safe: search is stateless after construction.
// Placement: vector/diskann/ — Native disk-resident vector index.
#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "vector/diskann/VamanaGraph.h"

namespace pangrova::vector::diskann {

struct DiskAnnSearchConfig {
  int64_t topK = 10;
  int64_t beamWidth = 4;
  int64_t rerankMultiplier = 10;
  int64_t searchListSize = 100;

  // L4-11D: Pre-filter bitset (raw bit-packed bytes).
  // When non-empty, only nodes whose bit is set are considered for reranking.
  // The PQ candidate generation still traverses the full graph, but only
  // nodes passing the filter are read from SSD for exact distance computation.
  const uint8_t* preFilterBitset = nullptr;
  size_t preFilterBitsetSize = 0;
};

struct DiskAnnResult {
  int64_t nodeId;
  float distance;
};

using VectorReader =
    std::function<bool(int64_t nodeId, float* buffer, int64_t dimension)>;

class DiskAnnSearcher {
 public:
  DiskAnnSearcher(const VamanaIndex& index, VectorReader reader);

  std::vector<DiskAnnResult> search(
      const float* query, const DiskAnnSearchConfig& config) const;

 private:
  const VamanaIndex& index_;
  VectorReader reader_;
};

}  // namespace pangrova::vector::diskann
