// DiskAnnSearcher: Full-precision reranking from SSD after PQ beam search.

#include "vector/diskann/DiskAnnSearcher.h"

#include <algorithm>
#include <stdexcept>

#include "vector/VectorDistance.h"

namespace pangrova::vector::diskann {

DiskAnnSearcher::DiskAnnSearcher(
    const VamanaIndex& index, VectorReader reader)
    : index_(index), reader_(std::move(reader)) {}

std::vector<DiskAnnResult> DiskAnnSearcher::search(
    const float* query, const DiskAnnSearchConfig& config) const {
  int64_t pqCandidates = config.topK * config.rerankMultiplier;
  auto pqResults = index_.search(
      query, pqCandidates, config.beamWidth, config.searchListSize);

  // L4-11D: Pre-filter bitset check helper.
  // When a pre-filter bitset is provided, only nodes whose bit is set
  // pass to the reranking phase. This avoids expensive SSD reads for
  // rows that will be filtered out by WHERE clauses.
  auto isNodeAllowed = [&](int64_t nodeId) -> bool {
    if (!config.preFilterBitset || config.preFilterBitsetSize == 0) return true;
    size_t byteIdx = static_cast<size_t>(nodeId) / 8;
    uint8_t bitIdx = static_cast<uint8_t>(nodeId % 8);
    if (byteIdx >= config.preFilterBitsetSize) return false;
    return (config.preFilterBitset[byteIdx] & (1u << bitIdx)) != 0;
  };

  std::vector<DiskAnnResult> results;
  results.reserve(pqResults.size());

  int64_t dim = index_.dimension();
  std::vector<float> vectorBuf(static_cast<size_t>(dim));

  for (auto& pr : pqResults) {
    // L4-11D: Skip nodes not in pre-filter bitset.
    if (!isNodeAllowed(pr.nodeId)) {
      continue;
    }

    if (reader_(pr.nodeId, vectorBuf.data(), dim)) {
      float exactDist = static_cast<float>(
          l2Distance(query, vectorBuf.data(), static_cast<size_t>(dim)));
      results.push_back({pr.nodeId, exactDist});
    } else {
      throw std::runtime_error(
          "DiskANN full-precision rescore could not read a candidate vector");
    }
  }

  std::sort(results.begin(), results.end(),
            [](const DiskAnnResult& a, const DiskAnnResult& b) {
              return a.distance < b.distance;
            });

  if (static_cast<int64_t>(results.size()) > config.topK) {
    results.resize(static_cast<size_t>(config.topK));
  }

  return results;
}

}  // namespace pangrova::vector::diskann
