#include "vector/diskann/VamanaGraph.h"

#include <cmath>
#include <queue>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace pangrova::vector::diskann {
namespace {

struct FrontierEntry {
  int64_t node;
  float distance;
  bool operator>(const FrontierEntry& other) const {
    return distance == other.distance ? node > other.node
                                      : distance > other.distance;
  }
};

}  // namespace

GraphTraversalStats VamanaIndex::traverse(
    const float* query,
    uint64_t max_expanded,
    pangrova::resource::OwnerMemoryResource& resource,
    const std::function<void()>& checkpoint,
    const std::function<void(int64_t, float)>& visitor) const {
  if (!query || !checkpoint || !visitor || max_expanded == 0 ||
      numVectors_ <= 0 || medoid_ < 0 || medoid_ >= numVectors_) {
    throw std::invalid_argument("Vamana traversal requires valid query and bounded work");
  }
  for (int64_t channel = 0; channel < dimension(); ++channel) {
    if (!std::isfinite(query[channel])) {
      throw std::invalid_argument("Vamana query contains a non-finite value");
    }
  }
  std::priority_queue<
      FrontierEntry, pangrova::resource::OwnerVector<FrontierEntry>, std::greater<>> frontier(
          std::greater<>{}, pangrova::resource::OwnerVector<FrontierEntry>(&resource));
  pangrova::resource::OwnerUnorderedSet<int64_t> visited(&resource);
  auto enqueue = [&](int64_t node) {
    if (node < 0 || node >= numVectors_) {
      throw std::runtime_error("Vamana neighbor is outside the immutable graph");
    }
    if (!visited.emplace(node).second) {
      return;
    }
    const auto* code = pqCodes_.data() +
        static_cast<size_t>(node) * static_cast<size_t>(pqCodeSize_);
    const float distance = computePQDistance(code, query);
    if (!std::isfinite(distance)) {
      throw std::runtime_error("Vamana navigation distance is not finite");
    }
    frontier.push({node, distance});
  };
  checkpoint();
  enqueue(medoid_);
  GraphTraversalStats stats;
  while (!frontier.empty() && stats.expanded < max_expanded) {
    checkpoint();
    const auto candidate = frontier.top();
    frontier.pop();
    ++stats.expanded;
    visitor(candidate.node, candidate.distance);
    for (const auto& neighbor : adjList_.at(static_cast<size_t>(candidate.node))) {
      checkpoint();
      enqueue(neighbor.nodeId);
    }
  }
  checkpoint();
  stats.frontier_exhausted = frontier.empty();
  return stats;
}

}  // namespace pangrova::vector::diskann
