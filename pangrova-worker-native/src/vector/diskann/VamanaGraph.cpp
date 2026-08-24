// VamanaGraph: Vamana graph build and beam search implementation.
//
// Build: single-pass greedy insertion with alpha-pruning (R-robust pruning).
// Search: beam search over PQ-compressed navigation vectors, then full-precision
// rerank of top candidates from SSD.

#include "vector/diskann/VamanaGraph.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <numeric>
#include <queue>
#include <random>
#include <unordered_set>

#include "vector/VectorDistance.h"

namespace pangrova::vector::diskann {

namespace {

struct DistEntry {
  int64_t nodeId;
  float distance;
  bool operator>(const DistEntry& o) const { return distance > o.distance; }
};

}  // namespace

VamanaIndex::VamanaIndex(VamanaConfig config, std::string indexPath)
    : config_(config), indexPath_(std::move(indexPath)) {}

VamanaIndex::~VamanaIndex() = default;

void VamanaIndex::build(
    const float* vectors, int64_t numVectors, int64_t dimension) {
  config_.dimension = dimension;
  config_.numVectors = numVectors;
  numVectors_ = numVectors;

  if (config_.pqSubvectors <= 0) {
    config_.pqSubvectors = std::max(int64_t{1}, dimension / 8);
  }

  adjList_.resize(static_cast<size_t>(numVectors));

  selectMedoid(vectors);
  buildPQCodebooks(vectors);
  buildVamanaGraph(vectors);
}

void VamanaIndex::selectMedoid(const float* vectors) {
  std::vector<double> centroid(static_cast<size_t>(config_.dimension), 0.0);
  for (int64_t i = 0; i < numVectors_; ++i) {
    const float* v = vectors + i * config_.dimension;
    for (int64_t d = 0; d < config_.dimension; ++d) {
      centroid[static_cast<size_t>(d)] += static_cast<double>(v[d]);
    }
  }
  double inv = 1.0 / static_cast<double>(numVectors_);
  std::vector<float> centroidF(static_cast<size_t>(config_.dimension));
  for (int64_t d = 0; d < config_.dimension; ++d) {
    centroidF[static_cast<size_t>(d)] =
        static_cast<float>(centroid[static_cast<size_t>(d)] * inv);
  }

  float bestDist = std::numeric_limits<float>::max();
  medoid_ = 0;
  for (int64_t i = 0; i < numVectors_; ++i) {
    const float* v = vectors + i * config_.dimension;
    float dist = static_cast<float>(
        l2Distance(v, centroidF.data(), static_cast<size_t>(config_.dimension)));
    if (dist < bestDist) {
      bestDist = dist;
      medoid_ = i;
    }
  }
}

void VamanaIndex::buildVamanaGraph(const float* vectors) {
  std::vector<int64_t> insertOrder(static_cast<size_t>(numVectors_));
  std::iota(insertOrder.begin(), insertOrder.end(), 0);
  std::mt19937 rng(42);
  std::shuffle(insertOrder.begin(), insertOrder.end(), rng);

  for (int64_t idx = 0; idx < numVectors_; ++idx) {
    int64_t p = insertOrder[static_cast<size_t>(idx)];
    const float* queryVec = vectors + p * config_.dimension;

    // Greedy search from medoid to find nearest neighbors of p.
    std::priority_queue<DistEntry, std::vector<DistEntry>, std::greater<>> minHeap;
    std::unordered_set<int64_t> visited;
    float medDist = static_cast<float>(l2Distance(
        queryVec, vectors + medoid_ * config_.dimension,
        static_cast<size_t>(config_.dimension)));
    minHeap.push({medoid_, medDist});
    visited.insert(medoid_);

    std::vector<NeighborEntry> candidates;
    int64_t searchBudget = config_.searchListSize;

    while (!minHeap.empty() && searchBudget > 0) {
      auto top = minHeap.top();
      minHeap.pop();
      searchBudget--;

      candidates.push_back({top.nodeId, top.distance});

      auto& neighbors = adjList_[static_cast<size_t>(top.nodeId)];
      for (auto& nb : neighbors) {
        if (visited.count(nb.nodeId)) continue;
        visited.insert(nb.nodeId);
        const float* nbVec = vectors + nb.nodeId * config_.dimension;
        float d = static_cast<float>(l2Distance(
            queryVec, nbVec, static_cast<size_t>(config_.dimension)));
        minHeap.push({nb.nodeId, d});
      }
    }

    robustPrune(p, candidates, config_.alpha, config_.maxDegree, vectors);

    // Add reverse edges and prune if over max degree.
    auto& pNeighbors = adjList_[static_cast<size_t>(p)];
    for (auto& nb : pNeighbors) {
      auto& reverseList = adjList_[static_cast<size_t>(nb.nodeId)];
      bool exists = false;
      for (auto& rn : reverseList) {
        if (rn.nodeId == p) { exists = true; break; }
      }
      if (!exists) {
        reverseList.push_back({p, nb.distance});
        if (static_cast<int64_t>(reverseList.size()) > config_.maxDegree) {
          robustPrune(
              nb.nodeId, reverseList, config_.alpha, config_.maxDegree, vectors);
        }
      }
    }
  }
}

void VamanaIndex::robustPrune(
    int64_t nodeId, std::vector<NeighborEntry>& candidates,
    double alpha, int64_t maxDegree, const float* vectors) {

  // Remove self-loops.
  candidates.erase(
      std::remove_if(candidates.begin(), candidates.end(),
                     [nodeId](const NeighborEntry& e) {
                       return e.nodeId == nodeId;
                     }),
      candidates.end());

  // Sort by distance ascending.
  std::sort(candidates.begin(), candidates.end(),
            [](const NeighborEntry& a, const NeighborEntry& b) {
              return a.distance < b.distance;
            });

  std::vector<NeighborEntry> pruned;
  for (auto& c : candidates) {
    if (static_cast<int64_t>(pruned.size()) >= maxDegree) break;

    bool dominated = false;
    for (auto& p : pruned) {
      float pqDist = static_cast<float>(l2Distance(
          vectors + c.nodeId * config_.dimension,
          vectors + p.nodeId * config_.dimension,
          static_cast<size_t>(config_.dimension)));
      if (static_cast<double>(pqDist) * alpha < static_cast<double>(c.distance)) {
        dominated = true;
        break;
      }
    }
    if (!dominated) {
      pruned.push_back(c);
    }
  }

  adjList_[static_cast<size_t>(nodeId)] = std::move(pruned);
}

void VamanaIndex::buildPQCodebooks(const float* vectors) {
  int64_t subDim = config_.dimension / config_.pqSubvectors;
  if (subDim <= 0) subDim = 1;
  int64_t numCentroids = int64_t{1} << config_.pqBitsPerCode;
  if (numCentroids > 256) numCentroids = 256;

  codebooks_.resize(static_cast<size_t>(config_.pqSubvectors));
  pqCodeSize_ = config_.pqSubvectors;
  pqCodes_.resize(
      static_cast<size_t>(numVectors_) * static_cast<size_t>(pqCodeSize_));

  std::mt19937 rng(123);
  int64_t sampleSize = std::min(numVectors_, int64_t{10000});

  // Pre-initialize centroids for all subvectors (must be serial due to RNG).
  for (int64_t s = 0; s < config_.pqSubvectors; ++s) {
    auto& cb = codebooks_[static_cast<size_t>(s)];
    cb.subvectorDim = subDim;
    cb.numCentroids = numCentroids;
    cb.centroids.resize(
        static_cast<size_t>(numCentroids) * static_cast<size_t>(subDim));

    int64_t offset = s * subDim;
    for (int64_t c = 0; c < numCentroids; ++c) {
      int64_t vecIdx = rng() % static_cast<uint64_t>(sampleSize);
      const float* src = vectors + vecIdx * config_.dimension + offset;
      std::memcpy(
          cb.centroids.data() + c * subDim, src,
          static_cast<size_t>(subDim) * sizeof(float));
    }
  }

  // Parallel PQ encoding: each subvector's assignment is independent.
  // This is the most expensive part of PQ training (N * K distance computations
  // per subvector). Parallelizing across subvectors gives ~Sx speedup where
  // S = min(pqSubvectors, hardware_concurrency).

  auto encodeSubvector = [&](int64_t s) {
    auto& cb = codebooks_[static_cast<size_t>(s)];
    int64_t offset = s * subDim;
    for (int64_t i = 0; i < numVectors_; ++i) {
      const float* subVec = vectors + i * config_.dimension + offset;
      float bestDist = std::numeric_limits<float>::max();
      uint8_t bestCode = 0;
      for (int64_t c = 0; c < numCentroids; ++c) {
        const float* cent = cb.centroids.data() + c * subDim;
        float dist = static_cast<float>(
            l2Distance(subVec, cent, static_cast<size_t>(subDim)));
        if (dist < bestDist) {
          bestDist = dist;
          bestCode = static_cast<uint8_t>(c);
        }
      }
      pqCodes_[static_cast<size_t>(i) * static_cast<size_t>(pqCodeSize_) +
               static_cast<size_t>(s)] = bestCode;
    }
  };

  if (config_.pqSubvectors <= 2 || numVectors_ < 1000) {
    // Small problem: serial execution
    for (int64_t s = 0; s < config_.pqSubvectors; ++s) {
      encodeSubvector(s);
    }
  } else {
    // Parallel execution: launch async tasks per subvector
    std::vector<std::future<void>> futures;
    futures.reserve(static_cast<size_t>(config_.pqSubvectors));
    for (int64_t s = 0; s < config_.pqSubvectors; ++s) {
      futures.push_back(std::async(std::launch::async, encodeSubvector, s));
    }
    for (auto& f : futures) {
      f.get();
    }
  }
}

float VamanaIndex::computePQDistance(
    const uint8_t* pqCode, const float* query) const {
  // Accumulate SQUARED per-subvector L2 distances; the total squared L2 is the
  // sum of the subspace squared distances. Summing the non-squared per-subvector
  // distances (as before) is not order-equivalent to true L2 and degrades the
  // beam-search navigation quality. l2Distance() returns Euclidean distance, so
  // square it back before summing, then take the root for a proper L2 estimate.
  double sumSq = 0.0;
  for (int64_t s = 0; s < config_.pqSubvectors; ++s) {
    auto& cb = codebooks_[static_cast<size_t>(s)];
    uint8_t code = pqCode[s];
    const float* centroid =
        cb.centroids.data() + static_cast<size_t>(code) * static_cast<size_t>(cb.subvectorDim);
    int64_t offset = s * cb.subvectorDim;
    const float* querySlice = query + offset;
    double d =
        l2Distance(querySlice, centroid, static_cast<size_t>(cb.subvectorDim));
    sumSq += d * d;
  }
  return static_cast<float>(std::sqrt(sumSq));
}

std::vector<SearchResult> VamanaIndex::search(
    const float* query, int64_t topK, int64_t beamWidth) const {
  if (numVectors_ <= 0 || topK <= 0) return {};

  if (beamWidth <= 0) beamWidth = config_.beamWidth;

  // Beam search using PQ distances for navigation.
  std::priority_queue<DistEntry, std::vector<DistEntry>, std::greater<>> frontier;
  std::unordered_set<int64_t> visited;

  const uint8_t* medPQ =
      pqCodes_.data() +
      static_cast<size_t>(medoid_) * static_cast<size_t>(pqCodeSize_);
  float medDist = computePQDistance(medPQ, query);
  frontier.push({medoid_, medDist});
  visited.insert(medoid_);

  std::vector<DistEntry> resultCandidates;
  int64_t budget = config_.searchListSize;

  while (!frontier.empty() && budget > 0) {
    auto top = frontier.top();
    frontier.pop();
    budget--;

    resultCandidates.push_back(top);

    auto& neighbors = adjList_[static_cast<size_t>(top.nodeId)];
    for (auto& nb : neighbors) {
      if (visited.count(nb.nodeId)) continue;
      visited.insert(nb.nodeId);

      const uint8_t* nbPQ =
          pqCodes_.data() +
          static_cast<size_t>(nb.nodeId) * static_cast<size_t>(pqCodeSize_);
      float d = computePQDistance(nbPQ, query);
      frontier.push({nb.nodeId, d});
    }
  }

  // Sort candidates by PQ distance, take topK * beamWidth for rerank.
  std::sort(resultCandidates.begin(), resultCandidates.end(),
            [](const DistEntry& a, const DistEntry& b) {
              return a.distance < b.distance;
            });

  int64_t rerankCount = std::min(
      static_cast<int64_t>(resultCandidates.size()), topK * beamWidth);

  std::vector<SearchResult> results;
  results.reserve(static_cast<size_t>(std::min(topK, rerankCount)));

  // Return PQ-distance results (full-precision rerank requires SSD I/O,
  // which is handled by DiskAnnSearcher with async I/O).
  for (int64_t i = 0; i < std::min(topK, rerankCount); ++i) {
    results.push_back(
        {resultCandidates[static_cast<size_t>(i)].nodeId,
         resultCandidates[static_cast<size_t>(i)].distance});
  }

  return results;
}

bool VamanaIndex::saveToDisk(const std::string& path) const {
  std::ofstream out(path, std::ios::binary);
  if (!out) return false;

  // Header: magic, version, config.
  uint64_t magic = 0x56414D414E410001ULL;
  out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  out.write(reinterpret_cast<const char*>(&numVectors_), sizeof(numVectors_));
  out.write(
      reinterpret_cast<const char*>(&config_.dimension),
      sizeof(config_.dimension));
  out.write(reinterpret_cast<const char*>(&medoid_), sizeof(medoid_));
  out.write(
      reinterpret_cast<const char*>(&config_.maxDegree),
      sizeof(config_.maxDegree));
  out.write(
      reinterpret_cast<const char*>(&config_.pqSubvectors),
      sizeof(config_.pqSubvectors));
  out.write(
      reinterpret_cast<const char*>(&config_.pqBitsPerCode),
      sizeof(config_.pqBitsPerCode));

  // Adjacency lists.
  for (int64_t i = 0; i < numVectors_; ++i) {
    auto& neighbors = adjList_[static_cast<size_t>(i)];
    int32_t degree = static_cast<int32_t>(neighbors.size());
    out.write(reinterpret_cast<const char*>(&degree), sizeof(degree));
    for (auto& nb : neighbors) {
      out.write(reinterpret_cast<const char*>(&nb.nodeId), sizeof(nb.nodeId));
      out.write(
          reinterpret_cast<const char*>(&nb.distance), sizeof(nb.distance));
    }
  }

  // PQ codebooks.
  for (auto& cb : codebooks_) {
    out.write(
        reinterpret_cast<const char*>(cb.centroids.data()),
        static_cast<std::streamsize>(
            cb.centroids.size() * sizeof(float)));
  }

  // PQ codes.
  out.write(
      reinterpret_cast<const char*>(pqCodes_.data()),
      static_cast<std::streamsize>(pqCodes_.size()));

  return out.good();
}

bool VamanaIndex::loadFromDisk(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;

  uint64_t magic = 0;
  in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  if (magic != 0x56414D414E410001ULL) return false;

  in.read(reinterpret_cast<char*>(&numVectors_), sizeof(numVectors_));
  in.read(
      reinterpret_cast<char*>(&config_.dimension), sizeof(config_.dimension));
  in.read(reinterpret_cast<char*>(&medoid_), sizeof(medoid_));
  in.read(
      reinterpret_cast<char*>(&config_.maxDegree), sizeof(config_.maxDegree));
  in.read(
      reinterpret_cast<char*>(&config_.pqSubvectors),
      sizeof(config_.pqSubvectors));
  in.read(
      reinterpret_cast<char*>(&config_.pqBitsPerCode),
      sizeof(config_.pqBitsPerCode));

  config_.numVectors = numVectors_;

  adjList_.resize(static_cast<size_t>(numVectors_));
  for (int64_t i = 0; i < numVectors_; ++i) {
    int32_t degree = 0;
    in.read(reinterpret_cast<char*>(&degree), sizeof(degree));
    auto& neighbors = adjList_[static_cast<size_t>(i)];
    neighbors.resize(static_cast<size_t>(degree));
    for (int32_t j = 0; j < degree; ++j) {
      in.read(
          reinterpret_cast<char*>(&neighbors[static_cast<size_t>(j)].nodeId),
          sizeof(int64_t));
      in.read(
          reinterpret_cast<char*>(&neighbors[static_cast<size_t>(j)].distance),
          sizeof(float));
    }
  }

  int64_t numCentroids = int64_t{1} << config_.pqBitsPerCode;
  if (numCentroids > 256) numCentroids = 256;
  int64_t subDim = config_.dimension / config_.pqSubvectors;
  if (subDim <= 0) subDim = 1;

  codebooks_.resize(static_cast<size_t>(config_.pqSubvectors));
  for (auto& cb : codebooks_) {
    cb.subvectorDim = subDim;
    cb.numCentroids = numCentroids;
    cb.centroids.resize(
        static_cast<size_t>(numCentroids) * static_cast<size_t>(subDim));
    in.read(
        reinterpret_cast<char*>(cb.centroids.data()),
        static_cast<std::streamsize>(
            cb.centroids.size() * sizeof(float)));
  }

  pqCodeSize_ = config_.pqSubvectors;
  pqCodes_.resize(
      static_cast<size_t>(numVectors_) * static_cast<size_t>(pqCodeSize_));
  in.read(
      reinterpret_cast<char*>(pqCodes_.data()),
      static_cast<std::streamsize>(pqCodes_.size()));

  return in.good();
}

bool VamanaIndex::readVector(
    int64_t nodeId, float* buffer, int64_t dimension) const {
  if (nodeId < 0 || nodeId >= numVectors_ || dimension != config_.dimension) {
    return false;
  }
  // Read from companion .vecs file (flat float array stored during build).
  std::string vecsPath = indexPath_ + ".vecs";
  std::ifstream in(vecsPath, std::ios::binary);
  if (!in) return false;
  auto offset = static_cast<std::streamoff>(
      static_cast<size_t>(nodeId) * static_cast<size_t>(dimension) *
      sizeof(float));
  in.seekg(offset);
  if (!in) return false;
  in.read(reinterpret_cast<char*>(buffer),
          static_cast<std::streamsize>(dimension * sizeof(float)));
  return in.good();
}

}  // namespace pangrova::vector::diskann
