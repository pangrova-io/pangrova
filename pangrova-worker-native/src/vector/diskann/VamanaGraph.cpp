// VamanaGraph: Vamana graph build and beam search implementation.
//
// Build: single-pass greedy insertion with alpha-pruning (R-robust pruning).
// Search: beam search over PQ-compressed navigation vectors, then full-precision
// rerank of top candidates from SSD.

#include "vector/diskann/VamanaGraph.h"

#include <algorithm>
#include <atomic>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <numeric>
#include <queue>
#include <random>
#include <unordered_set>
#include <thread>

#include "vector/VectorDistance.h"

namespace pangrova::vector::diskann {

namespace {

constexpr uint64_t kVamanaFormatMagic = 0x56414D414E410002ULL;

struct DistEntry {
  int64_t nodeId;
  float distance;
  bool operator>(const DistEntry& o) const { return distance > o.distance; }
};

void writeUint32(std::ostream& output, uint32_t value) {
  std::array<char, 4> bytes{};
  for (size_t index = 0; index < bytes.size(); ++index) {
    bytes[index] = static_cast<char>((value >> (index * 8)) & 0xffU);
  }
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

void writeUint64(std::ostream& output, uint64_t value) {
  std::array<char, 8> bytes{};
  for (size_t index = 0; index < bytes.size(); ++index) {
    bytes[index] = static_cast<char>((value >> (index * 8)) & 0xffU);
  }
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

void writeFloat32(std::ostream& output, float value) {
  writeUint32(output, std::bit_cast<uint32_t>(value));
}

bool readUint32(std::istream& input, uint32_t& value) {
  std::array<unsigned char, 4> bytes{};
  input.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
  if (!input.good()) {
    return false;
  }
  value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint32_t>(bytes[index]) << (index * 8);
  }
  return true;
}

bool readUint64(std::istream& input, uint64_t& value) {
  std::array<unsigned char, 8> bytes{};
  input.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
  if (!input.good()) {
    return false;
  }
  value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint64_t>(bytes[index]) << (index * 8);
  }
  return true;
}

bool readFloat32(std::istream& input, float& value) {
  uint32_t bits = 0;
  if (!readUint32(input, bits)) {
    return false;
  }
  value = std::bit_cast<float>(bits);
  return std::isfinite(value);
}

bool checkedAdd(uint64_t& total, uint64_t value, uint64_t limit) {
  if (value > limit || total > limit - value) {
    return false;
  }
  total += value;
  return true;
}

uint64_t saturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
      ? std::numeric_limits<uint64_t>::max()
      : left + right;
}

uint64_t saturatingMultiply(uint64_t left, uint64_t right) {
  return left != 0 && right > std::numeric_limits<uint64_t>::max() / left
      ? std::numeric_limits<uint64_t>::max()
      : left * right;
}

}  // namespace

VamanaIndex::VamanaIndex(VamanaConfig config, std::string indexPath)
    : config_(config), indexPath_(std::move(indexPath)) {}

VamanaIndex::~VamanaIndex() = default;

uint64_t VamanaIndex::memoryBytes() const {
  uint64_t bytes = saturatingAdd(sizeof(VamanaIndex), indexPath_.capacity());
  bytes = saturatingAdd(bytes, saturatingMultiply(
      adjList_.capacity(), sizeof(std::vector<NeighborEntry>)));
  for (const auto& neighbors : adjList_) {
    bytes = saturatingAdd(bytes, saturatingMultiply(
        neighbors.capacity(), sizeof(NeighborEntry)));
  }
  bytes = saturatingAdd(bytes, saturatingMultiply(
      codebooks_.capacity(), sizeof(PQCodebook)));
  for (const auto& codebook : codebooks_) {
    bytes = saturatingAdd(bytes, saturatingMultiply(
        codebook.centroids.capacity(), sizeof(float)));
  }
  bytes = saturatingAdd(bytes, pqCodes_.capacity() * sizeof(uint8_t));
  return bytes;
}

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
    // Parallel execution uses a bounded worker set. Launching one async task
    // per subvector lets an untrusted dimension create thousands of threads.
    const auto hardware = std::max(1U, std::thread::hardware_concurrency());
    const int64_t worker_count = std::min<int64_t>(
        config_.pqSubvectors, std::min<uint32_t>(hardware, 32));
    std::atomic<int64_t> next_subvector{0};
    std::vector<std::future<void>> futures;
    futures.reserve(static_cast<size_t>(worker_count));
    for (int64_t worker = 0; worker < worker_count; ++worker) {
      futures.push_back(std::async(std::launch::async, [&] {
        for (;;) {
          const int64_t subvector = next_subvector.fetch_add(1);
          if (subvector >= config_.pqSubvectors) {
            return;
          }
          encodeSubvector(subvector);
        }
      }));
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
    const float* query,
    int64_t topK,
    int64_t beamWidth,
    int64_t searchListSize) const {
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
  int64_t budget = searchListSize > 0
      ? searchListSize
      : config_.searchListSize;

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
  uint64_t magic = kVamanaFormatMagic;
  writeUint64(out, magic);
  writeUint64(out, std::bit_cast<uint64_t>(numVectors_));
  writeUint64(out, std::bit_cast<uint64_t>(config_.dimension));
  writeUint64(out, std::bit_cast<uint64_t>(medoid_));
  writeUint64(out, std::bit_cast<uint64_t>(config_.maxDegree));
  writeUint64(out, std::bit_cast<uint64_t>(config_.pqSubvectors));
  writeUint64(out, std::bit_cast<uint64_t>(config_.pqBitsPerCode));
  // The content-addressed cache path differs from the build path. Persist the
  // graph/PQ structural footprint; readers add their own local path storage to
  // the reservation before allocating.
  writeUint64(out, memoryBytes() - indexPath_.capacity());

  // Adjacency lists.
  for (int64_t i = 0; i < numVectors_; ++i) {
    auto& neighbors = adjList_[static_cast<size_t>(i)];
    int32_t degree = static_cast<int32_t>(neighbors.size());
    writeUint32(out, std::bit_cast<uint32_t>(degree));
    for (auto& nb : neighbors) {
      writeUint64(out, std::bit_cast<uint64_t>(nb.nodeId));
      writeFloat32(out, nb.distance);
    }
  }

  // PQ codebooks.
  for (auto& cb : codebooks_) {
    for (float centroid : cb.centroids) {
      writeFloat32(out, centroid);
    }
  }

  // PQ codes.
  out.write(
      reinterpret_cast<const char*>(pqCodes_.data()),
      static_cast<std::streamsize>(pqCodes_.size()));

  return out.good();
}

std::optional<uint64_t> VamanaIndex::residentBytesFromDisk(
    const std::string& path, int64_t expectedDimension) {
  std::ifstream input(path, std::ios::binary);
  uint64_t magic = 0;
  uint64_t num_vectors = 0;
  uint64_t dimension = 0;
  uint64_t ignored = 0;
  uint64_t resident_bytes = 0;
  if (!input || !readUint64(input, magic) || magic != kVamanaFormatMagic ||
      !readUint64(input, num_vectors) || !readUint64(input, dimension) ||
      !readUint64(input, ignored) || !readUint64(input, ignored) ||
      !readUint64(input, ignored) || !readUint64(input, ignored) ||
      !readUint64(input, resident_bytes) || num_vectors == 0 ||
      dimension != static_cast<uint64_t>(expectedDimension) ||
      resident_bytes == 0) {
    return std::nullopt;
  }
  return saturatingAdd(resident_bytes, std::string(path).capacity());
}

bool VamanaIndex::loadFromDisk(
    const std::string& path, uint64_t maxResidentBytes) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  in.seekg(0, std::ios::end);
  const auto file_size = in.tellg();
  if (file_size < static_cast<std::streamoff>(8 * sizeof(uint64_t)) ||
      file_size > static_cast<std::streamoff>(2ULL * 1024 * 1024 * 1024)) {
    return false;
  }
  in.seekg(0, std::ios::beg);

  uint64_t magic = 0;
  uint64_t encoded = 0;
  if (!readUint64(in, magic) || magic != kVamanaFormatMagic ||
      !readUint64(in, encoded)) {
    return false;
  }
  numVectors_ = std::bit_cast<int64_t>(encoded);
  if (!readUint64(in, encoded)) return false;
  config_.dimension = std::bit_cast<int64_t>(encoded);
  if (!readUint64(in, encoded)) return false;
  medoid_ = std::bit_cast<int64_t>(encoded);
  if (!readUint64(in, encoded)) return false;
  config_.maxDegree = std::bit_cast<int64_t>(encoded);
  if (!readUint64(in, encoded)) return false;
  config_.pqSubvectors = std::bit_cast<int64_t>(encoded);
  if (!readUint64(in, encoded)) return false;
  config_.pqBitsPerCode = std::bit_cast<int64_t>(encoded);
  uint64_t declared_structural_bytes = 0;
  if (!readUint64(in, declared_structural_bytes) ||
      declared_structural_bytes == 0 ||
      saturatingAdd(declared_structural_bytes, indexPath_.capacity()) >
          maxResidentBytes) {
    return false;
  }

  if (numVectors_ <= 0 || numVectors_ > 1'000'000LL ||
      config_.dimension <= 0 || config_.dimension > 65'536 ||
      medoid_ < 0 || medoid_ >= numVectors_ || config_.maxDegree <= 0 ||
      config_.maxDegree > 512 || config_.pqSubvectors <= 0 ||
      config_.pqSubvectors > config_.dimension ||
      config_.dimension % config_.pqSubvectors != 0 ||
      config_.pqBitsPerCode <= 0 || config_.pqBitsPerCode > 8) {
    return false;
  }
  const auto file_bytes = static_cast<uint64_t>(file_size);
  if (static_cast<uint64_t>(numVectors_) > file_bytes / sizeof(uint32_t) ||
      static_cast<uint64_t>(numVectors_) >
          std::numeric_limits<size_t>::max() /
              static_cast<uint64_t>(config_.pqSubvectors)) {
    return false;
  }

  config_.numVectors = numVectors_;

  uint64_t resident_bytes = sizeof(VamanaIndex);
  if (!checkedAdd(
          resident_bytes,
          saturatingMultiply(
              static_cast<uint64_t>(numVectors_),
              sizeof(std::vector<NeighborEntry>)),
          declared_structural_bytes)) {
    return false;
  }
  adjList_.resize(static_cast<size_t>(numVectors_));
  for (int64_t i = 0; i < numVectors_; ++i) {
    uint32_t encoded_degree = 0;
    if (!readUint32(in, encoded_degree)) return false;
    int32_t degree = std::bit_cast<int32_t>(encoded_degree);
    if (degree < 0 || degree > config_.maxDegree) return false;
    if (!checkedAdd(
            resident_bytes,
            saturatingMultiply(
                static_cast<uint64_t>(degree), sizeof(NeighborEntry)),
            declared_structural_bytes)) {
      return false;
    }
    auto& neighbors = adjList_[static_cast<size_t>(i)];
    neighbors.resize(static_cast<size_t>(degree));
    for (int32_t j = 0; j < degree; ++j) {
      if (!readUint64(in, encoded)) return false;
      auto& neighbor = neighbors[static_cast<size_t>(j)];
      neighbor.nodeId = std::bit_cast<int64_t>(encoded);
      if (neighbor.nodeId < 0 || neighbor.nodeId >= numVectors_ ||
          !readFloat32(in, neighbor.distance) || neighbor.distance < 0) {
        return false;
      }
    }
  }

  int64_t numCentroids = int64_t{1} << config_.pqBitsPerCode;
  if (numCentroids > 256) numCentroids = 256;
  int64_t subDim = config_.dimension / config_.pqSubvectors;
  if (subDim <= 0) subDim = 1;

  const auto payload_position = in.tellg();
  if (payload_position < 0 || payload_position > file_size) {
    return false;
  }
  const auto remaining_bytes =
      static_cast<uint64_t>(file_size - payload_position);
  const auto codebook_values =
      static_cast<uint64_t>(numCentroids) *
      static_cast<uint64_t>(config_.dimension);
  const auto codebook_bytes = codebook_values * sizeof(float);
  const auto pq_bytes =
      static_cast<uint64_t>(numVectors_) *
      static_cast<uint64_t>(config_.pqSubvectors);
  if (codebook_bytes > remaining_bytes ||
      pq_bytes != remaining_bytes - codebook_bytes) {
    return false;
  }

  if (!checkedAdd(
          resident_bytes,
          saturatingMultiply(
              static_cast<uint64_t>(config_.pqSubvectors), sizeof(PQCodebook)),
          declared_structural_bytes) ||
      !checkedAdd(
          resident_bytes, codebook_bytes, declared_structural_bytes) ||
      !checkedAdd(resident_bytes, pq_bytes, declared_structural_bytes)) {
    return false;
  }

  codebooks_.resize(static_cast<size_t>(config_.pqSubvectors));
  for (auto& cb : codebooks_) {
    cb.subvectorDim = subDim;
    cb.numCentroids = numCentroids;
    cb.centroids.resize(
        static_cast<size_t>(numCentroids) * static_cast<size_t>(subDim));
    for (float& centroid : cb.centroids) {
      if (!readFloat32(in, centroid)) return false;
    }
  }

  pqCodeSize_ = config_.pqSubvectors;
  pqCodes_.resize(
      static_cast<size_t>(numVectors_) * static_cast<size_t>(pqCodeSize_));
  in.read(reinterpret_cast<char*>(pqCodes_.data()),
          static_cast<std::streamsize>(pqCodes_.size()));

  if (!in) {
    return false;
  }

  indexPath_ = path;

  return in.peek() == std::char_traits<char>::eof() &&
      memoryBytes() - indexPath_.capacity() <= declared_structural_bytes &&
      memoryBytes() <= maxResidentBytes;
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
  for (int64_t index = 0; index < dimension; ++index) {
    if (!readFloat32(in, buffer[index])) {
      return false;
    }
  }
  return true;
}

}  // namespace pangrova::vector::diskann
