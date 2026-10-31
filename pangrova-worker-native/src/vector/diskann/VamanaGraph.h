// VamanaGraph: SSD-resident Vamana graph for DiskANN.
//
// Implements the Vamana graph structure from the DiskANN paper
// (Subramanya et al., NeurIPS 2019). The graph is stored on SSD with
// PQ-compressed vectors in memory for fast beam search navigation.
//
// Memory layout:
//   In-memory: PQ codebooks + compressed navigation vectors (~16 bytes/vector)
//   On-SSD: full-precision vectors + neighbor lists
//
// Thread-safe: search is lock-free; build parallelizes PQ codebook training.
// Placement: vector/diskann/ — Native disk-resident vector index.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "engine/resource/OwnerMemoryResource.h"

namespace pangrova::vector::diskann {

struct VamanaConfig {
  int64_t maxDegree = 64;
  int64_t searchListSize = 100;
  double alpha = 1.2;
  int64_t pqSubvectors = 0;
  int64_t pqBitsPerCode = 8;
  int64_t beamWidth = 4;
  int64_t dimension = 0;
  int64_t numVectors = 0;
};

struct NeighborEntry {
  int64_t nodeId;
  float distance;
};

struct GraphNode {
  int64_t nodeId;
  std::vector<NeighborEntry> neighbors;
};

struct PQCodebook {
  int64_t subvectorDim;
  int64_t numCentroids;
  std::vector<float> centroids;
};

struct SearchResult {
  int64_t nodeId;
  float distance;
};

/** Actual graph work, independent of the number of SQL result rows. */
struct GraphTraversalStats {
  uint64_t expanded = 0;
  bool frontier_exhausted = false;
};

class VamanaIndex {
 public:
  VamanaIndex(VamanaConfig config, std::string indexPath);
  ~VamanaIndex();

  VamanaIndex(const VamanaIndex&) = delete;
  VamanaIndex& operator=(const VamanaIndex&) = delete;

  void build(const float* vectors, int64_t numVectors, int64_t dimension);

  std::vector<SearchResult> search(
      const float* query,
      int64_t topK,
      int64_t beamWidth = 0,
      int64_t searchListSize = 0) const;

  /**
   * Visits nodes in the genuine PQ-guided graph walk. The visitor decides
   * candidate eligibility; ineligible nodes still expose their neighbors.
   * Both frontier and visited allocations belong to the caller's resource.
   * checkpoint must propagate cancellation/deadline failures; neither this
   * function nor its callers may switch to another execution plan.
   */
  GraphTraversalStats traverse(
      const float* query,
      uint64_t max_expanded,
      pangrova::resource::OwnerMemoryResource& resource,
      const std::function<void()>& checkpoint,
      const std::function<void(int64_t, float)>& visitor) const;

  /** Reads the immutable graph's declared process-resident footprint. */
  static std::optional<uint64_t> residentBytesFromDisk(
      const std::string& path, int64_t expectedDimension);

  /** Loads only when every resident allocation fits inside maxResidentBytes. */
  bool loadFromDisk(const std::string& path, uint64_t maxResidentBytes);
  bool saveToDisk(const std::string& path) const;

  // Read a full-precision vector from the companion .vecs file on SSD.
  // Returns true if the vector was successfully read into buffer.
  bool readVector(int64_t nodeId, float* buffer, int64_t dimension) const;

  int64_t numVectors() const { return numVectors_; }
  int64_t dimension() const { return config_.dimension; }
  int64_t medoid() const { return medoid_; }
  const std::string& indexPath() const { return indexPath_; }
  /** Process-resident graph/PQ bytes used for node-level cache admission. */
  uint64_t memoryBytes() const;

 private:
  void buildVamanaGraph(const float* vectors);
  void selectMedoid(const float* vectors);
  void robustPrune(
      int64_t nodeId, std::vector<NeighborEntry>& candidates,
      double alpha, int64_t maxDegree, const float* vectors);
  void buildPQCodebooks(const float* vectors);
  float computePQDistance(const uint8_t* pqCode, const float* query) const;

  VamanaConfig config_;
  std::string indexPath_;
  int64_t numVectors_ = 0;
  int64_t medoid_ = 0;

  std::vector<std::vector<NeighborEntry>> adjList_;
  std::vector<PQCodebook> codebooks_;
  std::vector<uint8_t> pqCodes_;
  int64_t pqCodeSize_ = 0;
};

}  // namespace pangrova::vector::diskann
