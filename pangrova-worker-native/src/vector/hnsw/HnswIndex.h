#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "vector/VectorDistance.h"

namespace pangrova::vector::hnsw {

struct HnswConfig final {
  int64_t dimension{0};
  int64_t max_connections{16};
  int64_t ef_construction{200};
  DistanceMetric metric{DistanceMetric::L2};
};

struct HnswCandidate final {
  int64_t node_id{0};
  double distance{0};
};

/** Immutable deterministic HNSW graph. Search is lock-free after publication. */
class HnswIndex final {
 public:
  explicit HnswIndex(HnswConfig config);

  /** Conservative peak estimate, including retained input and graph scratch. */
  static uint64_t estimatePeakBuildBytes(
      int64_t count, int64_t dimension, int64_t max_connections);
  void build(
      const float* vectors, int64_t count, int64_t dimension,
      uint64_t memory_grant_bytes = std::numeric_limits<uint64_t>::max(),
      uint64_t retained_input_bytes = 0);
  std::vector<HnswCandidate> search(
      const float* query, int64_t limit, int64_t ef_search) const;
  bool save(const std::string& path) const;
  bool load(const std::string& path, uint64_t max_resident_bytes);

  static std::optional<uint64_t> residentBytesFromDisk(
      const std::string& path, int64_t expected_dimension);
  uint64_t memoryBytes() const;
  int64_t dimension() const { return config_.dimension; }
  DistanceMetric metric() const { return config_.metric; }
  int64_t size() const { return static_cast<int64_t>(levels_.size()); }

 private:
  struct Neighbor final {
    int64_t node_id{0};
    double distance{0};
  };

  int32_t deterministicLevel(int64_t node_id) const;
  std::vector<Neighbor> searchLayer(
      const float* query, int64_t entry, int32_t level, int64_t ef,
      int64_t visible_count) const;
  int64_t greedySearch(
      const float* query, int64_t entry, int32_t level) const;
  void connect(int64_t node, int64_t neighbor, int32_t level);
  void prune(int64_t node, int32_t level, int64_t maximum);
  const float* vectorAt(int64_t node) const;
  double vectorDistance(const float* left, const float* right, size_t dimension) const {
    return metricDistance(config_.metric, left, right, dimension);
  }

  HnswConfig config_;
  std::vector<float> vectors_;
  std::vector<int32_t> levels_;
  std::vector<std::vector<std::vector<int64_t>>> links_;
  int64_t entry_point_{-1};
  int32_t maximum_level_{-1};
};

/** Shared quota-backed cache for parsed HNSW artifacts. */
class HnswIndexCache final {
 public:
  static HnswIndexCache& instance();
  std::shared_ptr<HnswIndex> getOrLoad(
      const std::string& path, int64_t dimension);
  bool enabled() const;

 private:
  HnswIndexCache() = default;
};

}  // namespace pangrova::vector::hnsw
