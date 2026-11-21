#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "vector/VectorDistance.h"

namespace pangrova::vector::ivfpq {

struct IvfPqConfig final {
  int64_t dimension{0};
  int64_t coarse_clusters{64};
  int64_t pq_subvectors{8};
  int64_t bits_per_code{8};
  int64_t training_iterations{20};
  int64_t training_samples{100'000};
  DistanceMetric metric{DistanceMetric::L2};
};

struct IvfPqCandidate final {
  int64_t node_id{0};
  double distance{0};
};

/** Immutable IVF-PQ index with deterministic sampled Lloyd training. */
class IvfPqIndex final {
 public:
  explicit IvfPqIndex(IvfPqConfig config);

  void build(const float* vectors, int64_t count, int64_t dimension);
  std::vector<IvfPqCandidate> search(
      const float* query, int64_t limit, int64_t probes) const;
  bool save(const std::string& path) const;
  bool load(const std::string& path, uint64_t max_resident_bytes);

  static std::optional<uint64_t> residentBytesFromDisk(
      const std::string& path, int64_t expected_dimension);
  uint64_t memoryBytes() const;
  int64_t dimension() const { return config_.dimension; }
  DistanceMetric metric() const { return config_.metric; }
  int64_t size() const { return static_cast<int64_t>(node_ids_.size()); }
  int64_t coarseClusters() const { return config_.coarse_clusters; }

 private:
  std::vector<float> trainCentroids(
      const std::vector<float>& samples, int64_t sample_count,
      int64_t sample_dimension, int64_t clusters) const;
  int64_t nearest(
      const float* value, const float* centroids,
      int64_t clusters, int64_t dimension) const;
  std::vector<int64_t> sampleRows(int64_t count) const;

  IvfPqConfig config_;
  int64_t codebook_size_{0};
  int64_t subvector_dimension_{0};
  std::vector<float> coarse_centroids_;
  std::vector<float> pq_codebooks_;
  std::vector<uint64_t> list_offsets_;
  std::vector<int64_t> node_ids_;
  std::vector<uint8_t> codes_;
};

/** Shared quota-backed cache for parsed IVF-PQ artifacts. */
class IvfPqIndexCache final {
 public:
  static IvfPqIndexCache& instance();
  std::shared_ptr<IvfPqIndex> getOrLoad(
      const std::string& path, int64_t dimension);
  bool enabled() const;

 private:
  IvfPqIndexCache() = default;
};

}  // namespace pangrova::vector::ivfpq
