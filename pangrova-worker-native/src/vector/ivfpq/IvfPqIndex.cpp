#include "vector/ivfpq/IvfPqIndex.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <numeric>
#include <queue>
#include <stdexcept>

#include "artifact/ParsedArtifactCache.h"
#include "vector/VectorDistance.h"
#include "vector/ann/BinaryIndexIo.h"

namespace pangrova::vector::ivfpq {
namespace {

constexpr uint64_t kMagic = 0x4956465051000002ULL;
constexpr uint64_t kMaximumNodes = 1'000'000;
constexpr uint64_t kMaximumDimension = 65'536;
constexpr uint64_t kMaximumClusters = 4'096;
constexpr uint64_t kMaximumSubvectors = 1'024;

uint64_t checkedMultiply(uint64_t left, uint64_t right) {
  if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
    throw std::overflow_error("IVF-PQ artifact size overflows");
  }
  return left * right;
}

uint64_t checkedAdd(uint64_t left, uint64_t right) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    throw std::overflow_error("IVF-PQ artifact size overflows");
  }
  return left + right;
}

}  // namespace

IvfPqIndex::IvfPqIndex(IvfPqConfig config) : config_(config) {}

std::vector<int64_t> IvfPqIndex::sampleRows(int64_t count) const {
  const int64_t sample_count = std::min(count, config_.training_samples);
  std::vector<int64_t> rows(static_cast<size_t>(sample_count));
  for (int64_t index = 0; index < sample_count; ++index) {
    rows[static_cast<size_t>(index)] =
        static_cast<int64_t>((static_cast<__int128>(index) * count) /
                             sample_count);
  }
  return rows;
}

int64_t IvfPqIndex::nearest(
    const float* value, const float* centroids,
    int64_t clusters, int64_t dimension) const {
  int64_t best = 0;
  double best_distance = std::numeric_limits<double>::infinity();
  for (int64_t cluster = 0; cluster < clusters; ++cluster) {
    const double distance = l2Distance(
        value, centroids + cluster * dimension,
        static_cast<size_t>(dimension));
    if (distance < best_distance) {
      best_distance = distance;
      best = cluster;
    }
  }
  return best;
}

std::vector<float> IvfPqIndex::trainCentroids(
    const std::vector<float>& samples, int64_t sample_count,
    int64_t sample_dimension, int64_t clusters) const {
  std::vector<float> centroids(
      static_cast<size_t>(clusters * sample_dimension));
  for (int64_t cluster = 0; cluster < clusters; ++cluster) {
    const int64_t row = static_cast<int64_t>(
        (static_cast<__int128>(cluster) * sample_count) / clusters);
    std::copy_n(
        samples.data() + row * sample_dimension, sample_dimension,
        centroids.data() + cluster * sample_dimension);
  }
  std::vector<int64_t> assignments(static_cast<size_t>(sample_count));
  std::vector<double> sums(static_cast<size_t>(clusters * sample_dimension));
  std::vector<uint64_t> counts(static_cast<size_t>(clusters));
  for (int64_t iteration = 0; iteration < config_.training_iterations;
       ++iteration) {
    std::fill(sums.begin(), sums.end(), 0.0);
    std::fill(counts.begin(), counts.end(), 0);
    bool changed = false;
    for (int64_t row = 0; row < sample_count; ++row) {
      const int64_t selected = nearest(
          samples.data() + row * sample_dimension, centroids.data(),
          clusters, sample_dimension);
      changed = changed || assignments[static_cast<size_t>(row)] != selected ||
          iteration == 0;
      assignments[static_cast<size_t>(row)] = selected;
      ++counts[static_cast<size_t>(selected)];
      for (int64_t dimension = 0; dimension < sample_dimension; ++dimension) {
        sums[static_cast<size_t>(selected * sample_dimension + dimension)] +=
            samples[static_cast<size_t>(row * sample_dimension + dimension)];
      }
    }
    for (int64_t cluster = 0; cluster < clusters; ++cluster) {
      if (counts[static_cast<size_t>(cluster)] == 0) {
        continue;
      }
      for (int64_t dimension = 0; dimension < sample_dimension; ++dimension) {
        centroids[static_cast<size_t>(cluster * sample_dimension + dimension)] =
            static_cast<float>(
                sums[static_cast<size_t>(cluster * sample_dimension + dimension)] /
                static_cast<double>(counts[static_cast<size_t>(cluster)]));
      }
    }
    if (!changed) {
      break;
    }
  }
  return centroids;
}

void IvfPqIndex::build(
    const float* vectors, int64_t count, int64_t dimension) {
  if (vectors == nullptr || count <= 0 ||
      static_cast<uint64_t>(count) > kMaximumNodes || dimension <= 0 ||
      static_cast<uint64_t>(dimension) > kMaximumDimension ||
      config_.dimension != dimension ||
      !validDistanceMetric(static_cast<uint64_t>(config_.metric)) ||
      config_.coarse_clusters <= 0 ||
      static_cast<uint64_t>(config_.coarse_clusters) > kMaximumClusters ||
      config_.pq_subvectors <= 0 ||
      static_cast<uint64_t>(config_.pq_subvectors) > kMaximumSubvectors ||
      dimension % config_.pq_subvectors != 0 ||
      config_.bits_per_code < 1 || config_.bits_per_code > 8 ||
      config_.training_iterations < 1 || config_.training_iterations > 100 ||
      config_.training_samples < 1 || config_.training_samples > 1'000'000) {
    throw std::invalid_argument("IVF-PQ build configuration is outside safe bounds");
  }
  codebook_size_ = std::min<int64_t>(
      int64_t{1} << config_.bits_per_code,
      std::min(count, config_.training_samples));
  subvector_dimension_ = dimension / config_.pq_subvectors;
  const auto sample_rows = sampleRows(count);
  const int64_t sample_count = static_cast<int64_t>(sample_rows.size());
  std::vector<float> training(
      static_cast<size_t>(sample_count * dimension));
  for (int64_t sample = 0; sample < sample_count; ++sample) {
    std::copy_n(
        vectors + sample_rows[static_cast<size_t>(sample)] * dimension,
        dimension, training.data() + sample * dimension);
  }
  coarse_centroids_ = trainCentroids(
      training, sample_count, dimension, config_.coarse_clusters);

  std::vector<int64_t> coarse_assignments(static_cast<size_t>(count));
  for (int64_t row = 0; row < count; ++row) {
    coarse_assignments[static_cast<size_t>(row)] = nearest(
        vectors + row * dimension, coarse_centroids_.data(),
        config_.coarse_clusters, dimension);
  }

  pq_codebooks_.resize(static_cast<size_t>(
      config_.pq_subvectors * codebook_size_ * subvector_dimension_));
  std::vector<float> residual_samples(
      static_cast<size_t>(sample_count * subvector_dimension_));
  for (int64_t subvector = 0; subvector < config_.pq_subvectors; ++subvector) {
    const int64_t offset = subvector * subvector_dimension_;
    for (int64_t sample = 0; sample < sample_count; ++sample) {
      const int64_t row = sample_rows[static_cast<size_t>(sample)];
      const int64_t coarse = coarse_assignments[static_cast<size_t>(row)];
      for (int64_t dim = 0; dim < subvector_dimension_; ++dim) {
        residual_samples[static_cast<size_t>(
            sample * subvector_dimension_ + dim)] =
            vectors[row * dimension + offset + dim] -
            coarse_centroids_[static_cast<size_t>(
                coarse * dimension + offset + dim)];
      }
    }
    auto codebook = trainCentroids(
        residual_samples, sample_count, subvector_dimension_, codebook_size_);
    std::copy(
        codebook.begin(), codebook.end(),
        pq_codebooks_.begin() +
            subvector * codebook_size_ * subvector_dimension_);
  }

  list_offsets_.assign(static_cast<size_t>(config_.coarse_clusters + 1), 0);
  for (int64_t cluster : coarse_assignments) {
    ++list_offsets_[static_cast<size_t>(cluster + 1)];
  }
  std::partial_sum(list_offsets_.begin(), list_offsets_.end(), list_offsets_.begin());
  std::vector<uint64_t> cursor = list_offsets_;
  node_ids_.resize(static_cast<size_t>(count));
  codes_.resize(static_cast<size_t>(count * config_.pq_subvectors));
  for (int64_t row = 0; row < count; ++row) {
    const int64_t coarse = coarse_assignments[static_cast<size_t>(row)];
    const uint64_t position = cursor[static_cast<size_t>(coarse)]++;
    node_ids_[static_cast<size_t>(position)] = row;
    for (int64_t subvector = 0; subvector < config_.pq_subvectors; ++subvector) {
      const int64_t offset = subvector * subvector_dimension_;
      std::vector<float> residual(static_cast<size_t>(subvector_dimension_));
      for (int64_t dim = 0; dim < subvector_dimension_; ++dim) {
        residual[static_cast<size_t>(dim)] =
            vectors[row * dimension + offset + dim] -
            coarse_centroids_[static_cast<size_t>(
                coarse * dimension + offset + dim)];
      }
      const float* codebook = pq_codebooks_.data() +
          subvector * codebook_size_ * subvector_dimension_;
      codes_[static_cast<size_t>(position * config_.pq_subvectors + subvector)] =
          static_cast<uint8_t>(nearest(
              residual.data(), codebook, codebook_size_, subvector_dimension_));
    }
  }
}

std::vector<IvfPqCandidate> IvfPqIndex::search(
    const float* query, int64_t limit, int64_t probes) const {
  if (query == nullptr || limit <= 0 || probes <= 0 ||
      probes > config_.coarse_clusters || node_ids_.empty()) {
    throw std::invalid_argument("IVF-PQ search configuration is outside safe bounds");
  }
  std::vector<std::pair<double, int64_t>> coarse;
  coarse.reserve(static_cast<size_t>(config_.coarse_clusters));
  for (int64_t cluster = 0; cluster < config_.coarse_clusters; ++cluster) {
    coarse.emplace_back(
        metricDistance(config_.metric, query,
                       coarse_centroids_.data() + cluster * config_.dimension,
                       static_cast<size_t>(config_.dimension)),
        cluster);
  }
  std::partial_sort(
      coarse.begin(), coarse.begin() + probes, coarse.end());

  struct CandidateLess {
    bool operator()(const IvfPqCandidate& left,
                    const IvfPqCandidate& right) const {
      return left.distance < right.distance;
    }
  };
  std::priority_queue<
      IvfPqCandidate, std::vector<IvfPqCandidate>, CandidateLess> best;
  const size_t lookup_size = static_cast<size_t>(
      config_.pq_subvectors * codebook_size_);
  std::vector<double> lookup(lookup_size);
  std::vector<double> norms(
      config_.metric == DistanceMetric::COSINE ? lookup_size : 0);
  double query_norm = 0;
  for (int64_t dim = 0; dim < config_.dimension; ++dim) {
    query_norm += static_cast<double>(query[dim]) * query[dim];
  }
  for (int64_t selected = 0; selected < probes; ++selected) {
    const int64_t cluster = coarse[static_cast<size_t>(selected)].second;
    for (int64_t subvector = 0; subvector < config_.pq_subvectors; ++subvector) {
      const int64_t offset = subvector * subvector_dimension_;
      for (int64_t code = 0; code < codebook_size_; ++code) {
        const float* centroid = pq_codebooks_.data() +
            (subvector * codebook_size_ + code) * subvector_dimension_;
        double value = 0;
        double norm = 0;
        for (int64_t dim = 0; dim < subvector_dimension_; ++dim) {
          const double reconstructed = static_cast<double>(centroid[dim]) +
              coarse_centroids_[static_cast<size_t>(
                  cluster * config_.dimension + offset + dim)];
          const double query_value = query[offset + dim];
          if (config_.metric == DistanceMetric::L2) {
            const double delta = query_value - reconstructed;
            value += delta * delta;
          } else {
            value += query_value * reconstructed;
            norm += reconstructed * reconstructed;
          }
        }
        const size_t ordinal = static_cast<size_t>(subvector * codebook_size_ + code);
        lookup[ordinal] = config_.metric == DistanceMetric::INNER_PRODUCT
            ? -value : value;
        if (config_.metric == DistanceMetric::COSINE) {
          norms[ordinal] = norm;
        }
      }
    }
    for (uint64_t position = list_offsets_[static_cast<size_t>(cluster)];
         position < list_offsets_[static_cast<size_t>(cluster + 1)]; ++position) {
      double score = 0;
      double norm = 0;
      for (int64_t subvector = 0; subvector < config_.pq_subvectors; ++subvector) {
        const uint8_t code = codes_[static_cast<size_t>(
            position * config_.pq_subvectors + subvector)];
        const size_t ordinal = static_cast<size_t>(
            subvector * codebook_size_ + code);
        score += lookup[ordinal];
        if (config_.metric == DistanceMetric::COSINE) {
          norm += norms[ordinal];
        }
      }
      if (config_.metric == DistanceMetric::L2) {
        score = std::sqrt(score);
      } else if (config_.metric == DistanceMetric::COSINE) {
        score = query_norm == 0 || norm == 0 ? 1.0
            : 1.0 - std::clamp(score / std::sqrt(query_norm * norm), -1.0, 1.0);
      }
      IvfPqCandidate candidate{
          node_ids_[static_cast<size_t>(position)], score};
      if (best.size() < static_cast<size_t>(limit)) {
        best.push(candidate);
      } else if (candidate.distance < best.top().distance) {
        best.pop();
        best.push(candidate);
      }
    }
  }
  std::vector<IvfPqCandidate> result;
  result.reserve(best.size());
  while (!best.empty()) {
    result.push_back(best.top());
    best.pop();
  }
  std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
    return left.distance < right.distance ||
        (left.distance == right.distance && left.node_id < right.node_id);
  });
  return result;
}

bool IvfPqIndex::save(const std::string& path) const {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output || node_ids_.empty()) {
    return false;
  }
  for (uint64_t value : {
           kMagic, static_cast<uint64_t>(size()),
           static_cast<uint64_t>(config_.dimension),
           static_cast<uint64_t>(config_.coarse_clusters),
           static_cast<uint64_t>(config_.pq_subvectors),
           static_cast<uint64_t>(config_.bits_per_code),
           static_cast<uint64_t>(codebook_size_),
           static_cast<uint64_t>(config_.metric)}) {
    ann::writeUint64(output, value);
  }
  for (float value : coarse_centroids_) {
    ann::writeFloat(output, value);
  }
  for (float value : pq_codebooks_) {
    ann::writeFloat(output, value);
  }
  for (uint64_t offset : list_offsets_) {
    ann::writeUint64(output, offset);
  }
  for (int64_t node : node_ids_) {
    ann::writeUint64(output, static_cast<uint64_t>(node));
  }
  output.write(
      reinterpret_cast<const char*>(codes_.data()),
      static_cast<std::streamsize>(codes_.size()));
  output.flush();
  return output.good();
}

std::optional<uint64_t> IvfPqIndex::residentBytesFromDisk(
    const std::string& path, int64_t expected_dimension) {
  std::ifstream input(path, std::ios::binary);
  uint64_t magic = 0;
  uint64_t count = 0;
  uint64_t dimension = 0;
  uint64_t coarse = 0;
  uint64_t subvectors = 0;
  uint64_t bits = 0;
  uint64_t codebook = 0;
  uint64_t metric = 0;
  if (!ann::readUint64(input, magic) || magic != kMagic ||
      !ann::readUint64(input, count) || count == 0 || count > kMaximumNodes ||
      !ann::readUint64(input, dimension) || dimension == 0 ||
      dimension > kMaximumDimension ||
      dimension != static_cast<uint64_t>(expected_dimension) ||
      !ann::readUint64(input, coarse) || coarse == 0 ||
      coarse > kMaximumClusters ||
      !ann::readUint64(input, subvectors) || subvectors == 0 ||
      subvectors > kMaximumSubvectors || dimension % subvectors != 0 ||
      !ann::readUint64(input, bits) || bits == 0 || bits > 8 ||
      !ann::readUint64(input, codebook) || codebook == 0 ||
      codebook > (uint64_t{1} << bits) || codebook > count ||
      !ann::readUint64(input, metric) || !validDistanceMetric(metric)) {
    return std::nullopt;
  }
  try {
    const uint64_t float_values = checkedAdd(
        checkedMultiply(coarse, dimension),
        checkedMultiply(codebook, dimension));
    uint64_t result = checkedAdd(
        sizeof(IvfPqIndex), checkedMultiply(float_values, sizeof(float)));
    result = checkedAdd(result, checkedMultiply(coarse + 1, sizeof(uint64_t)));
    result = checkedAdd(result, checkedMultiply(count, sizeof(int64_t)));
    result = checkedAdd(result, checkedMultiply(count, subvectors));
    return result;
  } catch (const std::overflow_error&) {
    return std::nullopt;
  }
}

bool IvfPqIndex::load(
    const std::string& path, uint64_t max_resident_bytes) {
  const auto estimate = residentBytesFromDisk(path, config_.dimension);
  if (!estimate || *estimate > max_resident_bytes) {
    return false;
  }
  std::ifstream input(path, std::ios::binary);
  uint64_t values[8]{};
  for (uint64_t& value : values) {
    if (!ann::readUint64(input, value)) {
      return false;
    }
  }
  IvfPqIndex loaded({
      static_cast<int64_t>(values[2]), static_cast<int64_t>(values[3]),
      static_cast<int64_t>(values[4]), static_cast<int64_t>(values[5]), 1, 1,
      static_cast<DistanceMetric>(values[7])});
  loaded.codebook_size_ = static_cast<int64_t>(values[6]);
  loaded.subvector_dimension_ = loaded.config_.dimension /
      loaded.config_.pq_subvectors;
  loaded.coarse_centroids_.resize(static_cast<size_t>(
      loaded.config_.coarse_clusters * loaded.config_.dimension));
  loaded.pq_codebooks_.resize(static_cast<size_t>(
      loaded.config_.pq_subvectors * loaded.codebook_size_ *
      loaded.subvector_dimension_));
  for (float* begin : {loaded.coarse_centroids_.data(),
                       loaded.pq_codebooks_.data()}) {
    const size_t count = begin == loaded.coarse_centroids_.data()
        ? loaded.coarse_centroids_.size() : loaded.pq_codebooks_.size();
    for (size_t index = 0; index < count; ++index) {
      if (!ann::readFloat(input, begin[index]) || !std::isfinite(begin[index])) {
        return false;
      }
    }
  }
  loaded.list_offsets_.resize(
      static_cast<size_t>(loaded.config_.coarse_clusters + 1));
  for (uint64_t& offset : loaded.list_offsets_) {
    if (!ann::readUint64(input, offset)) {
      return false;
    }
  }
  const uint64_t count = values[1];
  if (loaded.list_offsets_.front() != 0 ||
      loaded.list_offsets_.back() != count ||
      !std::is_sorted(loaded.list_offsets_.begin(), loaded.list_offsets_.end())) {
    return false;
  }
  loaded.node_ids_.resize(static_cast<size_t>(count));
  std::vector<bool> seen(static_cast<size_t>(count));
  for (int64_t& node : loaded.node_ids_) {
    uint64_t value = 0;
    if (!ann::readUint64(input, value) || value >= count || seen[value]) {
      return false;
    }
    seen[value] = true;
    node = static_cast<int64_t>(value);
  }
  loaded.codes_.resize(static_cast<size_t>(count * values[4]));
  input.read(
      reinterpret_cast<char*>(loaded.codes_.data()),
      static_cast<std::streamsize>(loaded.codes_.size()));
  if (!input || input.peek() != std::char_traits<char>::eof() ||
      std::any_of(loaded.codes_.begin(), loaded.codes_.end(),
                  [&](uint8_t code) { return code >= values[6]; })) {
    return false;
  }
  *this = std::move(loaded);
  return true;
}

uint64_t IvfPqIndex::memoryBytes() const {
  return sizeof(*this) + coarse_centroids_.capacity() * sizeof(float) +
      pq_codebooks_.capacity() * sizeof(float) +
      list_offsets_.capacity() * sizeof(uint64_t) +
      node_ids_.capacity() * sizeof(int64_t) + codes_.capacity();
}

IvfPqIndexCache& IvfPqIndexCache::instance() {
  static IvfPqIndexCache cache;
  return cache;
}

std::shared_ptr<IvfPqIndex> IvfPqIndexCache::getOrLoad(
    const std::string& path, int64_t dimension) {
  return artifact::ParsedArtifactCache::instance().getOrLoad<IvfPqIndex>(
      "ivfpq:" + path,
      [path, dimension] {
        return IvfPqIndex::residentBytesFromDisk(path, dimension).value_or(0);
      },
      [path, dimension](uint64_t reservation) {
        auto value = std::make_shared<IvfPqIndex>(
            IvfPqConfig{dimension, 1, 1, 1, 1, 1});
        if (!value->load(path, reservation)) {
          value.reset();
        }
        return artifact::ParsedArtifactCache::LoadedArtifact<IvfPqIndex>{
            value, value ? value->memoryBytes() : 0};
      });
}

bool IvfPqIndexCache::enabled() const {
  return artifact::ParsedArtifactCache::instance().enabled();
}

}  // namespace pangrova::vector::ivfpq
