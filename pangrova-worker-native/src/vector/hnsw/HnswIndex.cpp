#include "vector/hnsw/HnswIndex.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <queue>
#include <stdexcept>
#include <unordered_set>

#include "artifact/ParsedArtifactCache.h"
#include "vector/VectorDistance.h"
#include "vector/ann/BinaryIndexIo.h"

namespace pangrova::vector::hnsw {
namespace {

constexpr uint64_t kMagic = 0x484E535700000002ULL;
constexpr uint64_t kMaximumNodes = 1'000'000;
constexpr uint64_t kMaximumDimension = 65'536;
constexpr uint32_t kMaximumLevel = 63;

uint64_t checkedMultiply(uint64_t left, uint64_t right) {
  if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
    throw std::overflow_error("HNSW artifact size overflows");
  }
  return left * right;
}

uint64_t checkedAdd(uint64_t left, uint64_t right) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    throw std::overflow_error("HNSW artifact size overflows");
  }
  return left + right;
}

uint64_t splitMix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

}  // namespace

HnswIndex::HnswIndex(HnswConfig config) : config_(config) {}

const float* HnswIndex::vectorAt(int64_t node) const {
  return vectors_.data() + node * config_.dimension;
}

int32_t HnswIndex::deterministicLevel(int64_t node_id) const {
  uint64_t bits = splitMix64(static_cast<uint64_t>(node_id));
  int32_t level = 0;
  const uint64_t divisor = static_cast<uint64_t>(config_.max_connections);
  while (level < static_cast<int32_t>(kMaximumLevel) && bits % divisor == 0) {
    ++level;
    bits /= divisor;
  }
  return level;
}

int64_t HnswIndex::greedySearch(
    const float* query, int64_t entry, int32_t level) const {
  int64_t current = entry;
  double current_distance = vectorDistance(
      query, vectorAt(current), static_cast<size_t>(config_.dimension));
  bool changed = true;
  while (changed) {
    changed = false;
    if (level > levels_[static_cast<size_t>(current)]) {
      break;
    }
    for (int64_t candidate :
         links_[static_cast<size_t>(current)][static_cast<size_t>(level)]) {
      const double distance = vectorDistance(
          query, vectorAt(candidate), static_cast<size_t>(config_.dimension));
      if (distance < current_distance) {
        current = candidate;
        current_distance = distance;
        changed = true;
      }
    }
  }
  return current;
}

std::vector<HnswIndex::Neighbor> HnswIndex::searchLayer(
    const float* query, int64_t entry, int32_t level, int64_t ef,
    int64_t visible_count) const {
  struct Ascending {
    bool operator()(const Neighbor& left, const Neighbor& right) const {
      return left.distance > right.distance;
    }
  };
  struct Descending {
    bool operator()(const Neighbor& left, const Neighbor& right) const {
      return left.distance < right.distance;
    }
  };
  std::priority_queue<Neighbor, std::vector<Neighbor>, Ascending> candidates;
  std::priority_queue<Neighbor, std::vector<Neighbor>, Descending> best;
  std::unordered_set<int64_t> visited;
  const auto initial = vectorDistance(
      query, vectorAt(entry), static_cast<size_t>(config_.dimension));
  candidates.push({entry, initial});
  best.push({entry, initial});
  visited.insert(entry);
  while (!candidates.empty()) {
    const Neighbor current = candidates.top();
    candidates.pop();
    if (best.size() >= static_cast<size_t>(ef) &&
        current.distance > best.top().distance) {
      break;
    }
    if (level > levels_[static_cast<size_t>(current.node_id)]) {
      continue;
    }
    for (int64_t neighbor : links_[static_cast<size_t>(current.node_id)]
                                      [static_cast<size_t>(level)]) {
      if (neighbor >= visible_count || !visited.emplace(neighbor).second) {
        continue;
      }
      const auto distance = vectorDistance(
          query, vectorAt(neighbor), static_cast<size_t>(config_.dimension));
      if (best.size() < static_cast<size_t>(ef) ||
          distance < best.top().distance) {
        candidates.push({neighbor, distance});
        best.push({neighbor, distance});
        if (best.size() > static_cast<size_t>(ef)) {
          best.pop();
        }
      }
    }
  }
  std::vector<Neighbor> result;
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

void HnswIndex::prune(int64_t node, int32_t level, int64_t maximum) {
  auto& neighbors = links_[static_cast<size_t>(node)][static_cast<size_t>(level)];
  std::sort(neighbors.begin(), neighbors.end(), [&](int64_t left, int64_t right) {
    const double left_distance = vectorDistance(
        vectorAt(node), vectorAt(left), static_cast<size_t>(config_.dimension));
    const double right_distance = vectorDistance(
        vectorAt(node), vectorAt(right), static_cast<size_t>(config_.dimension));
    return left_distance < right_distance ||
        (left_distance == right_distance && left < right);
  });
  neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
  if (neighbors.size() > static_cast<size_t>(maximum)) {
    neighbors.resize(static_cast<size_t>(maximum));
  }
}

void HnswIndex::connect(int64_t node, int64_t neighbor, int32_t level) {
  links_[static_cast<size_t>(node)][static_cast<size_t>(level)].push_back(neighbor);
  links_[static_cast<size_t>(neighbor)][static_cast<size_t>(level)].push_back(node);
  prune(node, level, level == 0 ? config_.max_connections * 2
                               : config_.max_connections);
  prune(neighbor, level, level == 0 ? config_.max_connections * 2
                                   : config_.max_connections);
}

uint64_t HnswIndex::estimatePeakBuildBytes(
    int64_t count, int64_t dimension, int64_t max_connections) {
  if (count < 0 || dimension <= 0 || max_connections < 2 ||
      static_cast<uint64_t>(count) > kMaximumNodes ||
      static_cast<uint64_t>(dimension) > kMaximumDimension ||
      max_connections > 512) {
    throw std::invalid_argument("HNSW build estimate is outside safe bounds");
  }
  // Four vector copies cover the incoming Velox batch, the accumulated source
  // vectors (including capacity growth), and the graph's resident copy. The
  // per-node allowance covers links at both levels, locator/hash storage,
  // search-layer queues/visited state, and allocator capacity slack. This is
  // deliberately an admission bound, not an after-the-fact usage counter.
  const uint64_t vector_bytes = checkedMultiply(
      checkedMultiply(static_cast<uint64_t>(count),
                      static_cast<uint64_t>(dimension)), 16);
  const uint64_t graph_bytes = checkedMultiply(
      static_cast<uint64_t>(count),
      checkedAdd(256, checkedMultiply(
          static_cast<uint64_t>(max_connections), 64)));
  return checkedAdd(checkedAdd(vector_bytes, graph_bytes), 1 << 20);
}

void HnswIndex::build(
    const float* vectors, int64_t count, int64_t dimension,
    uint64_t memory_grant_bytes, uint64_t retained_input_bytes) {
  if (vectors == nullptr || count <= 0 ||
      static_cast<uint64_t>(count) > kMaximumNodes || dimension <= 0 ||
      static_cast<uint64_t>(dimension) > kMaximumDimension ||
      config_.dimension != dimension ||
      !validDistanceMetric(static_cast<uint64_t>(config_.metric)) ||
      config_.max_connections < 2 ||
      config_.max_connections > 512 ||
      config_.ef_construction < config_.max_connections ||
      config_.ef_construction > 100'000) {
    throw std::invalid_argument("HNSW build configuration is outside safe bounds");
  }
  const uint64_t values = checkedMultiply(
      static_cast<uint64_t>(count), static_cast<uint64_t>(dimension));
  if (memory_grant_bytes != std::numeric_limits<uint64_t>::max() &&
      estimatePeakBuildBytes(count, dimension, config_.max_connections) >
          memory_grant_bytes) {
    throw std::runtime_error(
        "RESOURCE_EXHAUSTED: HNSW estimated peak exceeds memory grant");
  }
  vectors_.assign(vectors, vectors + values);
  levels_.resize(static_cast<size_t>(count));
  links_.resize(static_cast<size_t>(count));
  entry_point_ = -1;
  maximum_level_ = -1;
  for (int64_t node = 0; node < count; ++node) {
    const int32_t node_level = deterministicLevel(node);
    levels_[static_cast<size_t>(node)] = node_level;
    links_[static_cast<size_t>(node)].resize(static_cast<size_t>(node_level) + 1);
    if (entry_point_ < 0) {
      entry_point_ = node;
      maximum_level_ = node_level;
      if (memory_grant_bytes != std::numeric_limits<uint64_t>::max() &&
          count == 1 &&
          checkedAdd(retained_input_bytes, memoryBytes()) >
              memory_grant_bytes) {
        throw std::runtime_error(
            "RESOURCE_EXHAUSTED: HNSW resident graph exceeds memory grant");
      }
      continue;
    }
    int64_t entry = entry_point_;
    for (int32_t level = maximum_level_; level > node_level; --level) {
      entry = greedySearch(vectorAt(node), entry, level);
    }
    for (int32_t level = std::min(node_level, maximum_level_);
         level >= 0; --level) {
      auto candidates = searchLayer(
          vectorAt(node), entry, level, config_.ef_construction, node);
      const size_t selected = std::min(
          candidates.size(), static_cast<size_t>(config_.max_connections));
      for (size_t index = 0; index < selected; ++index) {
        connect(node, candidates[index].node_id, level);
      }
      if (!candidates.empty()) {
        entry = candidates.front().node_id;
      }
    }
    if (node_level > maximum_level_) {
      entry_point_ = node;
      maximum_level_ = node_level;
    }
    if (memory_grant_bytes != std::numeric_limits<uint64_t>::max() &&
        ((node + 1) % 2048 == 0 || node + 1 == count) &&
        checkedAdd(retained_input_bytes, memoryBytes()) >
            memory_grant_bytes) {
      throw std::runtime_error(
          "RESOURCE_EXHAUSTED: HNSW resident graph exceeds memory grant");
    }
  }
}

std::vector<HnswCandidate> HnswIndex::search(
    const float* query, int64_t limit, int64_t ef_search) const {
  if (query == nullptr || entry_point_ < 0 || limit <= 0 ||
      ef_search < limit || ef_search > 100'000) {
    throw std::invalid_argument("HNSW search configuration is outside safe bounds");
  }
  int64_t entry = entry_point_;
  for (int32_t level = maximum_level_; level > 0; --level) {
    entry = greedySearch(query, entry, level);
  }
  auto neighbors = searchLayer(query, entry, 0, ef_search, size());
  if (neighbors.size() > static_cast<size_t>(limit)) {
    neighbors.resize(static_cast<size_t>(limit));
  }
  std::vector<HnswCandidate> result;
  result.reserve(neighbors.size());
  for (const auto& neighbor : neighbors) {
    result.push_back({neighbor.node_id, neighbor.distance});
  }
  return result;
}

bool HnswIndex::save(const std::string& path) const {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output || levels_.empty()) {
    return false;
  }
  ann::writeUint64(output, kMagic);
  ann::writeUint64(output, static_cast<uint64_t>(size()));
  ann::writeUint64(output, static_cast<uint64_t>(config_.dimension));
  ann::writeUint64(output, static_cast<uint64_t>(config_.max_connections));
  ann::writeUint64(output, static_cast<uint64_t>(config_.metric));
  ann::writeUint64(output, static_cast<uint64_t>(entry_point_));
  ann::writeUint32(output, static_cast<uint32_t>(maximum_level_));
  for (float value : vectors_) {
    ann::writeFloat(output, value);
  }
  for (size_t node = 0; node < levels_.size(); ++node) {
    ann::writeUint32(output, static_cast<uint32_t>(levels_[node]));
    for (const auto& level : links_[node]) {
      ann::writeUint32(output, static_cast<uint32_t>(level.size()));
      for (int64_t neighbor : level) {
        ann::writeUint64(output, static_cast<uint64_t>(neighbor));
      }
    }
  }
  output.flush();
  return output.good();
}

std::optional<uint64_t> HnswIndex::residentBytesFromDisk(
    const std::string& path, int64_t expected_dimension) {
  std::ifstream input(path, std::ios::binary);
  uint64_t magic = 0;
  uint64_t count = 0;
  uint64_t dimension = 0;
  uint64_t connections = 0;
  uint64_t metric = 0;
  uint64_t entry = 0;
  uint32_t maximum_level = 0;
  if (!ann::readUint64(input, magic) || magic != kMagic ||
      !ann::readUint64(input, count) || count == 0 || count > kMaximumNodes ||
      !ann::readUint64(input, dimension) || dimension == 0 ||
      dimension > kMaximumDimension ||
      dimension != static_cast<uint64_t>(expected_dimension) ||
      !ann::readUint64(input, connections) || connections < 2 ||
      connections > 512 || !ann::readUint64(input, metric) ||
      !validDistanceMetric(metric) ||
      !ann::readUint64(input, entry) || entry >= count ||
      !ann::readUint32(input, maximum_level) || maximum_level > kMaximumLevel) {
    return std::nullopt;
  }
  try {
    const uint64_t vector_bytes = checkedMultiply(
        checkedMultiply(count, dimension), sizeof(float));
    uint64_t resident = checkedAdd(sizeof(HnswIndex), vector_bytes);
    resident = checkedAdd(
        resident, checkedMultiply(count, sizeof(int32_t)));
    resident = checkedAdd(
        resident,
        checkedMultiply(count, sizeof(std::vector<std::vector<int64_t>>)));
    input.seekg(static_cast<std::streamoff>(vector_bytes), std::ios::cur);
    if (!input) {
      return std::nullopt;
    }
    for (uint64_t node = 0; node < count; ++node) {
      uint32_t level_count = 0;
      if (!ann::readUint32(input, level_count) ||
          level_count > maximum_level || level_count > kMaximumLevel) {
        return std::nullopt;
      }
      resident = checkedAdd(
          resident,
          checkedMultiply(
              static_cast<uint64_t>(level_count) + 1,
              sizeof(std::vector<int64_t>)));
      for (uint32_t level = 0; level <= level_count; ++level) {
        uint32_t degree = 0;
        if (!ann::readUint32(input, degree) || degree > connections * 2) {
          return std::nullopt;
        }
        const uint64_t neighbor_bytes = checkedMultiply(
            degree, sizeof(int64_t));
        resident = checkedAdd(resident, neighbor_bytes);
        input.seekg(
            static_cast<std::streamoff>(neighbor_bytes), std::ios::cur);
        if (!input) {
          return std::nullopt;
        }
      }
    }
    if (input.peek() != std::char_traits<char>::eof()) {
      return std::nullopt;
    }
    return resident;
  } catch (const std::overflow_error&) {
    return std::nullopt;
  }
}

bool HnswIndex::load(
    const std::string& path, uint64_t max_resident_bytes) {
  const auto estimate = residentBytesFromDisk(path, config_.dimension);
  if (!estimate || *estimate > max_resident_bytes) {
    return false;
  }
  std::ifstream input(path, std::ios::binary);
  uint64_t magic = 0;
  uint64_t count = 0;
  uint64_t dimension = 0;
  uint64_t connections = 0;
  uint64_t metric = 0;
  uint64_t entry = 0;
  uint32_t maximum_level = 0;
  if (!ann::readUint64(input, magic) || !ann::readUint64(input, count) ||
      !ann::readUint64(input, dimension) ||
      !ann::readUint64(input, connections) || !ann::readUint64(input, metric) ||
      !validDistanceMetric(metric) || !ann::readUint64(input, entry) ||
      !ann::readUint32(input, maximum_level) || magic != kMagic ||
      entry >= count || maximum_level > kMaximumLevel) {
    return false;
  }
  HnswIndex loaded({static_cast<int64_t>(dimension),
                    static_cast<int64_t>(connections),
                    static_cast<int64_t>(connections),
                    static_cast<DistanceMetric>(metric)});
  loaded.vectors_.resize(static_cast<size_t>(count * dimension));
  for (float& value : loaded.vectors_) {
    if (!ann::readFloat(input, value) || !std::isfinite(value)) {
      return false;
    }
  }
  loaded.levels_.resize(static_cast<size_t>(count));
  loaded.links_.resize(static_cast<size_t>(count));
  for (uint64_t node = 0; node < count; ++node) {
    uint32_t level_count = 0;
    if (!ann::readUint32(input, level_count) || level_count > kMaximumLevel ||
        level_count > maximum_level) {
      return false;
    }
    loaded.levels_[node] = static_cast<int32_t>(level_count);
    loaded.links_[node].resize(static_cast<size_t>(level_count) + 1);
    for (auto& level : loaded.links_[node]) {
      uint32_t degree = 0;
      if (!ann::readUint32(input, degree) || degree > connections * 2) {
        return false;
      }
      level.resize(degree);
      std::unordered_set<int64_t> unique;
      for (int64_t& neighbor : level) {
        uint64_t value = 0;
        if (!ann::readUint64(input, value) || value >= count || value == node ||
            !unique.emplace(static_cast<int64_t>(value)).second) {
          return false;
        }
        neighbor = static_cast<int64_t>(value);
      }
    }
  }
  if (input.peek() != std::char_traits<char>::eof() ||
      loaded.levels_[entry] != static_cast<int32_t>(maximum_level)) {
    return false;
  }
  loaded.entry_point_ = static_cast<int64_t>(entry);
  loaded.maximum_level_ = static_cast<int32_t>(maximum_level);
  *this = std::move(loaded);
  return true;
}

uint64_t HnswIndex::memoryBytes() const {
  uint64_t result = sizeof(*this) + vectors_.capacity() * sizeof(float) +
      levels_.capacity() * sizeof(int32_t) +
      links_.capacity() * sizeof(std::vector<std::vector<int64_t>>);
  for (const auto& node : links_) {
    result += node.capacity() * sizeof(std::vector<int64_t>);
    for (const auto& level : node) {
      result += level.capacity() * sizeof(int64_t);
    }
  }
  return result;
}

HnswIndexCache& HnswIndexCache::instance() {
  static HnswIndexCache cache;
  return cache;
}

std::shared_ptr<HnswIndex> HnswIndexCache::getOrLoad(
    const std::string& path, int64_t dimension) {
  return artifact::ParsedArtifactCache::instance().getOrLoad<HnswIndex>(
      "hnsw:" + path,
      [path, dimension] {
        return HnswIndex::residentBytesFromDisk(path, dimension).value_or(0);
      },
      [path, dimension](uint64_t reservation) {
        auto value = std::make_shared<HnswIndex>(HnswConfig{dimension, 2, 2});
        if (!value->load(path, reservation)) {
          value.reset();
        }
        return artifact::ParsedArtifactCache::LoadedArtifact<HnswIndex>{
            value, value ? value->memoryBytes() : 0};
      });
}

bool HnswIndexCache::enabled() const {
  return artifact::ParsedArtifactCache::instance().enabled();
}

}  // namespace pangrova::vector::hnsw
