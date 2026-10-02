#include "cache/SemanticResultCache.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <list>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>

#include "vector/VectorDistance.h"
#include "vector/hnsw/HnswIndex.h"

namespace pangrova::cache {
namespace {

using Clock = std::chrono::steady_clock;

struct Entry final {
  ReuseIdentity identity;
  std::vector<float> request_key;
  CachedValue value;
  Clock::time_point expires_at;
  uint64_t bytes{0};
  uint64_t access_sequence{0};
  bool live{true};
};

struct Partition final {
  std::string tenant_id;
  std::vector<Entry> entries;
  std::shared_ptr<vector::hnsw::HnswIndex> index;
  std::vector<size_t> indexed_entries;
  size_t active_entries{0};
  uint64_t bytes{0};
  size_t indexed_source_size{0};
};

struct TenantUsage final {
  uint64_t entries{0};
  uint64_t bytes{0};
};

struct State final {
  std::mutex mutex;
  std::unordered_map<std::string, Partition> partitions;
  std::unordered_map<std::string, TenantUsage> tenants;
  std::unordered_map<std::string, TaskCacheUsage> task_usage;
  uint64_t entries{0};
  uint64_t bytes{0};
  uint64_t access_sequence{0};
};

// These are worker-owned safety limits. They deliberately cannot be supplied
// by an OperatorBinding: an untrusted query may narrow its partition quota, but
// it must never be able to raise the tenant or process-wide resident set.
constexpr uint64_t kWorkerMaxEntries = 1'000'000;
constexpr uint64_t kWorkerMaxBytes = 4ULL * 1024 * 1024 * 1024;
constexpr uint64_t kTenantMaxEntries = 250'000;
constexpr uint64_t kTenantMaxBytes = 1024ULL * 1024 * 1024;
constexpr uint64_t kPartitionMaxEntries = 100'000;
constexpr uint64_t kPartitionMaxBytes = 512ULL * 1024 * 1024;
// Conservatively reserves graph links, the duplicate vector retained by HNSW,
// and container/node overhead. Logical quota accounting must upper-bound the
// cache's resident set rather than counting only returned value bytes.
constexpr uint64_t kIndexAndContainerReservationPerEntry = 1024;

State& state() {
  static State value;
  return value;
}

void append(std::string& target, const std::string& value) {
  const uint64_t size = value.size();
  target.append(reinterpret_cast<const char*>(&size), sizeof(size));
  target.append(value);
}

std::string partitionKey(const ReuseIdentity& identity) {
  std::string key;
  append(key, identity.tenant_id);
  append(key, identity.principal);
  append(key, identity.policy_context_hash);
  append(key, identity.model_identity);
  append(key, identity.capability_identity);
  append(key, identity.function_name);
  append(key, identity.result_type);
  append(key, identity.parameter_fingerprint);
  append(key, identity.dependency_scope_hash);
  return key;
}

double similarity(
    const std::vector<float>& left, const std::vector<float>& right) {
  if (left.size() != right.size() || left.empty()) return -1.0;
  return 1.0 - vector::cosineDistance(left.data(), right.data(), left.size());
}

void retire(State& cache, Partition& partition, Entry& entry) {
  if (!entry.live) return;
  entry.live = false;
  --partition.active_entries;
  partition.bytes -= entry.bytes;
  auto tenant = cache.tenants.find(partition.tenant_id);
  if (tenant == cache.tenants.end() || tenant->second.entries == 0 ||
      tenant->second.bytes < entry.bytes) {
    throw std::logic_error("Semantic cache tenant accounting is corrupt");
  }
  --tenant->second.entries;
  tenant->second.bytes -= entry.bytes;
  if (tenant->second.entries == 0) cache.tenants.erase(tenant);
  --cache.entries;
  cache.bytes -= entry.bytes;
}

void removeExpiredAndStale(
    State& cache,
    Partition& partition,
    const std::string& dependency_version,
    Clock::time_point now,
    TaskCacheUsage& usage) {
  for (auto& entry : partition.entries) {
    if (entry.live &&
        (entry.expires_at <= now ||
         entry.identity.dependency_version_hash != dependency_version)) {
      retire(cache, partition, entry);
      ++usage.evictions;
    }
  }
}

void rebuildIndex(Partition& partition, uint32_t dimension) {
  if (partition.active_entries < 256) {
    partition.index.reset();
    partition.indexed_entries.clear();
    partition.indexed_source_size = partition.entries.size();
    return;
  }
  std::vector<float> vectors;
  partition.indexed_entries.clear();
  vectors.reserve(partition.active_entries * dimension);
  for (size_t index = 0; index < partition.entries.size(); ++index) {
    const auto& entry = partition.entries[index];
    if (!entry.live) continue;
    partition.indexed_entries.push_back(index);
    vectors.insert(
        vectors.end(), entry.request_key.begin(), entry.request_key.end());
  }
  auto rebuilt = std::make_shared<vector::hnsw::HnswIndex>(
      vector::hnsw::HnswConfig{static_cast<int64_t>(dimension), 16, 100});
  rebuilt->build(
      vectors.data(), static_cast<int64_t>(partition.indexed_entries.size()),
      static_cast<int64_t>(dimension));
  partition.index = std::move(rebuilt);
  partition.indexed_source_size = partition.entries.size();
}

std::vector<size_t> candidates(
    const Partition& partition, const std::vector<float>& request_key) {
  std::vector<size_t> result;
  if (partition.index) {
    auto nearest = partition.index->search(
        request_key.data(), std::min<int64_t>(32, partition.index->size()),
        std::min<int64_t>(128, partition.index->size()));
    result.reserve(nearest.size() +
                   partition.entries.size() - partition.indexed_source_size);
    for (const auto& candidate : nearest) {
      result.push_back(partition.indexed_entries.at(
          static_cast<size_t>(candidate.node_id)));
    }
    for (size_t index = partition.indexed_source_size;
         index < partition.entries.size(); ++index) {
      result.push_back(index);
    }
    return result;
  }
  result.resize(partition.entries.size());
  for (size_t index = 0; index < result.size(); ++index) result[index] = index;
  return result;
}

Entry* oldestLiveEntry(Partition& partition) {
  Entry* oldest = nullptr;
  for (auto& entry : partition.entries) {
    if (entry.live &&
        (!oldest || entry.access_sequence < oldest->access_sequence)) {
      oldest = &entry;
    }
  }
  return oldest;
}

void enforcePartitionLimits(
    State& cache,
    Partition& partition,
    const ReusePolicy& policy,
    TaskCacheUsage& usage) {
  const uint64_t max_entries =
      std::min(policy.max_entries, kPartitionMaxEntries);
  const uint64_t max_bytes = std::min(policy.max_bytes, kPartitionMaxBytes);
  while (partition.active_entries > max_entries ||
         partition.bytes > max_bytes) {
    auto* oldest = oldestLiveEntry(partition);
    if (!oldest) break;
    retire(cache, partition, *oldest);
    ++usage.evictions;
  }
}

void enforceTenantLimits(
    State& cache, const std::string& tenant_id, TaskCacheUsage& usage) {
  auto tenant = cache.tenants.find(tenant_id);
  while (tenant != cache.tenants.end() &&
         (tenant->second.entries > kTenantMaxEntries ||
          tenant->second.bytes > kTenantMaxBytes)) {
    Partition* oldest_partition = nullptr;
    Entry* oldest_entry = nullptr;
    for (auto& [key, partition] : cache.partitions) {
      (void)key;
      if (partition.tenant_id != tenant_id) continue;
      auto* candidate = oldestLiveEntry(partition);
      if (candidate &&
          (!oldest_entry ||
           candidate->access_sequence < oldest_entry->access_sequence)) {
        oldest_partition = &partition;
        oldest_entry = candidate;
      }
    }
    if (!oldest_entry) {
      throw std::logic_error("Semantic cache tenant quota cannot be enforced");
    }
    retire(cache, *oldest_partition, *oldest_entry);
    ++usage.evictions;
    tenant = cache.tenants.find(tenant_id);
  }
}

void compactIfNeeded(Partition& partition, uint32_t dimension) {
  const size_t retired = partition.entries.size() - partition.active_entries;
  if (retired < 1024 && retired <= partition.active_entries) return;
  std::vector<Entry> compacted;
  compacted.reserve(partition.active_entries);
  for (auto& entry : partition.entries) {
    if (entry.live) compacted.push_back(std::move(entry));
  }
  partition.entries.swap(compacted);
  partition.index.reset();
  partition.indexed_entries.clear();
  partition.indexed_source_size = 0;
  if (partition.active_entries >= 256) rebuildIndex(partition, dimension);
}

uint64_t fnv1a(const char* data, size_t size) {
  uint64_t hash = 1469598103934665603ULL;
  for (size_t i = 0; i < size; ++i) {
    hash ^= static_cast<unsigned char>(data[i]);
    hash *= 1099511628211ULL;
  }
  return hash;
}

uint64_t identityBytes(const ReuseIdentity& identity) {
  return identity.tenant_id.size() + identity.principal.size() +
      identity.policy_context_hash.size() + identity.model_identity.size() +
      identity.capability_identity.size() + identity.function_name.size() +
      identity.result_type.size() + identity.parameter_fingerprint.size() +
      identity.dependency_scope_hash.size() +
      identity.dependency_version_hash.size();
}

}  // namespace

SemanticResultCache& SemanticResultCache::instance() {
  static SemanticResultCache cache;
  return cache;
}

std::optional<CachedValue> SemanticResultCache::lookup(
    const ReuseIdentity& identity,
    const std::vector<float>& request_key,
    const ReusePolicy& policy,
    const std::string& task_id) {
  if (request_key.size() != policy.key_dimension || task_id.empty() ||
      policy.ttl_ms == 0 || policy.max_entries == 0 || policy.max_bytes == 0 ||
      policy.similarity_threshold < 0.0 ||
      policy.similarity_threshold > 1.0) {
    throw std::invalid_argument("Semantic cache lookup identity is incomplete");
  }
  auto& cache = state();
  std::lock_guard lock(cache.mutex);
  auto& usage = cache.task_usage[task_id];
  auto found = cache.partitions.find(partitionKey(identity));
  if (found == cache.partitions.end()) {
    ++usage.misses;
    return std::nullopt;
  }
  auto& partition = found->second;
  removeExpiredAndStale(
      cache, partition, identity.dependency_version_hash, Clock::now(), usage);
  if (partition.active_entries == 0) {
    cache.partitions.erase(found);
    ++usage.misses;
    return std::nullopt;
  }
  Entry* best = nullptr;
  double best_similarity = policy.similarity_threshold;
  for (size_t index : candidates(partition, request_key)) {
    auto& entry = partition.entries.at(index);
    if (!entry.live) continue;
    const double score = similarity(request_key, entry.request_key);
    if (score >= best_similarity &&
        (!best || score > best_similarity ||
         entry.access_sequence > best->access_sequence)) {
      best = &entry;
      best_similarity = score;
    }
  }
  if (!best) {
    ++usage.misses;
    return std::nullopt;
  }
  best->access_sequence = ++cache.access_sequence;
  ++usage.hits;
  usage.hit_bytes += best->bytes;
  return best->value;
}

bool SemanticResultCache::put(
    ReuseIdentity identity,
    std::vector<float> request_key,
    CachedValue value,
    const ReusePolicy& policy,
    const std::string& task_id) {
  if (request_key.size() != policy.key_dimension || task_id.empty() ||
      policy.ttl_ms == 0 || policy.max_entries == 0 || policy.max_bytes == 0 ||
      policy.similarity_threshold < 0.0 ||
      policy.similarity_threshold > 1.0) {
    throw std::invalid_argument("Semantic cache write identity is incomplete");
  }
  auto& cache = state();
  std::lock_guard lock(cache.mutex);
  auto& usage = cache.task_usage[task_id];
  const uint64_t bytes = sizeof(Entry) + identityBytes(identity) +
      valueBytes(value) + 2 * request_key.size() * sizeof(float) +
      kIndexAndContainerReservationPerEntry;
  usage.miss_bytes += bytes;
  const uint64_t partition_max_bytes =
      std::min(policy.max_bytes, kPartitionMaxBytes);
  if (bytes > partition_max_bytes || bytes > kTenantMaxBytes ||
      bytes > kWorkerMaxBytes) {
    ++usage.admission_rejections;
    return false;
  }
  const std::string tenant_id = identity.tenant_id;
  const std::string partition_key = partitionKey(identity);
  auto [partition_it, inserted] = cache.partitions.try_emplace(partition_key);
  auto& partition = partition_it->second;
  if (inserted) {
    partition.tenant_id = tenant_id;
  } else if (partition.tenant_id != tenant_id) {
    throw std::logic_error("Semantic cache partition tenant mismatch");
  }
  removeExpiredAndStale(
      cache, partition, identity.dependency_version_hash, Clock::now(), usage);
  partition.entries.push_back(Entry{
      std::move(identity), std::move(request_key), std::move(value),
      Clock::now() + std::chrono::milliseconds(policy.ttl_ms), bytes,
      ++cache.access_sequence, true});
  ++partition.active_entries;
  partition.bytes += bytes;
  auto& tenant = cache.tenants[tenant_id];
  ++tenant.entries;
  tenant.bytes += bytes;
  ++cache.entries;
  cache.bytes += bytes;
  Entry& admitted = partition.entries.back();
  enforcePartitionLimits(cache, partition, policy, usage);
  enforceTenantLimits(cache, tenant_id, usage);
  if (admitted.live &&
      (cache.entries > kWorkerMaxEntries || cache.bytes > kWorkerMaxBytes)) {
    // A worker limit is an admission fence, not a global eviction policy. A
    // tenant filling the worker must not evict a different tenant's entries.
    retire(cache, partition, admitted);
    ++usage.admission_rejections;
  }
  const bool cached = admitted.live;
  compactIfNeeded(partition, policy.key_dimension);
  const size_t pending = partition.entries.size() - partition.indexed_source_size;
  if (partition.active_entries >= 256 &&
      (!partition.index || pending >= 128 ||
       pending * 10 >= partition.active_entries)) {
    rebuildIndex(partition, policy.key_dimension);
  }
  if (partition.active_entries == 0) cache.partitions.erase(partition_key);
  return cached;
}

void SemanticResultCache::invalidateAll() {
  auto& cache = state();
  std::lock_guard lock(cache.mutex);
  cache.partitions.clear();
  cache.tenants.clear();
  cache.entries = 0;
  cache.bytes = 0;
}

TaskCacheUsage SemanticResultCache::taskUsage(
    const std::string& task_id) const {
  auto& cache = state();
  std::lock_guard lock(cache.mutex);
  auto found = cache.task_usage.find(task_id);
  return found == cache.task_usage.end() ? TaskCacheUsage{} : found->second;
}

void SemanticResultCache::releaseTaskUsage(const std::string& task_id) {
  auto& cache = state();
  std::lock_guard lock(cache.mutex);
  cache.task_usage.erase(task_id);
}

std::vector<float> SemanticResultCache::requestKey(
    const std::string& normalized_text, uint32_t dimension) {
  if (normalized_text.empty() || dimension < 64 || dimension > 4'096) {
    throw std::invalid_argument("Semantic cache request text or dimension is invalid");
  }
  std::vector<float> result(dimension, 0.0F);
  std::string text;
  text.reserve(normalized_text.size());
  bool space = true;
  for (unsigned char value : normalized_text) {
    if (std::isspace(value)) {
      if (!space) text.push_back(' ');
      space = true;
    } else {
      text.push_back(static_cast<char>(std::tolower(value)));
      space = false;
    }
  }
  if (!text.empty() && text.back() == ' ') text.pop_back();
  if (text.empty()) throw std::invalid_argument("Semantic cache key is empty");
  if (text.size() < 3) text.append(3 - text.size(), '\0');
  for (size_t index = 0; index + 3 <= text.size(); ++index) {
    const uint64_t hash = fnv1a(text.data() + index, 3);
    const size_t bucket = static_cast<size_t>(hash % dimension);
    result[bucket] += (hash >> 63U) == 0 ? 1.0F : -1.0F;
  }
  double norm = 0.0;
  for (float value : result) norm += static_cast<double>(value) * value;
  norm = std::sqrt(norm);
  if (norm == 0.0) throw std::invalid_argument("Semantic cache key has zero norm");
  for (float& value : result) value = static_cast<float>(value / norm);
  return result;
}

uint64_t SemanticResultCache::valueBytes(const CachedValue& value) {
  return std::visit([](const auto& typed) -> uint64_t {
    using T = std::decay_t<decltype(typed)>;
    if constexpr (std::is_same_v<T, std::string>) return typed.size();
    if constexpr (std::is_same_v<T, std::vector<float>>) {
      return typed.size() * sizeof(float);
    }
    return sizeof(T);
  }, value);
}

}  // namespace pangrova::cache
