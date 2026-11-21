#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace pangrova::cache {

using CachedValue = std::variant<
    bool, int32_t, int64_t, float, double, std::string, std::vector<float>>;

struct ReusePolicy final {
  uint64_t ttl_ms{0};
  uint64_t max_entries{0};
  uint64_t max_bytes{0};
  double similarity_threshold{1.0};
  uint32_t key_dimension{0};
};

struct ReuseIdentity final {
  std::string tenant_id;
  std::string principal;
  std::string policy_context_hash;
  std::string model_identity;
  std::string capability_identity;
  std::string function_name;
  std::string result_type;
  std::string parameter_fingerprint;
  std::string dependency_scope_hash;
  std::string dependency_version_hash;
};

struct TaskCacheUsage final {
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t hit_bytes{0};
  uint64_t miss_bytes{0};
  uint64_t evictions{0};
  uint64_t admission_rejections{0};
};

class SemanticResultCache final {
 public:
  static SemanticResultCache& instance();

  std::optional<CachedValue> lookup(
      const ReuseIdentity& identity,
      const std::vector<float>& request_key,
      const ReusePolicy& policy,
      const std::string& task_id);

  bool put(
      ReuseIdentity identity,
      std::vector<float> request_key,
      CachedValue value,
      const ReusePolicy& policy,
      const std::string& task_id);

  void invalidateAll();
  TaskCacheUsage taskUsage(const std::string& task_id) const;
  void releaseTaskUsage(const std::string& task_id);

  static std::vector<float> requestKey(
      const std::string& normalized_text, uint32_t dimension);
  static uint64_t valueBytes(const CachedValue& value);

 private:
  SemanticResultCache() = default;
};

}  // namespace pangrova::cache
