// GlobalEgressRegistry: Multi-tenant registry for Java ShuffleByteEgress GlobalRefs.
// Supports concurrent egress callbacks from multiple executor handles.
// Thread-safe: sharded by handle hash for reduced lock contention.
//
// This is the egress counterpart to GlobalWriterRegistry — while the writer
// registry routes WRITE task callbacks, this registry routes shuffle byte
// egress callbacks for JVM-resident shuffle providers (local-spill, object-store,
// celeborn). The "native" provider never uses this path.
//
// Placement: jni/bridge/ — JNI infrastructure.
#pragma once

#include <array>
#include <jni.h>
#include <mutex>
#include <string>
#include <unordered_map>

namespace pangrova::jni {

// Multi-tenant registry for Java ShuffleByteEgress GlobalRefs.
// Each worker registers a single egress instance (or per-tenant in multi-tenant
// deployments). Shuffle egress tasks look up the egress by query_id prefix
// (tenant routing) or fall back to the default egress.
//
// @ThreadSafe — sharded by key hash for reduced lock contention.
class GlobalEgressRegistry {
 public:
  static GlobalEgressRegistry& instance() {
    static GlobalEgressRegistry registry;
    return registry;
  }

  // Registers the default egress (single-tenant or fallback).
  // Must be called from a JNI-attached thread after nativeInit.
  void registerDefaultEgress(JNIEnv* env, jobject egress) {
    std::lock_guard<std::mutex> lock(default_mutex_);
    if (default_egress_ref_) {
      env->DeleteGlobalRef(default_egress_ref_);
    }
    default_egress_ref_ = egress ? env->NewGlobalRef(egress) : nullptr;
  }

  // Registers an egress for a specific tenant.
  void registerEgress(JNIEnv* env, const std::string& tenant_id, jobject egress) {
    auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.egresses.find(tenant_id);
    if (it != shard.egresses.end() && it->second) {
      env->DeleteGlobalRef(it->second);
    }
    shard.egresses[tenant_id] = egress ? env->NewGlobalRef(egress) : nullptr;
  }

  // Returns the egress for a specific tenant, or nullptr if not registered.
  jobject egressForTenant(const std::string& tenant_id) const {
    const auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.egresses.find(tenant_id);
    if (it != shard.egresses.end()) {
      return it->second;
    }
    return nullptr;
  }

  // Returns the egress for a query, resolving tenant from query_id.
  // Query IDs follow the format "tenant_id.query_id".
  // Falls back to default egress if no tenant-specific egress is found.
  jobject egressForQuery(const std::string& query_id) const {
    auto tenant_id = extractTenantId(query_id);
    if (!tenant_id.empty()) {
      jobject ref = egressForTenant(tenant_id);
      if (ref) return ref;
    }
    return defaultEgressRef();
  }

  // Returns the default egress GlobalRef, or nullptr if not registered.
  jobject defaultEgressRef() const {
    std::lock_guard<std::mutex> lock(default_mutex_);
    return default_egress_ref_;
  }

  // Returns true if an egress is available (default or any tenant-specific).
  bool hasEgress() const {
    if (defaultEgressRef()) return true;
    for (const auto& shard : shards_) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      if (!shard.egresses.empty()) return true;
    }
    return false;
  }

  // Removes a tenant's egress. Called when tenant is deregistered.
  void removeEgress(JNIEnv* env, const std::string& tenant_id) {
    auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.egresses.find(tenant_id);
    if (it != shard.egresses.end()) {
      if (it->second && env) {
        env->DeleteGlobalRef(it->second);
      }
      shard.egresses.erase(it);
    }
  }

  // Releases all GlobalRefs. Called during shutdown.
  void clear(JNIEnv* env) {
    for (auto& shard : shards_) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      for (auto& [id, ref] : shard.egresses) {
        if (ref && env) {
          env->DeleteGlobalRef(ref);
        }
      }
      shard.egresses.clear();
    }
    std::lock_guard<std::mutex> lock(default_mutex_);
    if (default_egress_ref_ && env) {
      env->DeleteGlobalRef(default_egress_ref_);
      default_egress_ref_ = nullptr;
    }
  }

  // Number of registered tenant egresses. For monitoring/diagnostics.
  size_t tenantCount() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      total += shard.egresses.size();
    }
    return total;
  }

 private:
  GlobalEgressRegistry() = default;
  ~GlobalEgressRegistry() = default;

  GlobalEgressRegistry(const GlobalEgressRegistry&) = delete;
  GlobalEgressRegistry& operator=(const GlobalEgressRegistry&) = delete;

  // Extract tenant_id from query_id (format: "tenant.query").
  static std::string extractTenantId(const std::string& query_id) {
    auto dot = query_id.find('.');
    if (dot == std::string::npos) return {};
    return query_id.substr(0, dot);
  }

  static constexpr size_t kNumShards = 8;

  struct Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::string, jobject> egresses;
  };

  Shard& shardFor(const std::string& key) {
    auto idx = std::hash<std::string>{}(key) % kNumShards;
    return shards_[idx];
  }

  const Shard& shardFor(const std::string& key) const {
    auto idx = std::hash<std::string>{}(key) % kNumShards;
    return shards_[idx];
  }

  std::array<Shard, kNumShards> shards_;
  mutable std::mutex default_mutex_;
  jobject default_egress_ref_ = nullptr;
};

}  // namespace pangrova::jni
