// GlobalWriterRegistry: Multi-tenant registry for Java NativeWriter GlobalRefs.
// Supports concurrent writers from multiple tenants with proper isolation.
// Each tenant registers its own writer; write tasks look up by tenant_id.
// Thread-safe: sharded by tenant_id hash for reduced lock contention.
#pragma once

#include <array>
#include <atomic>
#include <jni.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace pangrova::jni {

// Multi-tenant registry for Java NativeWriter GlobalRefs.
// Supports concurrent writers from multiple tenants with proper isolation.
// Each tenant has its own writer instance; write tasks look up by tenant_id
// (extracted from task_id prefix). A default writer is available for
// single-tenant deployments.
//
// @ThreadSafe — sharded by tenant_id hash for reduced lock contention.
class GlobalWriterRegistry {
 public:
  static GlobalWriterRegistry& instance() {
    static GlobalWriterRegistry registry;
    return registry;
  }

  // Registers a writer for a specific tenant.
  // Must be called from a JNI-attached thread.
  // Replaces any previously registered writer for this tenant.
  void registerWriter(JNIEnv* env, const std::string& tenant_id, jobject writer) {
    auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.writers.find(tenant_id);
    if (it != shard.writers.end() && it->second.ref) {
      env->DeleteGlobalRef(it->second.ref);
    }
    jobject ref = writer ? env->NewGlobalRef(writer) : nullptr;
    shard.writers[tenant_id] = WriterEntry{ref, std::make_shared<std::atomic<int>>(0)};
  }

  // Registers the default writer (single-tenant or fallback).
  // Must be called from a JNI-attached thread after nativeInit.
  void registerDefaultWriter(JNIEnv* env, jobject writer) {
    std::lock_guard<std::mutex> lock(default_mutex_);
    if (default_writer_ref_) {
      env->DeleteGlobalRef(default_writer_ref_);
    }
    default_writer_ref_ = writer ? env->NewGlobalRef(writer) : nullptr;
    default_lease_guard_ = std::make_shared<std::atomic<int>>(0);
  }



  // RAII lease that prevents clear() from deleting the GlobalRef while
  // the caller uses the writer. Holds a shared_ptr to an epoch counter
  // that clear() waits on before DeleteGlobalRef.
  class WriterLease {
   public:
    WriterLease() : ref_(nullptr) {}
    explicit WriterLease(jobject ref, std::shared_ptr<std::atomic<int>> guard)
        : ref_(ref), guard_(std::move(guard)) {
      if (guard_) guard_->fetch_add(1, std::memory_order_acquire);
    }
    ~WriterLease() {
      if (guard_) guard_->fetch_sub(1, std::memory_order_release);
    }
    WriterLease(const WriterLease&) = delete;
    WriterLease& operator=(const WriterLease&) = delete;
    WriterLease(WriterLease&& o) noexcept : ref_(o.ref_), guard_(std::move(o.guard_)) { o.ref_ = nullptr; }
    WriterLease& operator=(WriterLease&& o) noexcept {
      if (this != &o) {
        if (guard_) guard_->fetch_sub(1, std::memory_order_release);
        ref_ = o.ref_; guard_ = std::move(o.guard_); o.ref_ = nullptr;
      }
      return *this;
    }
    jobject get() const { return ref_; }
    explicit operator bool() const { return ref_ != nullptr; }
   private:
    jobject ref_;
    std::shared_ptr<std::atomic<int>> guard_;
  };

  // Returns the writer for a task, resolving tenant from task_id.
  // Task IDs follow the format "tenant_id.query_id.task_id".
  // Falls back to default writer if no tenant-specific writer is found.
  // The returned WriterLease keeps the GlobalRef alive until released.
  WriterLease writerForTask(const std::string& task_id) const {
    auto tenant_id = extractTenantId(task_id);
    if (!tenant_id.empty()) {
      WriterLease lease = leaseForTenant(tenant_id);
      if (lease) return lease;
    }
    return defaultWriterLease();
  }

  // Returns the raw writer for a specific tenant (legacy, prefer writerForTask).
  jobject writerForTenant(const std::string& tenant_id) const {
    const auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.writers.find(tenant_id);
    if (it != shard.writers.end()) {
      return it->second.ref;
    }
    return nullptr;
  }

  // Returns a lease for a specific tenant's writer.
  WriterLease leaseForTenant(const std::string& tenant_id) const {
    const auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.writers.find(tenant_id);
    if (it != shard.writers.end() && it->second.ref) {
      return WriterLease(it->second.ref, it->second.lease_guard);
    }
    return WriterLease();
  }

  // Returns the default writer GlobalRef, or nullptr if not registered.
  jobject defaultWriterRef() const {
    std::lock_guard<std::mutex> lock(default_mutex_);
    return default_writer_ref_;
  }

  // Returns a lease on the default writer.
  WriterLease defaultWriterLease() const {
    std::lock_guard<std::mutex> lock(default_mutex_);
    if (!default_writer_ref_) return WriterLease();
    return WriterLease(default_writer_ref_, default_lease_guard_);
  }

  // Removes a tenant's writer. Called when tenant is deregistered.
  void removeWriter(JNIEnv* env, const std::string& tenant_id) {
    auto& shard = shardFor(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.writers.find(tenant_id);
    if (it != shard.writers.end()) {
      if (it->second.ref && env) {
        env->DeleteGlobalRef(it->second.ref);
      }
      shard.writers.erase(it);
    }
  }

  // Releases all GlobalRefs. Called during shutdown.
  // Waits for outstanding leases to be released before deleting refs.
  void clear(JNIEnv* env) {
    for (auto& shard : shards_) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      for (auto& [id, entry] : shard.writers) {
        // Spin-wait for outstanding leases (brief; drain path is bounded).
        if (entry.lease_guard) {
          while (entry.lease_guard->load(std::memory_order_acquire) > 0) {
            std::this_thread::yield();
          }
        }
        if (entry.ref && env) {
          env->DeleteGlobalRef(entry.ref);
        }
      }
      shard.writers.clear();
    }
    std::lock_guard<std::mutex> lock(default_mutex_);
    if (default_lease_guard_) {
      while (default_lease_guard_->load(std::memory_order_acquire) > 0) {
        std::this_thread::yield();
      }
    }
    if (default_writer_ref_ && env) {
      env->DeleteGlobalRef(default_writer_ref_);
      default_writer_ref_ = nullptr;
    }
  }

  // Number of registered tenant writers. For monitoring/diagnostics.
  size_t tenantCount() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
      std::lock_guard<std::mutex> lock(shard.mutex);
      total += shard.writers.size();
    }
    return total;
  }


 private:
  GlobalWriterRegistry() = default;
  ~GlobalWriterRegistry() = default;

  GlobalWriterRegistry(const GlobalWriterRegistry&) = delete;
  GlobalWriterRegistry& operator=(const GlobalWriterRegistry&) = delete;

  // Extract tenant_id from task_id (format: "tenant.query.task").
  static std::string extractTenantId(const std::string& task_id) {
    auto dot = task_id.find('.');
    if (dot == std::string::npos) return {};
    return task_id.substr(0, dot);
  }

  static constexpr size_t kNumShards = 8;

  struct WriterEntry {
    jobject ref = nullptr;
    std::shared_ptr<std::atomic<int>> lease_guard;
  };

  struct Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::string, WriterEntry> writers;
  };

  Shard& shardFor(const std::string& tenant_id) {
    auto idx = std::hash<std::string>{}(tenant_id) % kNumShards;
    return shards_[idx];
  }

  const Shard& shardFor(const std::string& tenant_id) const {
    auto idx = std::hash<std::string>{}(tenant_id) % kNumShards;
    return shards_[idx];
  }

  std::array<Shard, kNumShards> shards_;
  mutable std::mutex default_mutex_;
  jobject default_writer_ref_ = nullptr;
  std::shared_ptr<std::atomic<int>> default_lease_guard_
      = std::make_shared<std::atomic<int>>(0);
};

}  // namespace pangrova::jni
