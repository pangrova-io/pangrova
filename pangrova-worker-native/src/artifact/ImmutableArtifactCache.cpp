// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "artifact/ImmutableArtifactCache.h"
#include "resource/TaskBudgetReadFile.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <list>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/file/File.h"

namespace pangrova::artifact {
namespace {

constexpr size_t kIoChunkBytes = 8U * 1024U * 1024U;
constexpr std::string_view kObjectSuffix = ".bin";
constexpr std::string_view kLockFile = ".pangrova-artifact-cache.lock";

class Sha256 final {
 public:
  Sha256() : context_(EVP_MD_CTX_new(), EVP_MD_CTX_free) {
    if (!context_ ||
        EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1) {
      throw std::runtime_error("Failed to initialize artifact cache SHA-256");
    }
  }

  void update(const void* data, size_t size) {
    if (EVP_DigestUpdate(context_.get(), data, size) != 1) {
      throw std::runtime_error("Failed to update artifact cache SHA-256");
    }
  }

  std::string finish() {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(context_.get(), digest.data(), &size) != 1 ||
        size != 32) {
      throw std::runtime_error("Failed to finalize artifact cache SHA-256");
    }
    return {reinterpret_cast<const char*>(digest.data()), size};
  }

 private:
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context_;
};

std::string toHex(std::string_view bytes) {
  constexpr char digits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char value : bytes) {
    result.push_back(digits[value >> 4]);
    result.push_back(digits[value & 0x0f]);
  }
  return result;
}

std::string inspectLocal(const std::filesystem::path& path, uint64_t expected) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Cannot open cached artifact: " + path.string());
  }
  Sha256 hash;
  std::string buffer(kIoChunkBytes, '\0');
  uint64_t size = 0;
  while (input) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count > 0) {
      hash.update(buffer.data(), static_cast<size_t>(count));
      size += static_cast<uint64_t>(count);
    }
  }
  if (size != expected) {
    throw std::runtime_error("Cached artifact size mismatch: " + path.string());
  }
  return hash.finish();
}

bool isLowerHex(char value) {
  return (value >= '0' && value <= '9') ||
      (value >= 'a' && value <= 'f');
}

std::string cacheKey(const std::filesystem::path& path) {
  const auto name = path.filename().string();
  if (name.size() != 64 + kObjectSuffix.size() ||
      name.compare(64, kObjectSuffix.size(), kObjectSuffix) != 0 ||
      !std::all_of(name.begin(), name.begin() + 64, isLowerHex)) {
    return {};
  }
  const auto key = name.substr(0, 64);
  if (path.parent_path().filename() != key.substr(0, 2)) {
    return {};
  }
  return key;
}

int acquireDirectoryLock(const std::filesystem::path& root) {
  const auto lock_path = root / kLockFile;
  const int descriptor = ::open(
      lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  if (descriptor < 0) {
    throw std::runtime_error(
        "Cannot open artifact cache ownership lock: " + lock_path.string());
  }
  if (::flock(descriptor, LOCK_EX | LOCK_NB) != 0) {
    ::close(descriptor);
    throw std::runtime_error(
        "Artifact cache directory is already owned by another Worker: " +
        root.string());
  }
  return descriptor;
}

void releaseDirectoryLock(int descriptor) {
  if (descriptor >= 0) {
    (void)::flock(descriptor, LOCK_UN);
    (void)::close(descriptor);
  }
}

}  // namespace

struct ImmutableArtifactCache::State {
  struct Entry {
    std::string key;
    std::string path;
    uint64_t size{0};
    bool verified{false};
    std::list<std::string>::iterator lru;
  };

  struct LoadState {
    bool complete{false};
    std::shared_ptr<Entry> entry;
    std::exception_ptr failure;
    std::condition_variable ready;
  };

  mutable std::mutex mutex;
  std::string root;
  uint64_t capacity{0};
  uint64_t used{0};
  uint64_t reserved{0};
  std::unordered_map<std::string, std::shared_ptr<Entry>> entries;
  std::unordered_map<std::string, std::shared_ptr<LoadState>> loads;
  std::list<std::string> lru;
  std::atomic<uint64_t> temp_sequence{0};
  int directory_lock{-1};
};

ImmutableArtifactCache& ImmutableArtifactCache::instance() {
  static ImmutableArtifactCache cache;
  return cache;
}

ImmutableArtifactCache::ImmutableArtifactCache()
    : state_(std::make_unique<State>()) {}

ImmutableArtifactCache::~ImmutableArtifactCache() = default;

void ImmutableArtifactCache::configure(
    std::string root_directory,
    uint64_t capacity_bytes) {
  if (root_directory.empty() || capacity_bytes == 0) {
    throw std::invalid_argument(
        "Artifact cache requires a local directory and positive capacity");
  }
  std::filesystem::path root = std::filesystem::absolute(root_directory);
  std::filesystem::create_directories(root);
  const int directory_lock = acquireDirectoryLock(root);
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (!state_->loads.empty()) {
    releaseDirectoryLock(directory_lock);
    throw std::logic_error("Cannot reconfigure artifact cache during a load");
  }
  if (!state_->root.empty() || state_->directory_lock >= 0) {
    releaseDirectoryLock(directory_lock);
    throw std::logic_error("Artifact cache is already configured");
  }
  state_->root = root.lexically_normal().string();
  state_->capacity = capacity_bytes;
  state_->used = 0;
  state_->reserved = 0;
  state_->entries.clear();
  state_->lru.clear();
  state_->directory_lock = directory_lock;

  struct WarmObject {
    std::string key;
    std::string path;
    uint64_t size;
    std::filesystem::file_time_type modified;
  };
  std::vector<WarmObject> warm_objects;
  try {
    for (const auto& item : std::filesystem::recursive_directory_iterator(root)) {
      if (!item.is_regular_file()) {
        continue;
      }
      const auto name = item.path().filename().string();
      if (name == kLockFile) {
        continue;
      }
      if (name.find(".tmp.") != std::string::npos) {
        std::filesystem::remove(item.path());
        continue;
      }
      const auto key = cacheKey(item.path());
      if (key.empty()) {
        throw std::runtime_error(
            "Artifact cache contains a non-canonical file: " +
            item.path().string());
      }
      warm_objects.push_back(WarmObject{
          key,
          item.path().string(),
          static_cast<uint64_t>(item.file_size()),
          item.last_write_time()});
    }
    std::sort(
        warm_objects.begin(), warm_objects.end(),
        [](const WarmObject& left, const WarmObject& right) {
          return left.modified > right.modified;
        });
    for (const auto& warm : warm_objects) {
      if (warm.size == 0 || warm.size > state_->capacity ||
          state_->used + warm.size > state_->capacity) {
        std::filesystem::remove(warm.path);
        continue;
      }
      auto entry = std::make_shared<State::Entry>();
      entry->key = warm.key;
      entry->path = warm.path;
      entry->size = warm.size;
      entry->verified = false;
      state_->lru.push_back(warm.key);
      entry->lru = std::prev(state_->lru.end());
      state_->entries.emplace(warm.key, std::move(entry));
      state_->used += warm.size;
    }
  } catch (...) {
    state_->entries.clear();
    state_->lru.clear();
    state_->root.clear();
    state_->capacity = 0;
    state_->used = 0;
    state_->directory_lock = -1;
    releaseDirectoryLock(directory_lock);
    throw;
  }
}

void ImmutableArtifactCache::shutdown() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (!state_->loads.empty()) {
    throw std::logic_error("Cannot stop artifact cache during a load");
  }
  state_->entries.clear();
  state_->lru.clear();
  state_->root.clear();
  state_->capacity = 0;
  state_->used = 0;
  state_->reserved = 0;
  const int directory_lock = state_->directory_lock;
  state_->directory_lock = -1;
  releaseDirectoryLock(directory_lock);
}

bool ImmutableArtifactCache::enabled() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return !state_->root.empty() && state_->capacity > 0;
}

uint64_t ImmutableArtifactCache::capacityBytes() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->capacity;
}

uint64_t ImmutableArtifactCache::usedBytes() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->used;
}

MaterializedArtifact ImmutableArtifactCache::materialize(
    const protocol::v2::ArtifactObject& object,
    uint64_t remaining_object_store_read_bytes,
    std::shared_ptr<resource::TaskObjectReadBudget> task_budget) {
  if (object.opaque_uri().empty() || object.size_bytes() == 0 ||
      !object.has_checksum() ||
      object.checksum().algorithm() !=
          protocol::v2::Checksum::ALGORITHM_SHA256 ||
      object.checksum().digest().size() != 32 ||
      !object.encryption_key_ref().empty()) {
    throw std::invalid_argument(
        "Artifact materialization requires an unencrypted immutable SHA-256 object");
  }
  const std::string expected_digest = object.checksum().digest();
  const std::string key = toHex(expected_digest);
  std::shared_ptr<State::LoadState> load;
  std::string root;
  {
    std::unique_lock<std::mutex> lock(state_->mutex);
    if (state_->root.empty() || state_->capacity == 0) {
      throw std::runtime_error("Worker immutable artifact cache is disabled");
    }
    auto hit = state_->entries.find(key);
    if (hit != state_->entries.end()) {
      if (hit->second->verified &&
          hit->second->size == object.size_bytes()) {
        state_->lru.splice(state_->lru.begin(), state_->lru, hit->second->lru);
        MaterializedArtifact result{hit->second->path, hit->second->size, 0};
        result.lease_ = hit->second;
        return result;
      }
      // Warm entries are accounted during configure(), but are not trusted
      // until the first binding supplies the canonical size and SHA-256.
      state_->used -= hit->second->size;
      state_->lru.erase(hit->second->lru);
      state_->entries.erase(hit);
    }
    auto loading = state_->loads.find(key);
    if (loading != state_->loads.end()) {
      load = loading->second;
      load->ready.wait(lock, [&] { return load->complete; });
      if (load->failure) {
        std::rethrow_exception(load->failure);
      }
      MaterializedArtifact result{load->entry->path, load->entry->size, 0};
      result.lease_ = load->entry;
      return result;
    }
    if (object.size_bytes() > state_->capacity) {
      throw std::runtime_error(
          "Artifact object exceeds cache capacity");
    }
    while (state_->used + state_->reserved + object.size_bytes() >
           state_->capacity) {
      bool evicted = false;
      for (auto it = state_->lru.rbegin(); it != state_->lru.rend(); ++it) {
        auto entry_it = state_->entries.find(*it);
        if (entry_it == state_->entries.end() ||
            entry_it->second.use_count() != 1) {
          continue;
        }
        auto victim = entry_it->second;
        std::error_code ignored;
        std::filesystem::remove(victim->path, ignored);
        state_->used -= victim->size;
        auto erase_it = std::next(it).base();
        state_->lru.erase(erase_it);
        state_->entries.erase(entry_it);
        evicted = true;
        break;
      }
      if (!evicted) {
        throw std::runtime_error(
            "Artifact cache capacity is exhausted by pinned objects");
      }
    }
    state_->reserved += object.size_bytes();
    load = std::make_shared<State::LoadState>();
    state_->loads.emplace(key, load);
    root = state_->root;
  }

  std::shared_ptr<State::Entry> loaded;
  std::exception_ptr failure;
  try {
    const auto directory = std::filesystem::path(root) / key.substr(0, 2);
    std::filesystem::create_directories(directory);
    const auto final_path = directory / (key + ".bin");
    bool downloaded = false;
    bool valid_local_object = false;
    if (std::filesystem::exists(final_path)) {
      try {
        valid_local_object =
            inspectLocal(final_path, object.size_bytes()) == expected_digest;
      } catch (const std::runtime_error&) {
        valid_local_object = false;
      }
      if (!valid_local_object) {
        std::filesystem::remove(final_path);
      }
    }
    if (!valid_local_object) {
      if (object.size_bytes() > remaining_object_store_read_bytes) {
        throw std::runtime_error(
            "Artifact cache miss exceeds task object-store read grant");
      }
      const auto sequence = state_->temp_sequence.fetch_add(1);
      const auto temp_path = directory /
          (key + ".tmp." + std::to_string(::getpid()) + "." +
           std::to_string(sequence));
      struct TempGuard {
        std::filesystem::path path;
        ~TempGuard() {
          std::error_code ignored;
          std::filesystem::remove(path, ignored);
        }
      } temp_guard{temp_path};
      auto fs = facebook::velox::filesystems::getFileSystem(
          object.opaque_uri(), nullptr);
      if (!fs) {
        throw std::runtime_error(
            "No registered filesystem for artifact URI: " + object.opaque_uri());
      }
      std::shared_ptr<facebook::velox::ReadFile> input(fs->openFileForRead(object.opaque_uri()));
      if (task_budget) {
        input = std::make_shared<resource::TaskBudgetReadFile>(
            std::move(input), task_budget, folly::CancellationToken{});
      }
      if (input->size() != object.size_bytes()) {
        throw std::runtime_error("Remote artifact object size mismatch");
      }
      std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
      if (!output) {
        throw std::runtime_error(
            "Cannot create artifact cache temporary file: " + temp_path.string());
      }
      Sha256 hash;
      std::string buffer(
          std::min<uint64_t>(kIoChunkBytes, object.size_bytes()), '\0');
      for (uint64_t offset = 0; offset < object.size_bytes();) {
        const uint64_t count = std::min<uint64_t>(
            buffer.size(), object.size_bytes() - offset);
        input->pread(offset, count, buffer.data());
        hash.update(buffer.data(), static_cast<size_t>(count));
        output.write(buffer.data(), static_cast<std::streamsize>(count));
        if (!output) {
          throw std::runtime_error("Failed writing artifact cache object");
        }
        offset += count;
      }
      output.flush();
      output.close();
      if (hash.finish() != expected_digest) {
        throw std::runtime_error("Remote artifact object SHA-256 mismatch");
      }
      std::error_code rename_error;
      std::filesystem::rename(temp_path, final_path, rename_error);
      if (rename_error) {
        if (!std::filesystem::exists(final_path) ||
            inspectLocal(final_path, object.size_bytes()) != expected_digest) {
          throw std::runtime_error(
              "Failed to atomically publish artifact cache object: " +
              rename_error.message());
        }
      }
      downloaded = true;
    }
    loaded = std::make_shared<State::Entry>();
    loaded->key = key;
    loaded->path = final_path.string();
    loaded->size = object.size_bytes();
    loaded->verified = true;
    MaterializedArtifact result{
        loaded->path, loaded->size, downloaded ? loaded->size : 0};
    result.lease_ = loaded;

    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->reserved -= object.size_bytes();
    state_->lru.push_front(key);
    loaded->lru = state_->lru.begin();
    state_->used += loaded->size;
    state_->entries.emplace(key, loaded);
    load->entry = loaded;
    load->complete = true;
    state_->loads.erase(key);
    load->ready.notify_all();
    return result;
  } catch (...) {
    failure = std::current_exception();
  }

  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->reserved -= object.size_bytes();
    load->failure = failure;
    load->complete = true;
    state_->loads.erase(key);
    load->ready.notify_all();
  }
  std::rethrow_exception(failure);
}

}  // namespace pangrova::artifact
