#pragma once

#include <cstddef>
#include <functional>
#include <limits>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace pangrova::resource {

/** Explicit non-owning allocation boundary. The owner outlives every container using it. */
class OwnerMemoryResource {
 public:
  virtual ~OwnerMemoryResource() = default;
  void* allocate(size_t bytes, size_t alignment) { return do_allocate(bytes, alignment); }
  void deallocate(void* pointer, size_t bytes, size_t alignment) { do_deallocate(pointer, bytes, alignment); }
  bool is_equal(const OwnerMemoryResource& other) const noexcept { return do_is_equal(other); }
 private:
  virtual void* do_allocate(size_t bytes, size_t alignment) = 0;
  virtual void do_deallocate(void* pointer, size_t bytes, size_t alignment) = 0;
  virtual bool do_is_equal(const OwnerMemoryResource& other) const noexcept = 0;
};

/** Standard allocator with mandatory owner, including node rebinding and container copies. */
template <typename T>
class OwnerAllocator {
 public:
  using value_type = T;
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::false_type;
  using propagate_on_container_swap = std::false_type;
  using is_always_equal = std::false_type;
  template <typename U> struct rebind { using other = OwnerAllocator<U>; };

  OwnerAllocator() = delete;
  OwnerAllocator(OwnerMemoryResource* owner) : owner_(owner) {
    if (!owner_) throw std::invalid_argument("Container allocation requires an explicit live owner");
  }
  template <typename U>
  OwnerAllocator(const OwnerAllocator<U>& other) noexcept : owner_(other.resource()) {}

  T* allocate(size_t count) {
    if (count > max_size()) throw std::bad_array_new_length();
    return static_cast<T*>(owner_->allocate(count * sizeof(T), alignof(T)));
  }
  void deallocate(T* pointer, size_t count) { owner_->deallocate(pointer, count * sizeof(T), alignof(T)); }
  size_t max_size() const noexcept { return std::numeric_limits<size_t>::max() / sizeof(T); }
  OwnerMemoryResource* resource() const noexcept { return owner_; }
  // Copy construction keeps the explicit owner; it cannot silently select a global heap.
  OwnerAllocator select_on_container_copy_construction() const noexcept { return *this; }
  template <typename U>
  bool operator==(const OwnerAllocator<U>& other) const noexcept {
    return owner_ == other.resource() || owner_->is_equal(*other.resource());
  }
  template <typename U>
  bool operator!=(const OwnerAllocator<U>& other) const noexcept { return !(*this == other); }
 private:
  OwnerMemoryResource* owner_;
};

template <typename T>
using OwnerVector = std::vector<T, OwnerAllocator<T>>;
template <typename T>
using OwnerUnorderedSet = std::unordered_set<T, std::hash<T>, std::equal_to<T>, OwnerAllocator<T>>;
template <typename Key, typename Value>
using OwnerUnorderedMap = std::unordered_map<Key, Value, std::hash<Key>, std::equal_to<Key>,
    OwnerAllocator<std::pair<const Key, Value>>>;

/** Existing standalone math/pre-task path only. Live operators supply their own leaf-pool owner. */
inline OwnerMemoryResource& standaloneHeapResource() {
  class HeapResource final : public OwnerMemoryResource {
   private:
    void* do_allocate(size_t bytes, size_t alignment) override {
      if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        throw std::invalid_argument("Standalone allocation alignment must be a power of two");
      }
      return alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__ ? ::operator new(bytes)
          : ::operator new(bytes, std::align_val_t(alignment));
    }
    void do_deallocate(void* pointer, size_t, size_t alignment) override {
      if (alignment <= __STDCPP_DEFAULT_NEW_ALIGNMENT__) ::operator delete(pointer);
      else ::operator delete(pointer, std::align_val_t(alignment));
    }
    bool do_is_equal(const OwnerMemoryResource& other) const noexcept override { return this == &other; }
  };
  static HeapResource resource;
  return resource;
}

}  // namespace pangrova::resource
