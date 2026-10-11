#pragma once

#include <limits>
#include <stdexcept>

#include "resource/OwnerMemoryResource.h"

#include "velox/common/memory/MemoryPool.h"

namespace pangrova::velox {

/** Owner-allocated storage charged to the real live operator's leaf pool. */
class OperatorMemoryResource final : public pangrova::resource::OwnerMemoryResource {
 public:
  explicit OperatorMemoryResource(facebook::velox::memory::MemoryPool* pool)
      : pool_(pool) {
    if (!pool_) {
      throw std::invalid_argument("Operator memory resource requires its pool");
    }
  }

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
        alignment > std::numeric_limits<uint32_t>::max()) {
      throw std::bad_alloc();
    }
    return pool_->allocate(static_cast<int64_t>(bytes),
                           static_cast<uint32_t>(alignment));
  }
  void do_deallocate(void* pointer, size_t bytes, size_t) override {
    pool_->free(pointer, static_cast<int64_t>(bytes));
  }
  bool do_is_equal(const pangrova::resource::OwnerMemoryResource& other) const noexcept override {
    return this == &other;
  }
  facebook::velox::memory::MemoryPool* const pool_;
};

}  // namespace pangrova::velox
