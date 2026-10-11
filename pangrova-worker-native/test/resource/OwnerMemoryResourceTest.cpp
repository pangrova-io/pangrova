#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "../TestMemoryHelper.h"
#include "resource/OwnerMemoryResource.h"
#include "velox/common/base/Exceptions.h"
#include "velox/plan/OperatorMemoryResource.h"

namespace pangrova::resource::test {
namespace {
static_assert(!std::is_default_constructible_v<OwnerAllocator<uint64_t>>);
static_assert(!std::allocator_traits<OwnerAllocator<uint64_t>>::propagate_on_container_copy_assignment::value);
static_assert(!std::allocator_traits<OwnerAllocator<uint64_t>>::propagate_on_container_move_assignment::value);
static_assert(!std::allocator_traits<OwnerAllocator<uint64_t>>::propagate_on_container_swap::value);
static_assert(!std::allocator_traits<OwnerAllocator<uint64_t>>::is_always_equal::value);

TEST(OwnerMemoryResourceTest, ActualLeafPoolChargesVectorAndReboundHashNodesAndReleasesOnUnwind) {
  auto root = pangrova::test::testRootPool("owner-allocation-unwind-root");
  auto pool = root->addLeafChild("owner-allocation-unwind");
  pangrova::velox::OperatorMemoryResource owner(pool.get());
  EXPECT_EQ(pool->usedBytes(), 0);
  try {
    OwnerVector<uint64_t> vector(&owner);
    vector.resize(128, 19);
    const auto vector_bytes = pool->usedBytes();
    EXPECT_GE(vector_bytes, 128 * static_cast<int64_t>(sizeof(uint64_t)));
    OwnerUnorderedMap<uint64_t, uint64_t> map(&owner);
    OwnerUnorderedSet<uint64_t> set(&owner);
    for (uint64_t index = 0; index < 128; ++index) {
      map.emplace(index, index * 2);
      set.emplace(index);
    }
    EXPECT_GT(pool->usedBytes(), vector_bytes);
    EXPECT_EQ(map.at(93), 186);
    EXPECT_TRUE(set.contains(93));
    EXPECT_EQ(vector.get_allocator().resource(), &owner);
    EXPECT_EQ(map.get_allocator().resource(), &owner);
    EXPECT_EQ(set.get_allocator().resource(), &owner);
    throw std::runtime_error("real owning-container unwind");
  } catch (const std::runtime_error& error) {
    EXPECT_STREQ(error.what(), "real owning-container unwind");
  }
  EXPECT_EQ(pool->usedBytes(), 0);
}

TEST(OwnerMemoryResourceTest, CopiesAndAssignmentsUseExplicitOwnersAndSameOwnerSwapIsSafe) {
  auto first_root = pangrova::test::testRootPool("owner-propagation-first-root");
  auto second_root = pangrova::test::testRootPool("owner-propagation-second-root");
  auto first_pool = first_root->addLeafChild("owner-propagation-first");
  auto second_pool = second_root->addLeafChild("owner-propagation-second");
  pangrova::velox::OperatorMemoryResource first(first_pool.get());
  pangrova::velox::OperatorMemoryResource second(second_pool.get());
  {
    OwnerVector<int64_t> source(64, 7, &first);
    OwnerVector<int64_t> copied(source);
    EXPECT_EQ(copied.get_allocator().resource(), &first);
    EXPECT_GE(first_pool->usedBytes(), 128 * static_cast<int64_t>(sizeof(int64_t)));
    OwnerVector<int64_t> assigned(32, 9, &second);
    EXPECT_GT(second_pool->usedBytes(), 0);
    assigned = source;
    EXPECT_EQ(assigned.get_allocator().resource(), &second);
    EXPECT_GE(second_pool->usedBytes(), 64 * static_cast<int64_t>(sizeof(int64_t)));
    OwnerVector<int64_t> moved(16, 11, &second);
    moved = std::move(copied);
    EXPECT_EQ(moved.get_allocator().resource(), &second);
    EXPECT_EQ(moved.size(), 64);
    EXPECT_EQ(moved.front(), 7);
    OwnerVector<int64_t> other(17, 13, &second);
    assigned.swap(other);  // Unequal non-propagating owners must never be swapped.
    EXPECT_EQ(assigned.get_allocator().resource(), &second);
    EXPECT_EQ(other.get_allocator().resource(), &second);
    EXPECT_EQ(assigned.front(), 13);
    EXPECT_EQ(other.front(), 7);
    OwnerVector<int64_t> moved_source(std::move(source));
    EXPECT_EQ(moved_source.get_allocator().resource(), &first);
    EXPECT_EQ(moved_source.front(), 7);
    EXPECT_TRUE(OwnerAllocator<int64_t>(&first) == OwnerAllocator<uint8_t>(&first));
    EXPECT_FALSE(OwnerAllocator<int64_t>(&first) == OwnerAllocator<int64_t>(&second));
    using Rebound = std::allocator_traits<OwnerAllocator<uint8_t>>::rebind_alloc<uint64_t>;
    Rebound rebound{OwnerAllocator<uint8_t>(&first)};
    EXPECT_EQ(rebound.resource(), &first);
  }
  EXPECT_EQ(first_pool->usedBytes(), 0);
  EXPECT_EQ(second_pool->usedBytes(), 0);
}

TEST(OwnerMemoryResourceTest, OwnerIsMandatoryAndOverflowCannotAllocateOrAlterAccounting) {
  EXPECT_THROW(OwnerAllocator<uint64_t>(nullptr), std::invalid_argument);
  EXPECT_THROW(pangrova::velox::OperatorMemoryResource(nullptr), std::invalid_argument);
  auto root = pangrova::test::testRootPool("owner-allocation-overflow-root");
  auto pool = root->addLeafChild("owner-allocation-overflow");
  pangrova::velox::OperatorMemoryResource owner(pool.get());
  OwnerAllocator<uint64_t> allocator(&owner);
  EXPECT_THROW(allocator.allocate(allocator.max_size() + 1), std::bad_array_new_length);
  EXPECT_EQ(pool->usedBytes(), 0);
  struct alignas(64) AlignedValue { uint64_t value; };
  ASSERT_GE(pool->alignment(), alignof(AlignedValue));
  {
    OwnerVector<AlignedValue> aligned(3, &owner);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(aligned.data()) % alignof(AlignedValue), 0);
    EXPECT_GE(pool->usedBytes(), 3 * static_cast<int64_t>(sizeof(AlignedValue)));
  }
  EXPECT_EQ(pool->usedBytes(), 0);
}

TEST(OwnerMemoryResourceTest, ActualRootCapacityFailurePreservesResidentValuesAndCharges) {
  auto root = pangrova::test::testMemoryManager()->addRootPool("owner-capacity-root", 1 << 20);
  auto leaf = root->addLeafChild("owner-capacity-leaf");
  pangrova::velox::OperatorMemoryResource owner(leaf.get());
  {
    OwnerVector<uint64_t> resident(16, 23, &owner);
    const auto before = leaf->usedBytes();
    bool rejected = false;
    try {
      resident.resize((2 << 20) / sizeof(uint64_t), 29);
    } catch (const facebook::velox::VeloxRuntimeError& failure) {
      rejected = true;
      EXPECT_EQ(failure.errorCode(), facebook::velox::error_code::kMemCapExceeded);
    }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(resident.size(), 16);
    EXPECT_EQ(resident.front(), 23);
    EXPECT_EQ(leaf->usedBytes(), before);
  }
  EXPECT_EQ(leaf->usedBytes(), 0);
}

TEST(OwnerMemoryResourceTest, StandaloneHeapOwnerIsExplicitAndSupportsOverAlignment) {
  auto& owner = standaloneHeapResource();
  struct alignas(256) AlignedValue { uint64_t value; };
  OwnerVector<AlignedValue> values(3, &owner);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(values.data()) % alignof(AlignedValue), 0);
  EXPECT_EQ(values.get_allocator().resource(), &owner);
}
}  // namespace
}  // namespace pangrova::resource::test
