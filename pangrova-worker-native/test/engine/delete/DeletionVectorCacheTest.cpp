// Tests for DeletionVectorCache — bounded LRU cache for DV bitmaps.
// Validates: Requirements R21.2, R21.5
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "engine/delete/io/DeletionVectorCache.h"

namespace pangrova::delete_filter::io::test {
namespace {

std::shared_ptr<roaring::Roaring> makeBitmap(
    std::initializer_list<uint32_t> values) {
  auto bm = std::make_shared<roaring::Roaring>();
  for (auto v : values) {
    bm->add(v);
  }
  return bm;
}

TEST(DeletionVectorCacheTest, EmptyCacheReturnsNullptr) {
  DeletionVectorCache cache(10);
  EXPECT_EQ(cache.get("nonexistent"), nullptr);
  EXPECT_EQ(cache.size(), 0u);
}

TEST(DeletionVectorCacheTest, PutAndGet) {
  DeletionVectorCache cache(10);
  auto bm = makeBitmap({1, 2, 3});
  cache.put("dv1.bin", bm);
  EXPECT_EQ(cache.size(), 1u);

  auto result = cache.get("dv1.bin");
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->contains(1));
  EXPECT_TRUE(result->contains(2));
  EXPECT_TRUE(result->contains(3));
  EXPECT_FALSE(result->contains(4));
}

TEST(DeletionVectorCacheTest, LruEviction) {
  DeletionVectorCache cache(3);
  cache.put("a", makeBitmap({1}));
  cache.put("b", makeBitmap({2}));
  cache.put("c", makeBitmap({3}));
  EXPECT_EQ(cache.size(), 3u);

  // Insert 4th entry — should evict "a" (least recently used).
  cache.put("d", makeBitmap({4}));
  EXPECT_EQ(cache.size(), 3u);
  EXPECT_EQ(cache.get("a"), nullptr);
  EXPECT_NE(cache.get("b"), nullptr);
  EXPECT_NE(cache.get("c"), nullptr);
  EXPECT_NE(cache.get("d"), nullptr);
}

TEST(DeletionVectorCacheTest, GetPromotesToMru) {
  DeletionVectorCache cache(3);
  cache.put("a", makeBitmap({1}));
  cache.put("b", makeBitmap({2}));
  cache.put("c", makeBitmap({3}));

  // Access "a" to promote it to MRU.
  cache.get("a");

  // Insert "d" — should evict "b" (now LRU), not "a".
  cache.put("d", makeBitmap({4}));
  EXPECT_NE(cache.get("a"), nullptr);
  EXPECT_EQ(cache.get("b"), nullptr);
  EXPECT_NE(cache.get("c"), nullptr);
  EXPECT_NE(cache.get("d"), nullptr);
}

TEST(DeletionVectorCacheTest, PutExistingKeyUpdatesAndPromotes) {
  DeletionVectorCache cache(3);
  cache.put("a", makeBitmap({1}));
  cache.put("b", makeBitmap({2}));
  cache.put("c", makeBitmap({3}));

  // Update "a" with new bitmap.
  cache.put("a", makeBitmap({10, 20}));
  EXPECT_EQ(cache.size(), 3u);

  auto result = cache.get("a");
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->contains(10));
  EXPECT_FALSE(result->contains(1));

  // "a" is now MRU. Insert "d" — should evict "b".
  cache.put("d", makeBitmap({4}));
  EXPECT_NE(cache.get("a"), nullptr);
  EXPECT_EQ(cache.get("b"), nullptr);
}

TEST(DeletionVectorCacheTest, Clear) {
  DeletionVectorCache cache(10);
  cache.put("a", makeBitmap({1}));
  cache.put("b", makeBitmap({2}));
  EXPECT_EQ(cache.size(), 2u);

  cache.clear();
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.get("a"), nullptr);
  EXPECT_EQ(cache.get("b"), nullptr);
}

TEST(DeletionVectorCacheTest, GetOrLoadCacheMiss) {
  DeletionVectorCache cache(10);
  int load_count = 0;

  auto result = cache.getOrLoad("dv1.bin", [&]() {
    load_count++;
    roaring::Roaring bm;
    bm.add(42);
    return bm;
  });

  EXPECT_EQ(load_count, 1);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->contains(42));
  EXPECT_EQ(cache.size(), 1u);
}

TEST(DeletionVectorCacheTest, GetOrLoadCacheHit) {
  DeletionVectorCache cache(10);
  cache.put("dv1.bin", makeBitmap({42}));
  int load_count = 0;

  auto result = cache.getOrLoad("dv1.bin", [&]() {
    load_count++;
    return roaring::Roaring();
  });

  EXPECT_EQ(load_count, 0);
  ASSERT_NE(result, nullptr);
  EXPECT_TRUE(result->contains(42));
}

TEST(DeletionVectorCacheTest, MaxEntriesDefault) {
  DeletionVectorCache cache;
  EXPECT_EQ(cache.maxEntries(), 1024u);
}

TEST(DeletionVectorCacheTest, MaxEntriesCustom) {
  DeletionVectorCache cache(256);
  EXPECT_EQ(cache.maxEntries(), 256u);
}

TEST(DeletionVectorCacheTest, ConcurrentAccess) {
  DeletionVectorCache cache(100);
  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 100;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&cache, t]() {
      for (int i = 0; i < kOpsPerThread; ++i) {
        std::string key = "dv-" + std::to_string(t) + "-" + std::to_string(i);
        auto bm = std::make_shared<roaring::Roaring>();
        bm->add(static_cast<uint32_t>(t * kOpsPerThread + i));
        cache.put(key, bm);
        cache.get(key);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  EXPECT_LE(cache.size(), 100u);
}

TEST(DeletionVectorCacheTest, SingleEntryCache) {
  DeletionVectorCache cache(1);
  cache.put("a", makeBitmap({1}));
  cache.put("b", makeBitmap({2}));
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_EQ(cache.get("a"), nullptr);
  EXPECT_NE(cache.get("b"), nullptr);
}

}  // namespace
}  // namespace pangrova::delete_filter::io::test
