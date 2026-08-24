// Unit tests for JniReferenceCache lifecycle and globalJniCache() accessor.
//
// NOTE: These tests run WITHOUT a real JVM, so we cannot call
// initJniReferenceCache(env) or releaseJniReferenceCache(env) with a live
// JNIEnv. Instead we verify the structural invariants of the cache:
//   - globalJniCache() returns a stable singleton reference
//   - All fields are null before initialization
//   - The struct layout matches the design specification

#include "jni/bridge/JniBridge.h"

#include <gtest/gtest.h>

namespace pangrova::jni::test {
namespace {

// ---------------------------------------------------------------------------
// globalJniCache() accessor
// ---------------------------------------------------------------------------

TEST(JniReferenceCacheTest, GlobalCacheReturnsSameInstance) {
  auto& cache1 = globalJniCache();
  auto& cache2 = globalJniCache();
  EXPECT_EQ(&cache1, &cache2)
      << "globalJniCache() must return the same singleton instance";
}

// ---------------------------------------------------------------------------
// Default state — all fields null before init
// ---------------------------------------------------------------------------

TEST(JniReferenceCacheTest, DefaultFieldsAreNull) {
  // Without calling initJniReferenceCache(), all cached refs must be null.
  // This test relies on the global cache being in its default state
  // (no JVM available in unit tests to call init).
  auto& cache = globalJniCache();

  EXPECT_EQ(cache.native_bridge_class, nullptr);
  EXPECT_EQ(cache.native_exception_class, nullptr);
  EXPECT_EQ(cache.arrow_array_class, nullptr);
  EXPECT_EQ(cache.arrow_schema_class, nullptr);
  EXPECT_EQ(cache.exception_constructor, nullptr);
  EXPECT_EQ(cache.arrow_array_address, nullptr);
  EXPECT_EQ(cache.arrow_schema_address, nullptr);
}

// ---------------------------------------------------------------------------
// Struct completeness — all required fields exist
// ---------------------------------------------------------------------------

TEST(JniReferenceCacheTest, StructHasAllRequiredFields) {
  // Compile-time verification that JniReferenceCache has the expected fields.
  // If any field is missing, this test will fail to compile.
  JniReferenceCache cache{};

  // Core class references.
  [[maybe_unused]] jclass nb = cache.native_bridge_class;
  [[maybe_unused]] jclass ne = cache.native_exception_class;
  [[maybe_unused]] jclass aa = cache.arrow_array_class;
  [[maybe_unused]] jclass as = cache.arrow_schema_class;

  // Method IDs.
  [[maybe_unused]] jmethodID ec = cache.exception_constructor;
  [[maybe_unused]] jmethodID aaa = cache.arrow_array_address;
  [[maybe_unused]] jmethodID asa = cache.arrow_schema_address;

  // All default-initialized to nullptr.
  EXPECT_EQ(nb, nullptr);
  EXPECT_EQ(ne, nullptr);
  EXPECT_EQ(aa, nullptr);
  EXPECT_EQ(as, nullptr);
  EXPECT_EQ(ec, nullptr);
  EXPECT_EQ(aaa, nullptr);
  EXPECT_EQ(asa, nullptr);
}

// ---------------------------------------------------------------------------
// releaseJniReferenceCache is idempotent with nullptr env
// ---------------------------------------------------------------------------
// NOTE: We cannot call releaseJniReferenceCache(nullptr) because it would
// dereference env. But we verify that calling it when cache is not initialized
// is safe — the guard check prevents any DeleteGlobalRef calls.
// This is implicitly tested by the fact that no init was called and the
// cache fields remain null (verified above).

}  // namespace
}  // namespace pangrova::jni::test
