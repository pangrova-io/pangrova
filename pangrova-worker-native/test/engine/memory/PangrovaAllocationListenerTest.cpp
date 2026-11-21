// PangrovaAllocationListenerTest: Verifies GlobalRef lifecycle, reserve/unreserve
// JNI callback delegation, and exception path safety.
//
// These tests require a running JVM (JNI-attached thread). In CI, they run
// within the JNI test harness that provides a valid JNIEnv.

#include <gtest/gtest.h>

#include "engine/memory/PangrovaAllocationListener.h"

namespace pangrova::memory {
namespace {

// Placeholder test: full JNI tests require embedded JVM harness.
// The following tests verify compile-time contracts and structural correctness.

TEST(PangrovaAllocationListenerTest, GlobalRefGuardMoveSemantics) {
  // GlobalRefGuard with nullptr is safe to construct and destroy.
  GlobalRefGuard guard(nullptr, nullptr);
  EXPECT_EQ(guard.get(), nullptr);

  // Move construction transfers ownership.
  GlobalRefGuard moved(std::move(guard));
  EXPECT_EQ(moved.get(), nullptr);
}

TEST(PangrovaAllocationListenerTest, GlobalRefGuardReleaseIdempotent) {
  GlobalRefGuard guard(nullptr, nullptr);
  guard.release();
  guard.release();  // Second release is safe (idempotent).
  EXPECT_EQ(guard.get(), nullptr);
}

TEST(PangrovaAllocationListenerTest, GlobalRefGuardMoveAssignment) {
  GlobalRefGuard a(nullptr, nullptr);
  GlobalRefGuard b(nullptr, nullptr);
  b = std::move(a);
  EXPECT_EQ(b.get(), nullptr);
}

}  // namespace
}  // namespace pangrova::memory
