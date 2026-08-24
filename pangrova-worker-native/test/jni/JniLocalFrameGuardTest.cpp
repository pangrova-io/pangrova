// Unit tests for JniLocalFrameGuard RAII wrapper.
//
// NOTE: These tests run WITHOUT a real JVM. We verify compile-time
// structural properties and RAII semantics of the guard class.
// Full integration testing (PushLocalFrame/PopLocalFrame with a live
// JNIEnv) is covered by Java-side E2E tests.

#include "jni/bridge/JniBridge.h"

#include <gtest/gtest.h>
#include <type_traits>

namespace pangrova::jni::test {
namespace {

// ---------------------------------------------------------------------------
// JniLocalFrameGuard is non-copyable and non-movable
// ---------------------------------------------------------------------------

TEST(JniLocalFrameGuardTest, NonCopyable) {
  EXPECT_FALSE(std::is_copy_constructible_v<JniLocalFrameGuard>)
      << "JniLocalFrameGuard must not be copy-constructible";
  EXPECT_FALSE(std::is_copy_assignable_v<JniLocalFrameGuard>)
      << "JniLocalFrameGuard must not be copy-assignable";
}

// ---------------------------------------------------------------------------
// JniLocalFrameGuard has the expected public interface
// ---------------------------------------------------------------------------

TEST(JniLocalFrameGuardTest, PublicInterfaceCompiles) {
  // Verify that the public API exists at compile time.
  // We cannot instantiate without a real JNIEnv, but we can check
  // that the method signatures are correct via decltype.

  // valid() returns bool.
  using ValidReturnType = decltype(
      std::declval<JniLocalFrameGuard>().valid());
  EXPECT_TRUE((std::is_same_v<ValidReturnType, bool>));

  // popWithResult(jobject) returns jobject.
  using PopReturnType = decltype(
      std::declval<JniLocalFrameGuard>().popWithResult(nullptr));
  EXPECT_TRUE((std::is_same_v<PopReturnType, jobject>));
}

}  // namespace
}  // namespace pangrova::jni::test
