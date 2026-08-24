// Custom test main that initializes Velox runtime before running GoogleTest.
//
// Velox requires folly::Init (which initializes glog, gflags, and folly
// internals) before any Velox API can be used.
//
// MemoryManager initialization is handled by TestMemoryHelper.h's
// ensureRuntimeInitialized() — called via static initializers in each
// test file that needs it. This avoids double-initialization conflicts.

#include <gtest/gtest.h>
#include <folly/init/Init.h>

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
