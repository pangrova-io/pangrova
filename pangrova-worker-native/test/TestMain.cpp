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
#include <google/protobuf/stubs/common.h>

#include "jni/RuntimeInitializer.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  const int result = RUN_ALL_TESTS();
  // Release Velox tasks and executors before process-global registries are
  // destroyed. CTest starts one process per case, so teardown order matters.
  pangrova::jni::shutdownVeloxRuntime();
  google::protobuf::ShutdownProtobufLibrary();
  return result;
}
