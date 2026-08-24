// BlockAllocationListenerTest: Verifies block size constant and documents
// the batching contract.
//
// PangrovaAllocationListener requires JNI (GlobalRef to Java AllocationListener),
// so full integration testing of the listener chain happens in E2E tests.
// These unit tests verify the compile-time contract and constants only.

#include <gtest/gtest.h>

#include "memory/BlockAllocationListener.h"

namespace pangrova::memory {
namespace {

TEST(BlockAllocationListenerTest, DefaultBlockSizeIs8MB) {
  EXPECT_EQ(BlockAllocationListener::kDefaultBlockSize, 8L * 1024 * 1024);
}

// Full listener chain integration (BlockAllocationListener → PangrovaAllocationListener
// → JNI → Java AllocationListener) is tested in NativeJniDataPathE2ETest.java
// which runs with a real JVM and real memory coordination.

}  // namespace
}  // namespace pangrova::memory
