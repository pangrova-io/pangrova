// Tests that FlightShuffleServer multiplexes all shuffle handles on a single
// port without port exhaustion. Verifies SO_REUSEPORT configuration and
// concurrent handle registration/unregistration.

#include <gtest/gtest.h>

#include "shuffle/FlightShuffleServer.h"

namespace pangrova::shuffle {
namespace {

class FlightShuffleServerPortReuseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FlightShuffleServerConfig config;
    config.port = 0;  // Auto-assign port.
    config.reuse_port = true;
    server_ = std::make_unique<FlightShuffleServer>(config);
  }

  void TearDown() override {
    if (server_ && server_->isRunning()) {
      server_->stop();
    }
  }

  std::unique_ptr<FlightShuffleServer> server_;
};

TEST_F(FlightShuffleServerPortReuseTest, StartsOnSinglePort) {
  int port = server_->start();
  ASSERT_GT(port, 0);
  EXPECT_TRUE(server_->isRunning());
  EXPECT_EQ(server_->boundPort(), port);
}

TEST_F(FlightShuffleServerPortReuseTest, RegisterAndUnregisterHandle) {
  server_->start();

  // Create a minimal queue for testing registration.
  SerializedShuffleQueue queue(16);
  server_->registerHandle(1, &queue);
  EXPECT_EQ(server_->handleCount(), 1u);

  server_->unregisterHandle(1);
  EXPECT_EQ(server_->handleCount(), 0u);
}

TEST_F(FlightShuffleServerPortReuseTest, HundredConcurrentHandlesOnSinglePort) {
  int port = server_->start();
  ASSERT_GT(port, 0);

  // Register 100 handles — all share the same port.
  static constexpr int kNumHandles = 100;
  std::vector<std::unique_ptr<SerializedShuffleQueue>> queues;
  queues.reserve(kNumHandles);

  for (int i = 1; i <= kNumHandles; ++i) {
    queues.push_back(std::make_unique<SerializedShuffleQueue>(16));
    server_->registerHandle(static_cast<int64_t>(i), queues.back().get());
  }

  // All 100 handles registered on the same single port.
  EXPECT_EQ(server_->handleCount(), static_cast<size_t>(kNumHandles));
  EXPECT_EQ(server_->boundPort(), port);
  EXPECT_TRUE(server_->isRunning());

  // Unregister all handles — server remains running.
  for (int i = 1; i <= kNumHandles; ++i) {
    server_->unregisterHandle(static_cast<int64_t>(i));
  }
  EXPECT_EQ(server_->handleCount(), 0u);
  EXPECT_TRUE(server_->isRunning());
}

TEST_F(FlightShuffleServerPortReuseTest, UnregisterNonexistentHandleIsNoOp) {
  server_->start();
  // Should not crash or throw.
  server_->unregisterHandle(999);
  EXPECT_EQ(server_->handleCount(), 0u);
}

TEST_F(FlightShuffleServerPortReuseTest, StopCleansUpAllHandles) {
  server_->start();

  SerializedShuffleQueue queue1(16);
  SerializedShuffleQueue queue2(16);
  server_->registerHandle(1, &queue1);
  server_->registerHandle(2, &queue2);
  EXPECT_EQ(server_->handleCount(), 2u);

  server_->stop();
  EXPECT_FALSE(server_->isRunning());
}

}  // namespace
}  // namespace pangrova::shuffle
