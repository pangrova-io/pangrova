// Tests that FlightShuffleServer multiplexes all shuffle handles on a single
// worker port and rejects a competing listener on the advertised port.

#include <gtest/gtest.h>

#include "engine/shuffle/FlightShuffleServer.h"

namespace pangrova::shuffle {
namespace {

class FlightShuffleServerPortReuseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FlightShuffleServerConfig config;
    config.port = 0;  // Auto-assign port.
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

TEST_F(FlightShuffleServerPortReuseTest, RejectsSecondWorkerOnTheAdvertisedPort) {
  const int port = server_->start();
  ASSERT_GT(port, 0);

  FlightShuffleServerConfig competing_config;
  competing_config.port = port;
  FlightShuffleServer competing_server(competing_config);
  EXPECT_THROW(competing_server.start(), std::runtime_error);
  EXPECT_FALSE(competing_server.isRunning());
  EXPECT_TRUE(server_->isRunning());
}

TEST_F(FlightShuffleServerPortReuseTest, RegisterAndUnregisterHandle) {
  server_->start();

  // Create a minimal queue for testing registration.
  SerializedShuffleQueue queue(16);
  server_->registerHandle(1, &queue, "q-port", 0, 1, 1, 0, 1);
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
    server_->registerHandle(static_cast<int64_t>(i), queues.back().get(),
                            "q-port", 0, 1, 1, 0, 1);
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
  server_->registerHandle(1, &queue1, "q-port", 0, 1, 1, 0, 1);
  server_->registerHandle(2, &queue2, "q-port", 0, 1, 1, 0, 1);
  EXPECT_EQ(server_->handleCount(), 2u);

  server_->stop();
  EXPECT_FALSE(server_->isRunning());
}

TEST(FlightShuffleServerSecurityTest, PartialTlsConfigurationIsRejected) {
  FlightShuffleServerConfig config;
  config.tls_cert_chain_path = "cert.pem";
  FlightShuffleServer server(config);

  EXPECT_THROW(server.start(), std::invalid_argument);
}

TEST(FlightShuffleServerSecurityTest, UnreadableTlsCredentialsAreRejected) {
  FlightShuffleServerConfig config;
  config.tls_cert_chain_path = "/path/that/does/not/exist/cert.pem";
  config.tls_private_key_path = "/path/that/does/not/exist/key.pem";
  config.tls_root_ca_path = "/path/that/does/not/exist/ca.pem";
  FlightShuffleServer server(config);

  EXPECT_THROW(server.start(), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::shuffle
