#include <gtest/gtest.h>

#include <limits>
#include <future>
#include <thread>

#include "bridge/RuntimeInitializer.h"
#include "bridge/command/FlightServerRegistry.h"
#include "engine/shuffle/ShuffleResourceTestSupport.h"

namespace pangrova::jni::test {
namespace {

TEST(FlightServerRegistryConfigTest, NativeShuffleRequiresExplicitPrebookedServiceLimits) {
  auto& registry = FlightServerRegistry::instance();
  registry.shutdownAll();
  registry.setHmacKey(std::string(32, 'k'), "");
  registry.configureShuffleResources(NativeRuntimeConfig{});
  registry.activateShuffleRuntime();
  EXPECT_THROW(registry.ensureShuffleServerRunning(), std::runtime_error);

  NativeRuntimeConfig partial;
  partial.shuffle_retention_bytes = 16 * 1024 * 1024;
  EXPECT_THROW(registry.configureShuffleResources(partial), std::invalid_argument);
  EXPECT_THROW(registry.ensureShuffleServerRunning(), std::runtime_error);
}

TEST(FlightServerRegistryConfigTest, DedicatedSpillDirectoryAndDiskHeadroomAreMandatory) {
  auto& registry = FlightServerRegistry::instance();
  registry.shutdownAll();
  pangrova::shuffle::test::SpillDirectory directory;
  NativeRuntimeConfig config;
  config.shuffle_retention_bytes = 16 * 1024 * 1024;
  config.shuffle_transit_bytes = 16 * 1024 * 1024;
  config.shuffle_spill_bytes = 64 * 1024 * 1024;
  config.shuffle_spill_files = 32;
  config.shuffle_spill_directory = "relative-spill";
  EXPECT_THROW(registry.configureShuffleResources(config), std::invalid_argument);
  config.shuffle_spill_directory = directory.path();
  config.shuffle_spill_bytes = std::numeric_limits<uint64_t>::max();
  EXPECT_THROW(registry.configureShuffleResources(config), std::invalid_argument);
  config.shuffle_spill_bytes = 64 * 1024 * 1024;
  EXPECT_NO_THROW(registry.configureShuffleResources(config));
  registry.setHmacKey(std::string(32, 'k'), "");
  registry.activateShuffleRuntime();
  EXPECT_GT(registry.ensureShuffleServerRunning(), 0);
  registry.shutdownAll();
  registry.configureShuffleResources(NativeRuntimeConfig{});
}

TEST(FlightServerRegistryConfigTest, ActualServiceLeaseBlocksShutdownWithoutHoldingRegistryLock) {
  using namespace std::chrono_literals;
  auto& registry = FlightServerRegistry::instance();
  registry.shutdownAll();
  pangrova::shuffle::test::SpillDirectory directory;
  NativeRuntimeConfig config;
  config.shuffle_retention_bytes = 16 * 1024 * 1024;
  config.shuffle_transit_bytes = 16 * 1024 * 1024;
  config.shuffle_spill_bytes = 64 * 1024 * 1024;
  config.shuffle_spill_files = 32;
  config.shuffle_spill_directory = directory.path();
  registry.configureShuffleResources(config);
  registry.configureTransportSecurity(false, "", "", "", "");
  registry.setHmacKey(std::string(32, 'k'), "");
  registry.activateShuffleRuntime();
  pangrova::shuffle::SerializedShuffleQueue queue(1);
  auto lease = registry.registerShuffleHandle(1, &queue, "q", 0, 1, 1, 0, 1);
  ASSERT_GT(lease.bound_port, 0);
  EXPECT_EQ(lease.server->boundPort(), lease.bound_port);
  auto closing = std::async(std::launch::async, [&] { registry.shutdownShuffleServer(); });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  bool sealed = false;
  while (!sealed && std::chrono::steady_clock::now() < deadline) {
    try { auto probe = lease.server->acquireControl(); }
    catch (const std::logic_error&) { sealed = true; }
    std::this_thread::yield();
  }
  EXPECT_TRUE(sealed);
  EXPECT_EQ(closing.wait_for(0ms), std::future_status::timeout);
  // This call must reject immediately while the actual close awaits a lease.
  EXPECT_THROW(registry.ensureShuffleServerRunning(), std::logic_error);
  lease.control.reset();
  ASSERT_EQ(closing.wait_for(5s), std::future_status::ready);
  EXPECT_NO_THROW(closing.get());
  EXPECT_FALSE(lease.server->isRunning());
  EXPECT_EQ(lease.registration->state(),
            pangrova::shuffle::ShuffleProducerRegistration::State::CLOSED);
  EXPECT_TRUE(queue.isAborted());
  EXPECT_THROW(registry.ensureShuffleServerRunning(), std::logic_error);
  registry.activateShuffleRuntime();
  EXPECT_GT(registry.ensureShuffleServerRunning(), 0);
  EXPECT_THROW(lease.server->acquireControl(), std::logic_error);
  registry.shutdownAll();
  registry.configureShuffleResources(NativeRuntimeConfig{});
}

}  // namespace
}  // namespace pangrova::jni::test
