#include <gtest/gtest.h>

#include <limits>

#include "jni/RuntimeInitializer.h"
#include "jni/command/FlightServerRegistry.h"
#include "shuffle/ShuffleResourceTestSupport.h"

namespace pangrova::jni::test {
namespace {

TEST(FlightServerRegistryConfigTest, NativeShuffleRequiresExplicitPrebookedServiceLimits) {
  auto& registry = FlightServerRegistry::instance();
  registry.shutdownAll();
  registry.setHmacKey(std::string(32, 'k'), "");
  registry.configureShuffleResources(NativeRuntimeConfig{});
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
  EXPECT_GT(registry.ensureShuffleServerRunning(), 0);
  registry.shutdownAll();
  registry.configureShuffleResources(NativeRuntimeConfig{});
}

}  // namespace
}  // namespace pangrova::jni::test
