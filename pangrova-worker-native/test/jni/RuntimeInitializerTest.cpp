// Tests for RuntimeInitializer — native runtime lifecycle management.
//
// Validates config parsing, runtime initialization (VeloxMemoryGovernor,
// AsyncDataCache, UDF registry), idempotency, and shutdown cleanup.
// All tests run without a real JVM — they exercise the C++ runtime only.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <folly/String.h>

#include "jni/RuntimeInitializer.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "engine/velox/connector/PangrovaVeloxConnector.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/DecodedVector.h"

namespace pangrova::jni::test {

namespace {

class RuntimeInitializerTest : public ::testing::Test {
 protected:
  void enableTestArtifactCache(NativeRuntimeConfig& config) {
    auto path = (std::filesystem::temp_directory_path() /
                 "pangrova-runtime-artifact-cache-XXXXXX").string();
    if (!::mkdtemp(path.data())) {
      throw std::runtime_error("Cannot create owned runtime artifact cache directory");
    }
    artifact_cache_path_ = std::move(path);
    config.cache_enabled = true;
    config.artifact_cache_path = artifact_cache_path_;
    config.artifact_cache_bytes = 64 * 1024 * 1024;
    config.artifact_index_memory_bytes = 16 * 1024 * 1024;
  }

  void TearDown() override {
    shutdownVeloxRuntime();
    if (!artifact_cache_path_.empty()) {
      std::error_code error;
      std::filesystem::remove_all(artifact_cache_path_, error);
      EXPECT_FALSE(error) << error.message();
    }
  }

 private:
  std::string artifact_cache_path_;
};

// --- Config parsing ---

TEST_F(RuntimeInitializerTest, EmptyConfigReturnsDefaults) {
  auto config = parseNativeConfig({});
  EXPECT_EQ(config.max_memory_bytes, 8L * 1024 * 1024 * 1024);
  EXPECT_EQ(config.io_thread_count, 4);
  EXPECT_FALSE(config.cache_enabled);
}

TEST_F(RuntimeInitializerTest, GarbageConfigIsRejected) {
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(parseNativeConfig(garbage), std::invalid_argument);
}

TEST_F(RuntimeInitializerTest, ParsesCanonicalV2RuntimeConfig) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  proto.set_max_memory_bytes(1024);
  proto.set_io_thread_count(7);
  proto.set_spill_enabled(true);
  proto.set_spill_directory("/tmp/pangrova-spill-test");
  proto.set_shuffle_retention_bytes(16 * 1024 * 1024);
  proto.set_shuffle_transit_bytes(16 * 1024 * 1024);
  proto.set_shuffle_spill_bytes(1024 * 1024 * 1024);
  proto.set_shuffle_spill_files(256);
  proto.set_shuffle_spill_directory("/var/lib/pangrova/shuffle");
  const auto bytes = proto.SerializeAsString();

  auto config = parseNativeConfig(
      std::vector<uint8_t>(bytes.begin(), bytes.end()));

  EXPECT_EQ(config.max_memory_bytes, 1024);
  EXPECT_EQ(config.io_thread_count, 7);
  EXPECT_TRUE(config.spill_enabled);
  EXPECT_EQ(config.spill_directory, "/tmp/pangrova-spill-test");
  EXPECT_EQ(config.shuffle_retention_bytes, 16u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_transit_bytes, 16u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_spill_bytes, 1024u * 1024 * 1024);
  EXPECT_EQ(config.shuffle_spill_files, 256u);
  EXPECT_EQ(config.shuffle_spill_directory, "/var/lib/pangrova/shuffle");
}

TEST_F(RuntimeInitializerTest, PartialTransportTlsConfigIsRejected) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  auto* security = proto.mutable_transport_security();
  security->set_tls_enabled(true);
  security->set_certificate_chain_path("cert.pem");
  const auto bytes = proto.SerializeAsString();

  EXPECT_THROW(
      parseNativeConfig(std::vector<uint8_t>(bytes.begin(), bytes.end())),
      std::invalid_argument);
}

TEST_F(RuntimeInitializerTest, CompleteTransportTlsConfigIsPreserved) {
  pangrova::protocol::v2::RuntimeInitConfig proto;
  auto* security = proto.mutable_transport_security();
  security->set_tls_enabled(true);
  security->set_certificate_chain_path("cert.pem");
  security->set_private_key_path("key.pem");
  security->set_root_ca_path("ca.pem");
  security->set_tls_identity("worker-1.internal");
  const auto bytes = proto.SerializeAsString();

  auto config = parseNativeConfig(
      std::vector<uint8_t>(bytes.begin(), bytes.end()));

  EXPECT_TRUE(config.transport_tls_enabled);
  EXPECT_EQ(config.transport_tls_certificate_chain_path, "cert.pem");
  EXPECT_EQ(config.transport_tls_private_key_path, "key.pem");
  EXPECT_EQ(config.transport_tls_root_ca_path, "ca.pem");
  EXPECT_EQ(config.transport_tls_identity, "worker-1.internal");
}

// --- Runtime initialization ---

TEST_F(RuntimeInitializerTest, InitCreatesMemoryBridge) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
}

TEST_F(RuntimeInitializerTest, CacheDisabledByDefault) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);

  // AsyncDataCache is null when cache is disabled.
  EXPECT_EQ(globalAsyncDataCache(), nullptr);
}

TEST_F(RuntimeInitializerTest, CacheEnabledCreatesAsyncDataCache) {
  NativeRuntimeConfig config;
  enableTestArtifactCache(config);
  initializeVeloxRuntime(config);

  EXPECT_NE(globalAsyncDataCache(), nullptr);
}

TEST_F(RuntimeInitializerTest, CachedPhysicalFilesRetainDistinctLiveIdentifiersAndValues) {
  using namespace facebook::velox;
  NativeRuntimeConfig config;
  enableTestArtifactCache(config);
  config.max_memory_bytes = 1024L * 1024 * 1024;
  initializeVeloxRuntime(config);
  auto root = facebook::velox::memory::MemoryManager::getInstance()->addRootPool("cached-file-test");
  auto pool = root->addLeafChild("scan");
  const auto type = ROW({"value"}, {BIGINT()});
  const auto first = config.artifact_cache_path + "/first.parquet";
  const auto second = config.artifact_cache_path + "/second.parquet";
  // Apache Parquet 1.17.1 ExampleParquetWriter; required int64 value,
  // UNCOMPRESSED, rows {11,13,17} and {101,103,107}. These are actual SDK
  // files, not a second format encoder or a simulated reader.
  auto write = [](const std::string& path, const std::string& hex) {
    const auto bytes = folly::unhexlify<std::string>(hex);
    std::ofstream output(path, std::ios::binary);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    ASSERT_TRUE(output);
  };
  write(first,
      "5041523115001530153015cbceb6f70d1c150615001508150800000b000000000000000d000000000000001100000000000000"
      "1911021918080b000000000000001918081100000000000000150219160000191c1608155e160000001502192c480b63616368"
      "65645f66696c6515020015042500180576616c7565001606191c191c26001c15041925080019180576616c756515001606165e"
      "165e26083c1808110000000000000018080b0000000000000016002808110000000000000018080b0000000000000000191c"
      "150015001502003c000016a40115141666153e00165e16062608165e140000191c18117772697465722e6d6f64656c2e6e616d"
      "6518076578616d706c6500184a706172717565742d6d722076657273696f6e20312e31372e3120286275696c64203738613864"
      "333233306562343736396462393364653566326632653138333633633034636165383129191c1c000000ff00000050415231");
  write(second,
      "5041523115001530153015ef8df0d2041c15061500150815080000650000000000000067000000000000006b00000000000000"
      "19110219180865000000000000001918086b00000000000000150219160000191c1608155e160000001502192c480b63616368"
      "65645f66696c6515020015042500180576616c7565001606191c191c26001c15041925080019180576616c756515001606165e"
      "165e26083c18086b0000000000000018086500000000000000160028086b000000000000001808650000000000000000191c"
      "150015001502003c000016a40115141666153e00165e16062608165e140000191c18117772697465722e6d6f64656c2e6e616d"
      "6518076578616d706c6500184a706172717565742d6d722076657273696f6e20312e31372e3120286275696c64203738613864"
      "333233306562343736396462393364653566326632653138333633633034636165383129191c1c000000ff00000050415231");
  auto scan = [&](const std::string& path, const std::vector<int64_t>& expected) {
    pangrova::velox::PangrovaVeloxDataSource source(pool.get(), type, type);
    pangrova::split::SplitConfig split;
    split.file_path = path;
    split.file_format = pangrova::split::FileFormatType::PARQUET;
    source.addSplit(std::make_shared<pangrova::velox::PangrovaConnectorSplit>("iceberg", split));
    ASSERT_NE(fileIds().id(path), StringIdMap::kNoId);
    std::vector<int64_t> actual;
    ContinueFuture future;
    while (true) {
      auto batch = source.next(16, future);
      if (!batch.has_value()) {
        ASSERT_TRUE(future.valid());
        std::move(future).get();
        continue;
      }
      if (!*batch) break;
      DecodedVector values(*(*batch)->childAt(0));
      for (vector_size_t row = 0; row < (*batch)->size(); ++row) {
        ASSERT_FALSE(values.isNullAt(row));
        actual.push_back(values.valueAt<int64_t>(row));
      }
    }
    EXPECT_EQ(actual, expected);
  };
  scan(first, {11, 13, 17});
  scan(second, {101, 103, 107});
  EXPECT_NE(fileIds().id(first), fileIds().id(second));
  const auto hits = globalAsyncDataCache()->refreshStats().numHit;
  scan(first, {11, 13, 17});
  scan(second, {101, 103, 107});
  EXPECT_GT(globalAsyncDataCache()->refreshStats().numHit, hits);
}

// --- Shutdown ---

TEST_F(RuntimeInitializerTest, ShutdownCleansAllState) {
  NativeRuntimeConfig config;
  enableTestArtifactCache(config);
  initializeVeloxRuntime(config);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
  EXPECT_NE(globalAsyncDataCache(), nullptr);

  shutdownVeloxRuntime();

  EXPECT_EQ(globalVeloxMemoryGovernor(), nullptr);
  EXPECT_EQ(globalAsyncDataCache(), nullptr);
}

TEST_F(RuntimeInitializerTest, ReinitAfterShutdownWorks) {
  NativeRuntimeConfig config;
  initializeVeloxRuntime(config);
  shutdownVeloxRuntime();

  NativeRuntimeConfig config2;
  config2.max_memory_bytes = 1L * 1024 * 1024 * 1024;
  initializeVeloxRuntime(config2);

  EXPECT_NE(globalVeloxMemoryGovernor(), nullptr);
}

}  // namespace
}  // namespace pangrova::jni::test
