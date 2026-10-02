#include <gtest/gtest.h>

#include <openssl/evp.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "artifact/ImmutableArtifactCache.h"

namespace pangrova::artifact::test {
namespace {

namespace proto = pangrova::protocol::v2;

class CacheTestDirectory final {
 public:
  CacheTestDirectory() {
    path_ = std::filesystem::temp_directory_path() /
        ("pangrova-immutable-cache-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(path_);
  }

  ~CacheTestDirectory() {
    if (ImmutableArtifactCache::instance().enabled()) {
      ImmutableArtifactCache::instance().shutdown();
    }
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

std::string sha256(const std::string& value) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  EXPECT_EQ(EVP_Digest(
      value.data(), value.size(), digest.data(), &size,
      EVP_sha256(), nullptr), 1);
  EXPECT_EQ(size, 32u);
  return {reinterpret_cast<const char*>(digest.data()), size};
}

proto::ArtifactObject createObject(
    const std::filesystem::path& source,
    const std::string& contents) {
  std::ofstream output(source, std::ios::binary | std::ios::trunc);
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  output.close();

  proto::ArtifactObject object;
  object.set_object_id(source.filename().string());
  object.set_opaque_uri(source.string());
  object.set_size_bytes(contents.size());
  object.mutable_checksum()->set_algorithm(
      proto::Checksum::ALGORITHM_SHA256);
  object.mutable_checksum()->set_digest(sha256(contents));
  object.set_object_role("TEST_OBJECT");
  object.set_source_object_id("source-object");
  object.set_row_count(1);
  return object;
}

size_t countCachedObjects(const std::filesystem::path& root) {
  size_t count = 0;
  for (const auto& item : std::filesystem::recursive_directory_iterator(root)) {
    if (item.is_regular_file() && item.path().extension() == ".bin") {
      ++count;
    }
  }
  return count;
}

}  // namespace

TEST(ImmutableArtifactCacheTest, RestoresAndVerifiesWarmObjectAfterRestart) {
  CacheTestDirectory directory;
  const std::string contents = "verified immutable artifact";
  auto object = createObject(directory.path() / "remote.bin", contents);
  const auto cache = directory.path() / "cache";

  ImmutableArtifactCache::instance().configure(cache.string(), 1024);
  auto first = ImmutableArtifactCache::instance().materialize(object, 1024);
  EXPECT_EQ(first.downloaded_bytes, contents.size());
  EXPECT_EQ(ImmutableArtifactCache::instance().usedBytes(), contents.size());
  first = {};
  ImmutableArtifactCache::instance().shutdown();

  ImmutableArtifactCache::instance().configure(cache.string(), 1024);
  EXPECT_EQ(ImmutableArtifactCache::instance().usedBytes(), contents.size());
  auto warm = ImmutableArtifactCache::instance().materialize(object, 0);
  EXPECT_EQ(warm.downloaded_bytes, 0u);
  EXPECT_EQ(warm.size_bytes, contents.size());
}

TEST(ImmutableArtifactCacheTest, ReplacesCorruptWarmObjectFromCanonicalSource) {
  CacheTestDirectory directory;
  const std::string contents = "canonical bytes for recovery";
  auto object = createObject(directory.path() / "remote.bin", contents);
  const auto cache = directory.path() / "cache";

  ImmutableArtifactCache::instance().configure(cache.string(), 1024);
  auto first = ImmutableArtifactCache::instance().materialize(object, 1024);
  const auto local_path = first.local_path;
  first = {};
  ImmutableArtifactCache::instance().shutdown();
  std::ofstream corrupt(local_path, std::ios::binary | std::ios::trunc);
  corrupt << "corrupt";
  corrupt.close();

  ImmutableArtifactCache::instance().configure(cache.string(), 1024);
  auto recovered = ImmutableArtifactCache::instance().materialize(object, 1024);
  EXPECT_EQ(recovered.downloaded_bytes, contents.size());
  std::ifstream input(recovered.local_path, std::ios::binary);
  EXPECT_EQ(
      std::string(
          std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()),
      contents);
}

TEST(ImmutableArtifactCacheTest, ReconcilesPersistentFilesToReducedQuota) {
  CacheTestDirectory directory;
  const std::string first_contents(64, 'a');
  const std::string second_contents(64, 'b');
  auto first_object = createObject(
      directory.path() / "remote-first.bin", first_contents);
  auto second_object = createObject(
      directory.path() / "remote-second.bin", second_contents);
  const auto cache = directory.path() / "cache";

  ImmutableArtifactCache::instance().configure(cache.string(), 128);
  auto first = ImmutableArtifactCache::instance().materialize(first_object, 64);
  auto second = ImmutableArtifactCache::instance().materialize(second_object, 64);
  first = {};
  second = {};
  ImmutableArtifactCache::instance().shutdown();

  ImmutableArtifactCache::instance().configure(cache.string(), 64);
  EXPECT_LE(ImmutableArtifactCache::instance().usedBytes(), 64u);
  EXPECT_EQ(countCachedObjects(cache), 1u);
}

TEST(ImmutableArtifactCacheTest, DoesNotEvictTaskPinnedObject) {
  CacheTestDirectory directory;
  const std::string first_contents(64, 'a');
  const std::string second_contents(64, 'b');
  auto first_object = createObject(
      directory.path() / "remote-first.bin", first_contents);
  auto second_object = createObject(
      directory.path() / "remote-second.bin", second_contents);

  ImmutableArtifactCache::instance().configure(
      (directory.path() / "cache").string(), 64);
  auto pinned = ImmutableArtifactCache::instance().materialize(first_object, 64);
  EXPECT_THROW(
      ImmutableArtifactCache::instance().materialize(second_object, 64),
      std::runtime_error);
  EXPECT_TRUE(std::filesystem::exists(pinned.local_path));
  EXPECT_EQ(ImmutableArtifactCache::instance().usedBytes(), 64u);
}

}  // namespace pangrova::artifact::test
