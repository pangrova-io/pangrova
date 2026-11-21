#include <gtest/gtest.h>

#include <sys/stat.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "TestMemoryHelper.h"
#include "engine/velox/connector/PaimonBranchDirectoryReader.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::velox::test {
namespace {

int64_t mtimeMillis(const struct stat& status) {
#if defined(__APPLE__)
  return static_cast<int64_t>(status.st_mtime) * 1000 +
      status.st_mtimespec.tv_nsec / 1000000;
#else
  return static_cast<int64_t>(status.st_mtime) * 1000 +
      status.st_mtim.tv_nsec / 1000000;
#endif
}

class BranchDirectory final {
 public:
  explicit BranchDirectory(bool spaced = false, std::string branchName = "dev") {
    char root[] = "/tmp/pangrova-branch-XXXXXX";
    auto created = mkdtemp(root);
    if (!created) throw std::runtime_error("Cannot create branch test root");
    root_ = created;
    path_ = root_ + (spaced ? "/with space" : "") +
        "/branch/branch-" + branchName;
    std::filesystem::create_directories(path_);
    if (::lstat(path_.c_str(), &status_) != 0) {
      throw std::runtime_error("Cannot stat branch test directory");
    }
  }

  ~BranchDirectory() { std::filesystem::remove_all(root_); }
  const std::string& path() const { return path_; }
  int64_t mtime() const { return mtimeMillis(status_); }
  uint64_t device() const { return static_cast<uint64_t>(status_.st_dev); }
  uint64_t inode() const { return static_cast<uint64_t>(status_.st_ino); }

 private:
  std::string root_;
  std::string path_;
  struct stat status_ {};
};

TEST(PaimonBranchDirectoryReaderTest, ReadsPinnedStatusAndProjection) {
  auto pool = pangrova::test::testLeafPool("PaimonBranches");
  BranchDirectory directory;
  auto fullType = facebook::velox::ROW(
      {"branch_name", "create_time"},
      {facebook::velox::VARCHAR(), facebook::velox::TIMESTAMP()});
  PaimonBranchDirectoryReader reader("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode(), 0, fullType, pool.get());
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::StringView>()
                ->valueAt(0).str(), "dev");
  EXPECT_EQ(row->childAt(1)->asFlatVector<facebook::velox::Timestamp>()
                ->valueAt(0).toMillis(), directory.mtime());
  EXPECT_EQ(reader.next(1), nullptr);

  auto projection = facebook::velox::ROW(
      {"create_time"}, {facebook::velox::TIMESTAMP()});
  PaimonBranchDirectoryReader projected("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode(), 0, projection, pool.get());
  ASSERT_NE(projected.next(1), nullptr);
}

TEST(PaimonBranchDirectoryReaderTest, RejectsChangedOrUnsupportedDirectory) {
  auto pool = pangrova::test::testLeafPool("PaimonBranchesFailure");
  BranchDirectory directory;
  auto type = facebook::velox::ROW(
      {"branch_name"}, {facebook::velox::VARCHAR()});
  EXPECT_THROW(PaimonBranchDirectoryReader(
      "s3://warehouse/branch/branch-dev", "dev", directory.mtime(),
      directory.device(), directory.inode(), 0, type, pool.get()), std::invalid_argument);
  EXPECT_THROW(PaimonBranchDirectoryReader(
      "file:" + directory.path(), "../dev", directory.mtime(),
      directory.device(), directory.inode(), 0, type, pool.get()), std::invalid_argument);
  PaimonBranchDirectoryReader reader("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode(), 0, type, pool.get());
  std::filesystem::last_write_time(directory.path(),
      std::filesystem::last_write_time(directory.path()) + std::chrono::seconds(2));
  EXPECT_THROW(reader.next(1), std::runtime_error);
  PaimonBranchDirectoryReader replaced("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode() + 1, 0, type, pool.get());
  EXPECT_THROW(replaced.next(1), std::runtime_error);
}

TEST(PaimonBranchDirectoryReaderTest, DecodesLocalFileUriAndRejectsUnsafeEncoding) {
  auto pool = pangrova::test::testLeafPool("PaimonBranchesUri");
  BranchDirectory directory(true, "feature beta");
  auto type = facebook::velox::ROW(
      {"branch_name"}, {facebook::velox::VARCHAR()});
  auto encoded = "file://" + directory.path();
  encoded.replace(encoded.find("with space"), 10, "with%20space");
  encoded.replace(encoded.find("feature beta"), 12, "feature%20beta");
  PaimonBranchDirectoryReader reader(encoded, "feature beta", directory.mtime(),
      directory.device(), directory.inode(), 0, type, pool.get());
  ASSERT_NE(reader.next(1), nullptr);
  auto unsafe = encoded;
  unsafe.replace(unsafe.find("%20"), 3, "%2F");
  EXPECT_THROW(PaimonBranchDirectoryReader(unsafe, "feature beta", directory.mtime(),
      directory.device(), directory.inode(), 0, type, pool.get()).next(1),
      std::invalid_argument);
}

TEST(PaimonBranchDirectoryReaderTest, ReplaysPinnedPaimonLocalWallClock) {
  auto pool = pangrova::test::testLeafPool("PaimonBranchesZone");
  BranchDirectory directory;
  auto type = facebook::velox::ROW(
      {"create_time"}, {facebook::velox::TIMESTAMP()});
  PaimonBranchDirectoryReader reader("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode(), 8 * 3600,
      type, pool.get());
  auto row = reader.next(1);
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(row->childAt(0)->asFlatVector<facebook::velox::Timestamp>()
                ->valueAt(0).toMillis(), directory.mtime() + 8LL * 3600 * 1000);
  EXPECT_THROW(PaimonBranchDirectoryReader("file:" + directory.path(), "dev",
      directory.mtime(), directory.device(), directory.inode(), 19 * 3600,
      type, pool.get()), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::velox::test
