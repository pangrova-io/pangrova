// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "capabilities/artifact/RowLocatorSidecar.h"

namespace pangrova::artifact::test {
namespace {

class TestFile final {
 public:
  TestFile() {
    path_ = std::filesystem::temp_directory_path() /
        ("pangrova-row-locators-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TestFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  const std::string path() const { return path_.string(); }

 private:
  std::filesystem::path path_;
};

TEST(RowLocatorSidecarTest, RoundTripsUnsignedPhysicalPositions) {
  TestFile file;
  const std::vector<uint64_t> positions{0, 7, 4'294'967'297ULL};
  RowLocatorSidecar::writePhysicalRowPositions(file.path(), positions);

  EXPECT_EQ(
      RowLocatorSidecar::readPhysicalRowPositions(file.path(), 3),
      positions);
}

TEST(RowLocatorSidecarTest, RejectsConfiguredEntryBound) {
  TestFile file;
  const std::vector<uint64_t> positions{0, 1, 2};
  RowLocatorSidecar::writePhysicalRowPositions(file.path(), positions);

  EXPECT_THROW(
      RowLocatorSidecar::readPhysicalRowPositions(file.path(), 2),
      std::runtime_error);
}

TEST(RowLocatorSidecarTest, RejectsTrailingOrTruncatedBytes) {
  TestFile file;
  const std::vector<uint64_t> positions{0, 1};
  RowLocatorSidecar::writePhysicalRowPositions(file.path(), positions);
  std::filesystem::resize_file(
      file.path(), std::filesystem::file_size(file.path()) - 1);

  EXPECT_THROW(
      RowLocatorSidecar::readPhysicalRowPositions(file.path(), 2),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::artifact::test
