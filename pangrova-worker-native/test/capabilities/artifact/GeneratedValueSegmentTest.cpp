// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include "capabilities/artifact/GeneratedValueSegment.h"

namespace pangrova::artifact::test {
namespace {

class TestFile final {
 public:
  TestFile() {
    path_ = std::filesystem::temp_directory_path() /
            ("pangrova-generated-values-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TestFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }
  std::string path() const { return path_.string(); }

 private:
  std::filesystem::path path_;
};

TEST(GeneratedValueSegmentTest, RoundTripsExactFloatVectors) {
  TestFile file;
  const std::vector<float> values{1.0F, -2.5F, 3.25F, 4.0F, 5.5F, -6.75F};
  GeneratedValueSegment::write(file.path(), values, std::vector<uint8_t>{1, 1},
                               2, 3);

  auto restored = GeneratedValueSegment::read(
      file.path(), 2, 3, std::filesystem::file_size(file.path()));

  EXPECT_EQ(restored.dimension, 3);
  EXPECT_EQ(restored.row_count, 2);
  EXPECT_EQ(restored.values, values);
  EXPECT_EQ(restored.validity, (std::vector<uint8_t>{1, 1}));
}

TEST(GeneratedValueSegmentTest, RejectsCorruptionAndContractMismatch) {
  TestFile file;
  const std::vector<float> values{1.0F, 2.0F, 3.0F, 4.0F};
  GeneratedValueSegment::write(file.path(), values, std::vector<uint8_t>{1, 1},
                               2, 2);

  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 3, 2, 1024),
               std::runtime_error);
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 2, 3, 1024),
               std::runtime_error);
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 2, 2, 8),
               std::invalid_argument);

  std::fstream corrupt(file.path(),
                       std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(corrupt.good());
  corrupt.put('X');
  corrupt.close();
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 2, 2, 1024),
               std::runtime_error);
}

TEST(GeneratedValueSegmentTest, RejectsNonFiniteAndTruncatedPayloads) {
  TestFile file;
  const std::vector<float> invalid{1.0F,
                                   std::numeric_limits<float>::infinity()};
  EXPECT_THROW(GeneratedValueSegment::write(file.path(), invalid,
                                            std::vector<uint8_t>{1}, 1, 2),
               std::invalid_argument);

  const std::vector<float> values{1.0F, 2.0F};
  GeneratedValueSegment::write(file.path(), values, std::vector<uint8_t>{1}, 1,
                               2);
  std::filesystem::resize_file(file.path(),
                               std::filesystem::file_size(file.path()) - 1);
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 1, 2, 1024),
               std::runtime_error);
}

TEST(GeneratedValueSegmentTest, RetainsMixedAndAllNullPhysicalRows) {
  TestFile file;
  const std::vector<float> values{0, 0, 3, 4, 0, 0};
  GeneratedValueSegment::write(file.path(), values,
                               std::vector<uint8_t>{0, 1, 0}, 3, 2);
  auto restored = GeneratedValueSegment::read(file.path(), 3, 2, 1024);
  EXPECT_EQ(restored.row_count, 3);
  EXPECT_EQ(restored.values, values);
  EXPECT_EQ(restored.validity, (std::vector<uint8_t>{0, 1, 0}));
  const std::vector<float> nulls(18, 0);
  const std::vector<uint8_t> validity(9, 0);
  GeneratedValueSegment::write(file.path(), nulls, validity, 9, 2);
  restored = GeneratedValueSegment::read(file.path(), 9, 2, 1024);
  EXPECT_EQ(restored.row_count, 9);
  EXPECT_EQ(restored.values, nulls);
  EXPECT_EQ(restored.validity, validity);
}

TEST(GeneratedValueSegmentTest, RejectsInvalidValidityAndInventedNullPayload) {
  TestFile file;
  const std::vector<float> zero{0, 0};
  EXPECT_THROW(GeneratedValueSegment::write(file.path(), zero,
                                            std::vector<uint8_t>{}, 1, 2),
               std::invalid_argument);
  EXPECT_THROW(GeneratedValueSegment::write(file.path(), zero,
                                            std::vector<uint8_t>{2}, 1, 2),
               std::invalid_argument);
  EXPECT_THROW(
      GeneratedValueSegment::write(file.path(), std::vector<float>{1, 0},
                                   std::vector<uint8_t>{0}, 1, 2),
      std::invalid_argument);
  EXPECT_THROW(
      GeneratedValueSegment::write(file.path(), std::vector<float>{-0.0F, 0},
                                   std::vector<uint8_t>{0}, 1, 2),
      std::invalid_argument);
}

TEST(GeneratedValueSegmentTest,
     RejectsOldFormatAndNonCanonicalBitmapOrNullPayload) {
  TestFile file;
  const std::vector<float> zero{0, 0};
  const auto corrupt = [&](std::streamoff offset, char value) {
    std::fstream output(file.path(),
                        std::ios::in | std::ios::out | std::ios::binary);
    output.seekp(offset);
    output.put(value);
  };
  GeneratedValueSegment::write(file.path(), zero, std::vector<uint8_t>{0}, 1,
                               2);
  corrupt(7, '1');
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 1, 2, 1024),
               std::runtime_error);
  GeneratedValueSegment::write(file.path(), zero, std::vector<uint8_t>{0}, 1,
                               2);
  corrupt(32, static_cast<char>(0x80));
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 1, 2, 1024),
               std::runtime_error);
  GeneratedValueSegment::write(file.path(), zero, std::vector<uint8_t>{0}, 1,
                               2);
  corrupt(33, 1);
  EXPECT_THROW(GeneratedValueSegment::read(file.path(), 1, 2, 1024),
               std::runtime_error);
}

}  // namespace
}  // namespace pangrova::artifact::test
