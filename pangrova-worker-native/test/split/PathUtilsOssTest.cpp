// PathUtilsOssTest: Verifies S3-compatible URI normalization and scheme handling.

#include <gtest/gtest.h>

#include "split/PathUtils.h"

namespace pangrova::split {
namespace {

TEST(PathUtilsOssTest, NormalizeOssToS3) {
  EXPECT_EQ(normalizeToS3Uri("oss://bucket/data/file.parquet"),
            "s3://bucket/data/file.parquet");
}

TEST(PathUtilsOssTest, NormalizeCosToS3) {
  EXPECT_EQ(normalizeToS3Uri("cos://bucket/data/file.parquet"),
            "s3://bucket/data/file.parquet");
}

TEST(PathUtilsOssTest, NormalizeObsToS3) {
  EXPECT_EQ(normalizeToS3Uri("obs://bucket/data/file.parquet"),
            "s3://bucket/data/file.parquet");
}

TEST(PathUtilsOssTest, S3UriUnchanged) {
  EXPECT_EQ(normalizeToS3Uri("s3://bucket/key"), "s3://bucket/key");
}

TEST(PathUtilsOssTest, HdfsUriUnchanged) {
  EXPECT_EQ(normalizeToS3Uri("hdfs://namenode/path"), "hdfs://namenode/path");
}

TEST(PathUtilsOssTest, LocalFileUnchanged) {
  EXPECT_EQ(normalizeToS3Uri("/tmp/data.parquet"), "/tmp/data.parquet");
}

TEST(PathUtilsOssTest, EmptyStringUnchanged) {
  EXPECT_EQ(normalizeToS3Uri(""), "");
}

TEST(PathUtilsOssTest, OssWithNestedPath) {
  EXPECT_EQ(normalizeToS3Uri("oss://my-bucket/warehouse/db/table/part-0.parquet"),
            "s3://my-bucket/warehouse/db/table/part-0.parquet");
}

TEST(PathUtilsOssTest, StripFilePrefixStillWorks) {
  EXPECT_EQ(stripFilePrefix("file:/tmp/data.parquet"), "/tmp/data.parquet");
  EXPECT_EQ(stripFilePrefix("/tmp/data.parquet"), "/tmp/data.parquet");
}

}  // namespace
}  // namespace pangrova::split
