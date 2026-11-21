// PathUtilsOssTest: Verifies S3-compatible URI normalization and scheme handling.

#include <gtest/gtest.h>

#include "engine/split/LocalFileUri.h"
#include "engine/split/PathUtils.h"

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

TEST(PathUtilsOssTest, DecodeLocalFileUriWithEscapedSpace) {
  EXPECT_EQ(decodeLocalFileUri("file:/tmp/iceberg%20warehouse/manifest.avro"),
      "/tmp/iceberg warehouse/manifest.avro");
  EXPECT_EQ(decodeLocalFileUri("file:///tmp/iceberg%20warehouse/manifest.avro"),
      "/tmp/iceberg warehouse/manifest.avro");
}

TEST(PathUtilsOssTest, RejectMalformedOrUnsafeLocalFileUri) {
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/bad%2"), std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/bad%GG"), std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/bad%2Fpath"), std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/bad%00path"), std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/%2e%2e/manifest.avro"),
      std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file://remote/tmp/manifest.avro"),
      std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:////tmp/manifest.avro"),
      std::invalid_argument);
  EXPECT_THROW(decodeLocalFileUri("file:/tmp/manifest.avro?query"),
      std::invalid_argument);
}

TEST(PathUtilsOssTest, RejectNonlocalUri) {
  EXPECT_THROW(decodeLocalFileUri("s3://bucket/manifest.avro"),
      std::invalid_argument);
}

TEST(PathUtilsOssTest, MetadataObjectPathPreservesS3AndDecodesLocalFile) {
  EXPECT_EQ(metadataObjectPath("s3://bucket/manifest.avro"),
      "s3://bucket/manifest.avro");
  EXPECT_EQ(metadataObjectPath("oss://bucket/manifest.avro"),
      "s3://bucket/manifest.avro");
  EXPECT_EQ(metadataObjectPath("file:/tmp/iceberg%20warehouse/manifest.avro"),
      "/tmp/iceberg warehouse/manifest.avro");
  EXPECT_THROW(metadataObjectPath("file:/tmp/bad%2"), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::split
