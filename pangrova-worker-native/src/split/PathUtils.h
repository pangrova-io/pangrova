#pragma once

#include <string>

namespace pangrova::split {

// Strip "file:" prefix from path for local file access.
// Shared utility — eliminates duplication between PangrovaVeloxConnector
// and PaimonDataSourceFactory.
inline std::string stripFilePrefix(const std::string& path) {
  if (path.rfind("file:", 0) == 0) {
    return path.substr(5);
  }
  return path;
}

// P1-MISC-12: Generalized S3-compatible URI normalization.
// Converts cos://, obs://, oss:// schemes to s3:// for Velox S3FileSystem.
// These cloud object stores (Tencent COS, Huawei OBS, Alibaba OSS) all
// provide S3-compatible APIs.
inline std::string normalizeToS3Uri(const std::string& path) {
  if (path.rfind("oss://", 0) == 0) {
    return "s3://" + path.substr(6);
  }
  if (path.rfind("cos://", 0) == 0) {
    return "s3://" + path.substr(6);
  }
  if (path.rfind("obs://", 0) == 0) {
    return "s3://" + path.substr(6);
  }
  return path;
}

}  // namespace pangrova::split
