#pragma once

#include <string>

namespace pangrova::index {

/// Applies a published, verified Bloom hint before Velox enqueues a data split.
class IndexHintFilter {
 public:
  /// Unusable hints retain the split. A negative proof must use the bytes
  /// whose length and SHA-256 match the published mapping for this data file.
  static bool shouldKeepTypedSplit(
      const std::string& file_scan_detail,
      const std::string& data_file_path,
      bool* evaluated = nullptr);
};

}  // namespace pangrova::index
