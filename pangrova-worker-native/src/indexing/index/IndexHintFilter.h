#pragma once

#include <string>
#include <vector>

namespace pangrova::protocol::v2 {
class IndexEvalHintDetail;
}
namespace pangrova::index {

/// Applies published, verified Bloom and B-tree hints before Velox enqueues a data split.
class IndexHintFilter {
 public:
  /// Unusable hints retain the split. A negative proof must use the bytes
  /// whose length and SHA-256 match the published mapping for this data file.
  static bool shouldKeepTypedSplit(
      const std::vector<protocol::v2::IndexEvalHintDetail>* hints,
      const std::string& data_file_path,
      bool* evaluated = nullptr);
};

}  // namespace pangrova::index
