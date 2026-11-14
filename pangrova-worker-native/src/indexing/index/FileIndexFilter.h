#pragma once

#include <string>

namespace pangrova::index {

/// Evaluates Java-compatible Split Block Bloom Filter membership.
class FileIndexFilter {
 public:
  /// Returns false only when a supported SBBF payload excludes the value.
  static bool mayContainValue(
      const std::string& index_data,
      const std::string& column_name,
      const std::string& value);
};

}  // namespace pangrova::index
