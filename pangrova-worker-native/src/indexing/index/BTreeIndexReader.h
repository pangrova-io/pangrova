#pragma once

#include <string>

namespace pangrova::protocol::v2 {
class IndexPredicate;
}

namespace pangrova::index {

/// Evaluates a validated B-tree payload without treating malformed or unsupported keys as negative proof.
class BTreeIndexReader {
 public:
  enum class Result {
    kMayMatch,
    kProvenAbsent,
    kUnknown,
  };

  static Result evaluate(const std::string& payload,
                         const pangrova::protocol::v2::IndexPredicate& predicate);
};

}  // namespace pangrova::index
