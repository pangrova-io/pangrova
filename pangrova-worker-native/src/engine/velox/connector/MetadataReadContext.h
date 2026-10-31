#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "folly/CancellationToken.h"

namespace facebook::velox { class ReadFile; }
namespace pangrova::resource { class TaskObjectReadBudget; }

namespace pangrova::velox {

struct MetadataObjectReference {
  std::string uri;
  uint64_t length;
  std::string sha256;
};

// One task's actual backend-read owner, shared by every metadata object and pass.
class MetadataReadContext final {
 public:
  MetadataReadContext(std::shared_ptr<resource::TaskObjectReadBudget> budget,
      folly::CancellationToken cancellation);
  // Standalone format inspection has no executing task. DataSource always uses
  // the admitted constructor above, which rejects a missing task owner.
  static MetadataReadContext unscoped();
  std::shared_ptr<facebook::velox::ReadFile> open(const std::string& uri) const;
  void checkpoint() const;
  folly::CancellationToken cancellationToken() const { return cancellation_; }
 private:
  MetadataReadContext() = default;
  std::shared_ptr<resource::TaskObjectReadBudget> budget_;
  folly::CancellationToken cancellation_;
};

}  // namespace pangrova::velox
