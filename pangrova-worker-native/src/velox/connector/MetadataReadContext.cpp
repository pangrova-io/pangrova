#include "velox/connector/MetadataReadContext.h"

#include <stdexcept>

#include "resource/TaskBudgetReadFile.h"
#include "split/PathUtils.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::velox {

MetadataReadContext::MetadataReadContext(std::shared_ptr<resource::TaskObjectReadBudget> budget,
    folly::CancellationToken cancellation)
    : budget_(std::move(budget)), cancellation_(std::move(cancellation)) {
  if (!budget_) throw std::invalid_argument("Metadata task read requires its admitted owner");
}

MetadataReadContext MetadataReadContext::unscoped() { return MetadataReadContext(); }

void MetadataReadContext::checkpoint() const {
  if (cancellation_.isCancellationRequested()) throw std::runtime_error("Metadata read cancelled");
}

std::shared_ptr<facebook::velox::ReadFile> MetadataReadContext::open(const std::string& uri) const {
  checkpoint();
  // Opening can itself contact a backend. Hold the real owner while it runs,
  // even though metadata/stat requests consume no payload-range bytes.
  auto opening = budget_ ? budget_->begin(0) : nullptr;
  const auto path = split::metadataObjectPath(uri);
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!filesystem) throw std::runtime_error("No Native filesystem for metadata URI");
  std::shared_ptr<facebook::velox::ReadFile> input = filesystem->openFileForRead(path);
  if (opening) opening->complete(0);
  checkpoint();
  if (budget_) return std::make_shared<resource::TaskBudgetReadFile>(
      std::move(input), budget_, cancellation_);
  return input;
}

}  // namespace pangrova::velox
