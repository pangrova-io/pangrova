#include "engine/velox/connector/MetadataReadContext.h"

#include <stdexcept>

#include "engine/resource/TaskBudgetReadFile.h"
#include "engine/split/PathUtils.h"
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
  const auto path = split::metadataObjectPath(uri);
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, nullptr);
  if (!filesystem) throw std::runtime_error("No Native filesystem for metadata URI");
  if (budget_) return resource::TaskBudgetReadFile::open(*filesystem, path, budget_, cancellation_);
  auto input = filesystem->openFileForRead(path);
  checkpoint();
  return input;
}

}  // namespace pangrova::velox
