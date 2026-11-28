#include "engine/velox/connector/MetadataReadContext.h"

#include <optional>
#include <stdexcept>

#include "engine/resource/TaskBudgetReadFile.h"
#include "engine/split/PathUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"

namespace pangrova::velox {

MetadataReadContext::MetadataReadContext(std::shared_ptr<resource::TaskObjectReadBudget> budget,
    folly::CancellationToken cancellation,
    std::shared_ptr<const facebook::velox::config::ConfigBase> fileSystemConfig)
    : budget_(std::move(budget)), cancellation_(std::move(cancellation)),
      file_system_config_(std::move(fileSystemConfig)) {
  if (!budget_) throw std::invalid_argument("Metadata task read requires its admitted owner");
}

MetadataReadContext MetadataReadContext::unscoped() { return MetadataReadContext(); }

void MetadataReadContext::checkpoint() const {
  if (cancellation_.isCancellationRequested()) throw std::runtime_error("Metadata read cancelled");
}

std::shared_ptr<facebook::velox::ReadFile> MetadataReadContext::open(const std::string& uri) const {
  checkpoint();
  // The task must still own its read fence before path resolution initializes
  // a filesystem. Count the adapter call only when it reaches the backend.
  auto opening = budget_ ? std::make_optional(budget_->begin(0, resource::TaskObjectReadBudget::Operation::METADATA))
                         : std::nullopt;
  const auto path = split::metadataObjectPath(uri);
  if (path.starts_with("s3://")
      && (!file_system_config_ || !file_system_config_->valueExists("hive.s3.aws-access-key"))) {
    throw std::runtime_error("Native S3 read requires configured filesystem credentials");
  }
  auto filesystem = facebook::velox::filesystems::getFileSystem(path, file_system_config_);
  if (!filesystem) throw std::runtime_error("No Native filesystem for metadata URI");
  if (opening) opening->startCall();
  facebook::velox::filesystems::FileOptions options;
  if (budget_ && path.starts_with("s3://")) options.stats = budget_->s3IoStats();
  std::shared_ptr<facebook::velox::ReadFile> input = filesystem->openFileForRead(path, options);
  if (opening) opening->completeCall(0);
  checkpoint();
  if (budget_) return std::make_shared<resource::TaskBudgetReadFile>(
      std::move(input), budget_, cancellation_, options.stats);
  return input;
}

}  // namespace pangrova::velox
