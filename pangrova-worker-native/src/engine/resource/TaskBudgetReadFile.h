#pragma once

#include <memory>

#include "folly/CancellationToken.h"
#include "engine/resource/TaskObjectReadBudget.h"
#include "velox/common/file/File.h"

namespace pangrova::resource {

/** Wraps the actual backend; cache hits above this file perform no backend request. */
class TaskBudgetReadFile final : public facebook::velox::ReadFile {
 public:
  TaskBudgetReadFile(std::shared_ptr<facebook::velox::ReadFile> input,
      std::shared_ptr<TaskObjectReadBudget> budget, folly::CancellationToken cancellation);
  using facebook::velox::ReadFile::pread;
  void verifySha256(std::string_view expected, uint64_t declaredLength) const;
  std::string_view pread(uint64_t offset, uint64_t length, void* buffer,
      const facebook::velox::FileIoContext& context = {}) const override;
  uint64_t preadv(uint64_t offset, const std::vector<folly::Range<char*>>& buffers,
      const facebook::velox::FileIoContext& context = {}) const override;
  uint64_t preadv(folly::Range<const facebook::velox::common::Region*> regions,
      folly::Range<folly::IOBuf*> buffers,
      const facebook::velox::FileIoContext& context = {}) const override;
  folly::SemiFuture<uint64_t> preadvAsync(uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const facebook::velox::FileIoContext& context = {}) const override;
  // This DWIO boundary supports synchronous backend reads only. Native async
  // requires a caller buffer owner contract that this ReadFile API cannot supply.
  bool hasPreadvAsync() const override { return false; }
  bool shouldCoalesce() const override { return input_->shouldCoalesce(); }
  uint64_t size() const override { return size_; }
  uint64_t memoryUsage() const override { return input_->memoryUsage(); }
  uint64_t bytesRead() const override { return input_->bytesRead(); }
  void resetBytesRead() override { input_->resetBytesRead(); }
  std::string getName() const override { return input_->getName(); }
  uint64_t getNaturalReadSize() const override { return input_->getNaturalReadSize(); }
 private:
  const std::shared_ptr<facebook::velox::ReadFile> input_;
  const std::shared_ptr<TaskObjectReadBudget> budget_;
  const folly::CancellationToken cancellation_;
  const uint64_t size_;
};

}  // namespace pangrova::resource
