#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "pangrova/protocol/v2/task.pb.h"
#include "pangrova/protocol/v2/result.pb.h"
#include "velox/vector/ComplexVector.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace facebook::velox::memory {
class MemoryPool;
}  // namespace facebook::velox::memory

namespace pangrova::lance {

struct PhysicalWriterConfig {
  protocol::v2::PhysicalFileWriteBinding binding;
  std::vector<uint8_t> taskAttempt;
  std::string tenantId;
  std::string storageBinding;
  std::string spillDirectory;
  uint64_t maxSpillBytes = 0;
  uint64_t maxObjectWriteBytes = 0;
  // Cumulative observations: the task owner assigns or computes a delta.
  std::function<void(const protocol::v2::ResourceUsage&)> resourceUsagePublisher;
};

// One task-local RowVector sink. Staging and conditional object creation never
// publish a table snapshot; the connector owns the one authoritative SDK commit.
class LancePhysicalBatchWriter final {
 public:
  LancePhysicalBatchWriter(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      facebook::velox::RowTypePtr outputType, PhysicalWriterConfig config);
  ~LancePhysicalBatchWriter();
  LancePhysicalBatchWriter(const LancePhysicalBatchWriter&) = delete;
  LancePhysicalBatchWriter& operator=(const LancePhysicalBatchWriter&) = delete;

  void writeBatch(const facebook::velox::RowVectorPtr& batch);
  std::vector<uint8_t> finish();
  // Idempotent explicit drain/destroy. Failure retains the complete task owner.
  void close();
  // Call only after recording a drain failure; never erases that task failure.
  void releaseDrained();
  bool isDrainedAndCleaned() const { return encoders_joined_ && private_stage_cleaned_; }
  const protocol::v2::ResourceUsage& resourceUsage() const { return resource_usage_; }

 private:
  void validateSchema();
  void refreshResourceUsage();
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  facebook::velox::RowTypePtr output_type_;
  PhysicalWriterConfig config_;
  std::shared_ptr<arrow::Schema> physical_schema_;
  void* handle_ = nullptr;
  bool failed_ = false;
  bool finished_ = false;
  bool encoders_joined_ = false;
  bool private_stage_cleaned_ = false;
  protocol::v2::ResourceUsage resource_usage_;
};

}  // namespace pangrova::lance
