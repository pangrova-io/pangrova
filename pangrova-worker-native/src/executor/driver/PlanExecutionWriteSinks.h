#pragma once

#include <memory>
#include <string>
#include "executor/driver/WriteOutputDrain.h"
#include "executor/queue/OutputQueue.h"
#include "velox/type/Type.h"

namespace facebook::velox::memory { class MemoryPool; }
namespace pangrova::lance { class LanceBatchWriter; class LancePhysicalBatchWriter; }
namespace pangrova::observability { class NativeMetricsCollector; }
namespace pangrova::shuffle { class SerializedShuffleQueue; }

namespace pangrova::executor {
struct TaskExecutorConfig;
struct WriteConfigInfo;
struct WorkerLocalSpillPolicy;

struct PhysicalWriterOwner {
  observability::NativeMetricsCollector& metrics;
  std::shared_ptr<lance::LancePhysicalBatchWriter>& writer;
};

void drainOutputToWriter(OutputQueue* queue, const WriteConfigInfo& config,
    const std::string& taskId,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WriteProducerOutcomeRef& outcome);
void drainOutputToLance(OutputQueue* queue, const WriteConfigInfo& config,
    const std::string& taskId, const std::string& tenantId,
    const facebook::velox::RowTypePtr& outputType,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WriteProducerOutcomeRef& outcome,
    std::shared_ptr<lance::LanceBatchWriter>& taskWriter);
void drainOutputToPhysicalWriter(OutputQueue* queue, const TaskExecutorConfig& task,
    const facebook::velox::RowTypePtr& outputType,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
    const WorkerLocalSpillPolicy& spillPolicy, const WriteProducerOutcomeRef& outcome,
    PhysicalWriterOwner owner);
void drainShuffleToEgress(pangrova::shuffle::SerializedShuffleQueue* queue,
    const TaskExecutorConfig& config);
}  // namespace pangrova::executor
