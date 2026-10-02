// Manages output queues and routes batches to output or shuffle queues.
// Owns OutputQueue and SerializedShuffleQueue.
// Extracted from TaskExecutor to enforce single-responsibility.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "executor/queue/OutputQueue.h"
#include "shuffle/SerializedShuffleQueue.h"

namespace pangrova::executor {

// Routes output batches to the appropriate queue (output or shuffle).
// Thread-safe: queue operations are internally synchronized.
//
// Output queue stores RowVectorPtr directly — Arrow C Data Interface export
// happens at the JNI boundary (nativeGetNextBatch), avoiding intermediate
// RecordBatch conversion.
//
// Shuffle queue stores pre-serialized ShuffleBatch (PrestoVectorSerde bytes),
// replacing the old ShuffleQueue<RecordBatch> to eliminate Arrow IPC overhead.
class TaskOutputRouter {
 public:
  TaskOutputRouter(int32_t num_shuffle_partitions,
                   size_t output_queue_capacity,
                   size_t shuffle_queue_capacity,
                   size_t shuffle_buffer_bytes,
                   std::chrono::milliseconds queue_poll_timeout);

  // Non-copyable, non-movable (owns unique_ptr members).
  TaskOutputRouter(const TaskOutputRouter&) = delete;
  TaskOutputRouter& operator=(const TaskOutputRouter&) = delete;

  // Route a RowVector to the output queue.
  // Returns false if the target queue push timed out or was closed.
  bool routeOutputBatch(facebook::velox::RowVectorPtr row_vector);

  // Pull next result RowVector from the output queue.
  // Returns true if a batch was retrieved, false on timeout or closed.
  bool getNextBatch(facebook::velox::RowVectorPtr& row_vector,
                    std::chrono::milliseconds timeout);

  // Queue access for producer side (Velox callback / test).
  OutputQueue* outputQueue();
  pangrova::shuffle::SerializedShuffleQueue* serializedShuffleQueue();

  // Close all queues — unblocks waiters.
  void closeAll();

  // Mark shuffle output failed before closing so readers cannot accept EOF.
  // False means successful shuffle commit already won the terminal race.
  bool abortShuffle();

  // F-NATIVE-EXEC-05: Drain unconsumed items from the output queue.
  // Called during release() to ensure RowVector memory pools are released
  // while still alive.
  void drainOutputQueue();

  // Number of shuffle partitions configured.
  int32_t numShufflePartitions() const;

 private:
  std::unique_ptr<OutputQueue> output_queue_;
  std::unique_ptr<pangrova::shuffle::SerializedShuffleQueue> serialized_shuffle_queue_;
  int32_t num_shuffle_partitions_;
  std::chrono::milliseconds queue_poll_timeout_;
};

}  // namespace pangrova::executor
