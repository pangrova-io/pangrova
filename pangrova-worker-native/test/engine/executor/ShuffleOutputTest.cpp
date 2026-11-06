#include "CanonicalTestPlan.h"
// Tests for TaskExecutor shuffle output integration — verifies that
// when num_shuffle_partitions > 0, serialized shuffle batches are routed
// to the SerializedShuffleQueue.
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <chrono>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

#include "engine/executor/driver/TaskExecutor.h"
#include "engine/shuffle/FlightShuffleServer.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "velox/vector/ComplexVector.h"

#include "TestMemoryHelper.h"

#if __has_include("substrait/plan.pb.h")
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
#define PANGROVA_HAS_SUBSTRAIT_PROTO 1
#else
#define PANGROVA_HAS_SUBSTRAIT_PROTO 0
#endif

namespace pangrova::executor::test {
namespace {

using std::chrono::milliseconds;

// Ensure Velox runtime is initialized before any test in this file.
static auto* g_init_memory = pangrova::test::testMemoryManager();

bool waitForTerminal(TaskExecutor& exec, milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto s = exec.state();
    if (s == ExecutorState::FINISHED || s == ExecutorState::FAILED ||
        s == ExecutorState::CANCELLED) {
      return true;
    }
    std::this_thread::sleep_for(milliseconds(10));
  }
  return false;
}

#if PANGROVA_HAS_SUBSTRAIT_PROTO

std::vector<uint8_t> serialize(const google::protobuf::MessageLite& msg) {
  std::vector<uint8_t> buf(msg.ByteSizeLong());
  msg.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

std::vector<uint8_t> buildValuesPlan() {
  auto plan = pangrova::test::canonicalTestPlan(substrait::Plan{});
  auto* plan_rel = plan.add_relations();
  auto* root = plan_rel->mutable_root();
  root->add_names("value");
  auto* read = root->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* vt = read->mutable_virtual_table();
  // Add one row with a single BIGINT value so ValuesNode produces 1 row.
  auto* row = vt->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(42);
  return serialize(plan);
}

std::vector<uint8_t> buildShuffleValuesPlan(int32_t partitions) {
  auto plan = pangrova::test::canonicalTestPlan(substrait::Plan{});
  auto* root = plan.add_relations()->mutable_root();
  root->add_names("value");
  auto* exchange = root->mutable_input()->mutable_exchange();
  exchange->set_partition_count(partitions);
  exchange->mutable_round_robin();
  auto* read = exchange->mutable_input()->mutable_read();
  read->mutable_base_schema()->add_names("value");
  read->mutable_base_schema()->mutable_struct_()->add_types()->mutable_i64()
      ->set_nullability(substrait::Type::NULLABILITY_REQUIRED);
  auto* row = read->mutable_virtual_table()->add_expressions();
  row->add_fields()->mutable_literal()->set_i64(42);
  return serialize(plan);
}

TaskExecutorConfig makeShuffleConfig(int32_t num_partitions) {
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "shuffle-test-task";
  config.query_id = "shuffle-test-query";
  config.substrait_plan = buildShuffleValuesPlan(num_partitions);
  config.num_shuffle_partitions = num_partitions;
  config.queue_poll_timeout = milliseconds(500);
  return config;
}

// --- Shuffle output routes to SerializedShuffleQueue ---

TEST(ShuffleOutputTest, SerializedQueueOwnsSubmittedBytes) {
  pangrova::shuffle::SerializedShuffleQueue queue(1);
  pangrova::shuffle::ShuffleBatch submitted;
  submitted.partition_id = 0;
  submitted.row_count = 1;
  submitted.data = folly::IOBuf::copyBuffer("page", 4);
  const auto* producer_bytes = submitted.data->data();
  ASSERT_TRUE(queue.push(std::move(submitted), milliseconds(100)));

  pangrova::shuffle::ShuffleBatch received;
  ASSERT_EQ(queue.pop(received, milliseconds(100)), PopResult::DATA);
  ASSERT_NE(received.data, nullptr);
  EXPECT_NE(received.data->data(), producer_bytes);
  EXPECT_EQ(received.data->computeChainDataLength(), 4);
  EXPECT_EQ(std::string_view(
                reinterpret_cast<const char*>(received.data->data()), 4),
            "page");
}

TEST(ShuffleOutputTest, SerializedShuffleBatchAvailable) {
  auto config = makeShuffleConfig(3);
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);

  // FINISHED is visible only after the producer has flushed its buffered
  // partition and committed a clean queue close. A reader arriving after task
  // completion must still receive the one VALUES row, then clean EOF.
  auto* queue = exec.serializedShuffleQueue();
  ASSERT_NE(queue, nullptr);
  EXPECT_TRUE(queue->isCommitted());
  EXPECT_TRUE(queue->isClosed());
  // The producer has completed before this Flight reader arrives. The
  // queue remains borrowed until Flight quiesces; release() is only legal
  // after that point because it drains any unconsumed batches.
  ASSERT_GT(queue->size(), 0);
  pangrova::shuffle::PartitionDemuxer demuxer(queue);
  demuxer.setMaxPartitions(3);
  demuxer.start();
  ASSERT_TRUE(demuxer.quiesce(std::chrono::seconds(2)));
  int64_t rows = 0;
  for (int32_t partition = 0; partition < 3; ++partition) {
    pangrova::shuffle::ShuffleBatch batch;
    while (demuxer.pop(partition, batch, milliseconds(10))) {
      rows += batch.row_count;
      ASSERT_NE(batch.data, nullptr);
    }
  }
  EXPECT_EQ(rows, 1);
  EXPECT_TRUE(demuxer.isFullyConsumed());
  demuxer.stop();
  exec.release();

  exec.cancel();
  EXPECT_EQ(exec.state(), ExecutorState::FINISHED);
  EXPECT_TRUE(queue->isCommitted());
}

TEST(ShuffleOutputTest, CancelBeforeCommitCannotPublishCleanEof) {
  auto config = makeShuffleConfig(1);
  TaskExecutor exec(config);
  auto* queue = exec.serializedShuffleQueue();
  ASSERT_NE(queue, nullptr);
  exec.cancel();
  EXPECT_EQ(exec.state(), ExecutorState::CANCELLED);
  EXPECT_TRUE(queue->isAborted());
  EXPECT_FALSE(queue->commit());
}

TEST(ShuffleOutputTest, DeadlineBeforeCommitCannotPublishCleanEof) {
  auto config = makeShuffleConfig(1);
  const auto now_ms = std::chrono::duration_cast<milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  config.deadline_epoch_ms = now_ms - 1;
  TaskExecutor exec(config);
  auto* queue = exec.serializedShuffleQueue();
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(exec.state(), ExecutorState::FAILED);
  EXPECT_TRUE(queue->isAborted());
  EXPECT_FALSE(queue->commit());
}

TEST(ShuffleOutputTest, OutputQueueEmptyWhenShuffleEnabled) {
  auto config = makeShuffleConfig(2);
  TaskExecutor exec(config);
  exec.start();
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
  ASSERT_EQ(exec.state(), ExecutorState::FINISHED) << exec.errorMessage();

  // With shuffle enabled, output_queue should NOT receive the batch.
  facebook::velox::RowVectorPtr row_vector;
  int rc = exec.getNextBatch(row_vector);
  EXPECT_NE(rc, kDataAvailable);
}

TEST(ShuffleOutputTest, NoShuffleRoutesToOutputQueue) {
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "no-shuffle-task";
  config.query_id = "no-shuffle-query";
  config.substrait_plan = buildValuesPlan();
  config.num_shuffle_partitions = 0;
  config.queue_poll_timeout = milliseconds(500);

  TaskExecutor exec(config);
  exec.start();

  // Poll output queue for the RowVector.
  facebook::velox::RowVectorPtr row_vector;
  int rc = kNotReady;
  auto deadline = std::chrono::steady_clock::now() + milliseconds(2000);
  while (rc == kNotReady &&
         std::chrono::steady_clock::now() < deadline) {
    rc = exec.getNextBatch(row_vector);
  }
  EXPECT_EQ(rc, kDataAvailable);
  ASSERT_NE(row_vector, nullptr);
  EXPECT_EQ(row_vector->size(), 1);

  // Wait for terminal state before destructor to avoid SEGFAULT from
  // Velox Task::terminate racing with resource cleanup.
  ASSERT_TRUE(waitForTerminal(exec, milliseconds(2000)));
}

TEST(ShuffleOutputTest, SerializedShuffleQueueCreatedWhenPartitionsSet) {
  auto config = makeShuffleConfig(5);
  TaskExecutor exec(config);

  // SerializedShuffleQueue should be created when num_shuffle_partitions > 0.
  EXPECT_NE(exec.serializedShuffleQueue(), nullptr);
}

TEST(ShuffleOutputTest, NoSerializedShuffleQueueWhenZeroPartitions) {
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "no-shuffle-task";
  config.query_id = "no-shuffle-query";
  config.substrait_plan = buildValuesPlan();
  config.num_shuffle_partitions = 0;
  config.queue_poll_timeout = milliseconds(500);

  TaskExecutor exec(config);

  // No shuffle queue when num_shuffle_partitions == 0.
  EXPECT_EQ(exec.serializedShuffleQueue(), nullptr);
}

#endif  // PANGROVA_HAS_SUBSTRAIT_PROTO

}  // namespace
}  // namespace pangrova::executor::test
