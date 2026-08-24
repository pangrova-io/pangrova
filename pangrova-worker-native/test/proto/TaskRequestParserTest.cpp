// Tests for TaskRequestParser — proto deserialization round-trip and edge cases.
// Validates: Requirements 42.4
//
// TaskRequestParser converts TaskRequest protobuf bytes into
// TaskExecutorConfig. Tests verify field mapping, boundary values,
// and round-trip consistency.
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "executor/driver/TaskExecutor.h"
#include "jni/command/TaskRequestParser.h"

#if __has_include("pangrova/native/protocol/v1/task.pb.h")
#include "pangrova/native/protocol/v1/task.pb.h"
#include "pangrova/native/protocol/v1/split.pb.h"
#include "pangrova/native/protocol/v1/common.pb.h"
#define PANGROVA_HAS_PROTO 1
#else
#define PANGROVA_HAS_PROTO 0
#endif

namespace pangrova::jni::test {
namespace {

using executor::TaskExecutorConfig;

// --- Edge cases: empty and invalid input ---

TEST(TaskRequestParserTest, EmptyInputReturnsDefaults) {
  auto config = parseTaskConfig({});
  EXPECT_EQ(config.task_id, "task-unknown");
  EXPECT_EQ(config.query_id, "query-unknown");
  EXPECT_TRUE(config.substrait_plan.empty());
  EXPECT_TRUE(config.splits.empty());
}

TEST(TaskRequestParserTest, GarbageInputThrows) {
  std::vector<uint8_t> garbage = {0xDE, 0xAD, 0xBE, 0xEF};
  EXPECT_THROW(parseTaskConfig(garbage), std::runtime_error);
}

TEST(TaskRequestParserTest, SingleByteInputThrows) {
  EXPECT_THROW(parseTaskConfig({0x01}), std::runtime_error);
}

#if PANGROVA_HAS_PROTO

namespace proto = pangrova::native::protocol::v1;

std::vector<uint8_t> serialize(const google::protobuf::MessageLite& msg) {
  std::vector<uint8_t> buf(msg.ByteSizeLong());
  msg.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

// --- Round-trip: basic fields ---

TEST(TaskRequestParserTest, RoundTripTaskIdQueryId) {
  proto::TaskRequest req;
  req.set_task_id("task-abc-123");
  req.set_query_id("query-xyz-789");

  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.task_id, "task-abc-123");
  EXPECT_EQ(config.query_id, "query-xyz-789");
}

TEST(TaskRequestParserTest, RoundTripSubstraitPlan) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  std::string plan(256, '\xAB');
  req.set_substrait_plan(plan);

  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.substrait_plan.size(), 256u);
  for (auto byte : config.substrait_plan) {
    EXPECT_EQ(byte, 0xAB);
  }
}

// --- Boundary values ---

TEST(TaskRequestParserTest, MaxInt64ResourceLimit) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* rl = req.mutable_resource_limit();
  rl->set_max_memory_bytes(INT64_MAX);
  rl->set_max_threads(INT32_MAX);

  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.resource_limit.max_memory_bytes, INT64_MAX);
  EXPECT_EQ(config.resource_limit.max_threads, INT32_MAX);
}

TEST(TaskRequestParserTest, ZeroResourceLimits) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* rl = req.mutable_resource_limit();
  rl->set_max_memory_bytes(0);
  rl->set_max_threads(0);
  rl->set_max_spill_bytes(0);

  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.resource_limit.max_memory_bytes, 0);
  EXPECT_EQ(config.resource_limit.max_threads, 0);
  EXPECT_EQ(config.resource_limit.max_spill_bytes, 0);
}

// --- Multiple splits ---

TEST(TaskRequestParserTest, ManySplitsPreserveOrder) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  for (int i = 0; i < 100; ++i) {
    auto* split = req.add_splits();
    split->set_split_id("s" + std::to_string(i));
    split->set_file_path("/data/part-" + std::to_string(i) + ".parquet");
  }

  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.splits.size(), 100u);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(config.splits[i].split_id, "s" + std::to_string(i));
  }
}

// --- Session properties boundary ---

TEST(TaskRequestParserTest, EmptySessionProperties) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");

  auto config = parseTaskConfig(serialize(req));
  EXPECT_TRUE(config.session_properties.empty());
}

TEST(TaskRequestParserTest, LargeSessionPropertyValue) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  std::string largeValue(10000, 'x');
  (*req.mutable_session_properties())["big_key"] = largeValue;

  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.session_properties.at("big_key").size(), 10000u);
}


#endif  // PANGROVA_HAS_PROTO

}  // namespace
}  // namespace pangrova::jni::test
