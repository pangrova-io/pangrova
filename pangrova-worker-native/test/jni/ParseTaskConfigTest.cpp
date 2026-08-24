// Tests for parseTaskConfig: TaskRequest protobuf → TaskExecutorConfig.
#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "executor/driver/TaskExecutor.h"
#include "jni/command/TaskRequestParser.h"

// Include generated proto when available.
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

// --- Empty / invalid input ---

TEST(ParseTaskConfigTest, EmptyDataReturnsDefaults) {
  auto config = parseTaskConfig({});
  EXPECT_EQ(config.task_id, "task-unknown");
  EXPECT_EQ(config.query_id, "query-unknown");
  EXPECT_TRUE(config.substrait_plan.empty());
  EXPECT_TRUE(config.splits.empty());
  EXPECT_TRUE(config.session_properties.empty());
  EXPECT_TRUE(config.trace_id.empty());
}

TEST(ParseTaskConfigTest, GarbageDataThrowsRuntimeError) {
  std::vector<uint8_t> garbage = {0xFF, 0xFE, 0xFD};
  EXPECT_THROW(parseTaskConfig(garbage), std::runtime_error);
}

#if PANGROVA_HAS_PROTO

namespace proto = pangrova::native::protocol::v1;

std::vector<uint8_t> serialize(const google::protobuf::MessageLite& msg) {
  std::vector<uint8_t> buf(msg.ByteSizeLong());
  msg.SerializeToArray(buf.data(), static_cast<int>(buf.size()));
  return buf;
}

// --- Basic fields ---

TEST(ParseTaskConfigTest, ParsesTaskIdAndQueryId) {
  proto::TaskRequest req;
  req.set_task_id("task-42");
  req.set_query_id("query-7");
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.task_id, "task-42");
  EXPECT_EQ(config.query_id, "query-7");
}

TEST(ParseTaskConfigTest, ParsesSubstraitPlan) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  std::string plan_bytes = "\x0a\x0b\x0c\x0d";
  req.set_substrait_plan(plan_bytes);
  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.substrait_plan.size(), 4u);
  EXPECT_EQ(config.substrait_plan[0], 0x0a);
  EXPECT_EQ(config.substrait_plan[3], 0x0d);
}

TEST(ParseTaskConfigTest, ParsesTaskType) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  req.set_task_type(proto::TASK_TYPE_SHUFFLE_WRITE);
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.task_type,
            static_cast<int32_t>(proto::TASK_TYPE_SHUFFLE_WRITE));
}

// --- Session properties ---

TEST(ParseTaskConfigTest, ParsesSessionProperties) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  (*req.mutable_session_properties())["key1"] = "val1";
  (*req.mutable_session_properties())["key2"] = "val2";
  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.session_properties.size(), 2u);
  EXPECT_EQ(config.session_properties.at("key1"), "val1");
  EXPECT_EQ(config.session_properties.at("key2"), "val2");
}

// --- Resource limits ---

TEST(ParseTaskConfigTest, ParsesResourceLimit) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* rl = req.mutable_resource_limit();
  rl->set_max_memory_bytes(1024 * 1024 * 512);
  rl->set_max_threads(8);
  rl->set_max_spill_bytes(1024 * 1024 * 256);
  rl->set_spill_directory("/tmp/spill");
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.resource_limit.max_memory_bytes, 1024 * 1024 * 512);
  EXPECT_EQ(config.resource_limit.max_threads, 8);
  EXPECT_EQ(config.resource_limit.max_spill_bytes, 1024 * 1024 * 256);
  EXPECT_EQ(config.resource_limit.spill_directory, "/tmp/spill");
}

TEST(ParseTaskConfigTest, MissingResourceLimitUsesDefaults) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.resource_limit.max_memory_bytes, 0);
  EXPECT_EQ(config.resource_limit.max_threads, 0);
  EXPECT_EQ(config.resource_limit.max_spill_bytes, 0);
  EXPECT_EQ(config.resource_limit.spill_directory, "");
}

// --- Splits ---

TEST(ParseTaskConfigTest, ParsesBasicSplit) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  split->set_split_id("s1");
  split->set_connector_id("iceberg");
  split->set_file_path("/data/part-0.parquet");
  split->set_file_format(proto::FILE_FORMAT_PARQUET);
  split->set_start_offset(100);
  split->set_length(5000);

  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.splits.size(), 1u);
  const auto& s = config.splits[0];
  EXPECT_EQ(s.split_id, "s1");
  EXPECT_EQ(s.connector_id, "iceberg");
  EXPECT_EQ(s.file_path, "/data/part-0.parquet");
  EXPECT_EQ(s.file_format, split::FileFormatType::PARQUET);
  EXPECT_EQ(s.start_offset, 100);
  EXPECT_EQ(s.length, 5000);
}

TEST(ParseTaskConfigTest, ParsesOrcFormat) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  split->set_split_id("s1");
  split->set_file_format(proto::FILE_FORMAT_ORC);
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.splits[0].file_format, split::FileFormatType::ORC);
}

TEST(ParseTaskConfigTest, ParsesMultipleSplits) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  req.add_splits()->set_split_id("s1");
  req.add_splits()->set_split_id("s2");
  req.add_splits()->set_split_id("s3");
  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.splits.size(), 3u);
  EXPECT_EQ(config.splits[0].split_id, "s1");
  EXPECT_EQ(config.splits[2].split_id, "s3");
}

// --- Iceberg split info ---

TEST(ParseTaskConfigTest, ParsesIcebergSplitInfo) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  split->set_connector_id("iceberg");
  auto* ice = split->mutable_iceberg_info();
  ice->set_snapshot_id(12345);
  auto* df = ice->add_delete_files();
  df->set_file_path("/deletes/pos-del.parquet");
  df->set_delete_type(proto::DELETE_FILE_TYPE_POSITION);

  auto config = parseTaskConfig(serialize(req));
  ASSERT_TRUE(config.splits[0].iceberg.has_value());
  EXPECT_EQ(config.splits[0].iceberg->snapshot_id, 12345);
  ASSERT_EQ(config.splits[0].iceberg->delete_files.size(), 1u);
  EXPECT_EQ(config.splits[0].iceberg->delete_files[0].file_path,
            "/deletes/pos-del.parquet");
  EXPECT_EQ(config.splits[0].iceberg->delete_files[0].delete_type,
            "position");
}

TEST(ParseTaskConfigTest, ParsesIcebergEqualityDeleteFiles) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  auto* ice = split->mutable_iceberg_info();
  auto* df = ice->add_delete_files();
  df->set_delete_type(proto::DELETE_FILE_TYPE_EQUALITY);
  df->add_equality_fields("col_a");
  df->add_equality_fields("col_b");

  auto config = parseTaskConfig(serialize(req));
  const auto& del = config.splits[0].iceberg->delete_files[0];
  EXPECT_EQ(del.delete_type, "equality");
  ASSERT_EQ(del.equality_fields.size(), 2u);
  EXPECT_EQ(del.equality_fields[0], "col_a");
  EXPECT_EQ(del.equality_fields[1], "col_b");
}

// --- Paimon split info ---

TEST(ParseTaskConfigTest, ParsesPaimonSplitInfo) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  split->set_connector_id("paimon");
  auto* pai = split->mutable_paimon_info();
  pai->set_merge_engine(proto::PAIMON_MERGE_PARTIAL_UPDATE);
  pai->set_read_mode(proto::PAIMON_READ_MODE_CHANGELOG);
  pai->add_primary_keys("pk1");
  pai->add_primary_keys("pk2");

  auto config = parseTaskConfig(serialize(req));
  ASSERT_TRUE(config.splits[0].paimon.has_value());
  EXPECT_EQ(config.splits[0].paimon->merge_engine, "partial_update");
  EXPECT_TRUE(config.splits[0].paimon->changelog_mode);
  ASSERT_EQ(config.splits[0].paimon->primary_keys.size(), 2u);
  EXPECT_EQ(config.splits[0].paimon->primary_keys[0], "pk1");
}

// --- Column mapping ---

TEST(ParseTaskConfigTest, ParsesColumnMapping) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* split = req.add_splits();
  auto* cm = split->add_column_mapping();
  cm->set_source_field_id(1);
  cm->set_target_field_id(2);
  cm->set_source_name("old_col");
  cm->set_target_name("new_col");

  auto config = parseTaskConfig(serialize(req));
  ASSERT_EQ(config.splits[0].column_mappings.size(), 1u);
  EXPECT_EQ(config.splits[0].column_mappings[0].source_id, 1);
  EXPECT_EQ(config.splits[0].column_mappings[0].target_id, 2);
  EXPECT_EQ(config.splits[0].column_mappings[0].source_name, "old_col");
  EXPECT_EQ(config.splits[0].column_mappings[0].target_name, "new_col");
}

// --- Backward compatibility: empty task_id/query_id ---

TEST(ParseTaskConfigTest, EmptyFieldsGetDefaults) {
  proto::TaskRequest req;
  // Leave task_id and query_id empty.
  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.task_id, "task-unknown");
  EXPECT_EQ(config.query_id, "query-unknown");
}

// --- Write config ---

TEST(ParseTaskConfigTest, ParsesWriteConfig) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto* wc = req.mutable_write_config();
  wc->set_target_table("db.orders");
  wc->set_write_mode("APPEND");
  wc->add_partition_spec("region");
  wc->add_partition_spec("date");
  wc->set_transaction_id("tx-42");
  wc->set_file_format(proto::FILE_FORMAT_PARQUET);
  wc->set_target_file_size(128 * 1024 * 1024);

  auto config = parseTaskConfig(serialize(req));
  EXPECT_EQ(config.write_config.target_table, "db.orders");
  EXPECT_EQ(config.write_config.write_mode, "APPEND");
  ASSERT_EQ(config.write_config.partition_spec.size(), 2u);
  EXPECT_EQ(config.write_config.partition_spec[0], "region");
  EXPECT_EQ(config.write_config.partition_spec[1], "date");
  EXPECT_EQ(config.write_config.transaction_id, "tx-42");
  EXPECT_EQ(config.write_config.file_format, split::FileFormatType::PARQUET);
  EXPECT_EQ(config.write_config.target_file_size, 128 * 1024 * 1024);
}

TEST(ParseTaskConfigTest, MissingWriteConfigUsesDefaults) {
  proto::TaskRequest req;
  req.set_task_id("t1");
  req.set_query_id("q1");
  auto config = parseTaskConfig(serialize(req));
  EXPECT_TRUE(config.write_config.target_table.empty());
  EXPECT_TRUE(config.write_config.write_mode.empty());
  EXPECT_TRUE(config.write_config.partition_spec.empty());
  EXPECT_EQ(config.write_config.target_file_size, 0);
}

#endif  // PANGROVA_HAS_PROTO

}  // namespace
}  // namespace pangrova::jni::test
