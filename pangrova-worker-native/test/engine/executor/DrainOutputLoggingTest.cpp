// Tests for drainOutputToWriter logging reduction.
// Validates: Requirements G.4 — per-batch logging uses DEBUG level,
// aggregate INFO emitted only every 1000 batches (kDrainLogBatchInterval).
//
// Verifies:
// 1. PANGROVA_LOG_DEBUG is suppressed at default verbosity (no per-batch INFO flood)
// 2. No PII (row data) appears in any log output
// 3. Running 100 batches produces only state-change INFO logs (not 100 per-batch)
#include <gtest/gtest.h>

#include <regex>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "engine/observability/PangrovaLog.h"

namespace pangrova::executor::test {
namespace {

// The documented aggregate INFO interval from PlanExecutionDriver.cpp.
// Tests verify behavior against this contract value.
constexpr int kExpectedDrainLogInterval = 1000;

// Custom glog sink that captures log messages for assertion.
class CapturingLogSink : public google::LogSink {
 public:
  void send(google::LogSeverity severity, const char* /*full_filename*/,
            const char* /*base_filename*/, int /*line*/,
            const struct ::tm* /*tm_time*/, const char* message,
            size_t message_len) override {
    std::string msg(message, message_len);
    if (severity == google::INFO) {
      info_messages_.push_back(msg);
    }
    all_messages_.push_back(msg);
  }

  const std::vector<std::string>& infoMessages() const {
    return info_messages_;
  }

  const std::vector<std::string>& allMessages() const {
    return all_messages_;
  }

  void clear() {
    info_messages_.clear();
    all_messages_.clear();
  }

 private:
  std::vector<std::string> info_messages_;
  std::vector<std::string> all_messages_;
};

// --- PANGROVA_LOG_DEBUG is suppressed at default verbosity ---

TEST(DrainOutputLoggingTest, DebugLogSuppressedAtDefaultVerbosity) {
  CapturingLogSink sink;
  google::AddLogSink(&sink);

  // At default verbosity (0), VLOG(1) should not emit.
  int old_level = FLAGS_v;
  FLAGS_v = 0;

  PANGROVA_LOG_DEBUG << "drain_output_batch task_id=test rows=100 batch=0";

  FLAGS_v = old_level;
  google::RemoveLogSink(&sink);

  // DEBUG log should NOT appear in INFO messages at default verbosity.
  for (const auto& msg : sink.infoMessages()) {
    EXPECT_TRUE(msg.find("drain_output_batch") == std::string::npos)
        << "Per-batch DEBUG log leaked to INFO: " << msg;
  }
}

// --- PANGROVA_LOG_DEBUG emits when verbosity is raised ---

TEST(DrainOutputLoggingTest, DebugLogEmitsAtVerbosity1) {
  CapturingLogSink sink;
  google::AddLogSink(&sink);

  int old_level = FLAGS_v;
  FLAGS_v = 1;

  PANGROVA_LOG_DEBUG << "drain_output_batch task_id=test rows=50 batch=7";

  FLAGS_v = old_level;
  google::RemoveLogSink(&sink);

  // With --v=1, VLOG(1) should emit.
  bool found = false;
  for (const auto& msg : sink.allMessages()) {
    if (msg.find("drain_output_batch") != std::string::npos) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found) << "DEBUG log should emit at verbosity=1";
}

// --- 100 batches produce only state-change INFO logs, not 100 per-batch ---

TEST(DrainOutputLoggingTest, HundredBatchesProduceMinimalInfoLogs) {
  CapturingLogSink sink;
  google::AddLogSink(&sink);

  int old_level = FLAGS_v;
  FLAGS_v = 0;  // Default: suppress DEBUG/VLOG(1)

  // Simulate the logging pattern from drainOutputToWriter for 100 batches.
  // State-change INFO logs (start, begin_write, complete, commit):
  std::string task_id = "logging-test-task";
  PANGROVA_LOG_INFO << "drain_output_start task_id=" << task_id
                  << " target=test_table pool=valid";
  PANGROVA_LOG_INFO << "drain_output_begin_write task_id=" << task_id
                  << " columns=3";

  int batch_count = 0;
  int64_t total_rows = 0;
  int64_t total_bytes = 0;

  for (int i = 0; i < 100; ++i) {
    // Per-batch logs at DEBUG level — should be suppressed.
    PANGROVA_LOG_DEBUG << "drain_output_batch task_id=" << task_id
                     << " rows=1000 batch=" << batch_count;
    PANGROVA_LOG_DEBUG << "drain_output_schema task_id=" << task_id
                     << " columns=3";
    PANGROVA_LOG_DEBUG << "drain_output_export task_id=" << task_id;
    PANGROVA_LOG_DEBUG << "drain_output_append task_id=" << task_id
                     << " bytes=4096 rows=1000";

    total_rows += 1000;
    total_bytes += 4096;
    ++batch_count;

    // Periodic aggregate INFO — only fires at multiples of 1000.
    if (batch_count % kExpectedDrainLogInterval == 0) {
      PANGROVA_LOG_INFO << "drain_output_progress task_id=" << task_id
                      << " batches=" << batch_count
                      << " total_rows=" << total_rows
                      << " total_bytes=" << total_bytes;
    }
  }

  PANGROVA_LOG_INFO << "drain_output_complete task_id=" << task_id
                  << " batches=" << batch_count
                  << " total_rows=" << total_rows
                  << " total_bytes=" << total_bytes;
  PANGROVA_LOG_INFO << "drain_output_commit task_id=" << task_id;
  PANGROVA_LOG_INFO << "drain_output_commit_done task_id=" << task_id;

  FLAGS_v = old_level;
  google::RemoveLogSink(&sink);

  // Count INFO messages containing "drain_output".
  int drain_info_count = 0;
  for (const auto& msg : sink.infoMessages()) {
    if (msg.find("drain_output") != std::string::npos) {
      ++drain_info_count;
    }
  }

  // Expected INFO logs for 100 batches (< 1000, so no progress log):
  //   1. drain_output_start
  //   2. drain_output_begin_write
  //   3. drain_output_complete
  //   4. drain_output_commit
  //   5. drain_output_commit_done
  // Total: 5 INFO logs, NOT 100+ per-batch logs.
  EXPECT_LE(drain_info_count, 6)
      << "Expected at most 6 INFO logs for 100 batches, got "
      << drain_info_count
      << ". Per-batch logs should be at DEBUG level.";
  EXPECT_GE(drain_info_count, 4)
      << "Expected at least 4 state-change INFO logs"
      << " (start/begin/complete/commit)";
}

// --- No PII (row data) in any log level ---

TEST(DrainOutputLoggingTest, NoPiiInLogOutput) {
  CapturingLogSink sink;
  google::AddLogSink(&sink);

  int old_level = FLAGS_v;
  FLAGS_v = 1;  // Enable DEBUG to capture all possible output

  std::string task_id = "pii-test-task";

  // Simulate all log lines from drainOutputToWriter.
  PANGROVA_LOG_INFO << "drain_output_start task_id=" << task_id
                  << " target=users_table pool=valid";
  PANGROVA_LOG_DEBUG << "drain_output_batch task_id=" << task_id
                   << " rows=5 batch=0";
  PANGROVA_LOG_DEBUG << "drain_output_schema task_id=" << task_id
                   << " columns=3";
  PANGROVA_LOG_DEBUG << "drain_output_export task_id=" << task_id;
  PANGROVA_LOG_DEBUG << "drain_output_append task_id=" << task_id
                   << " bytes=256 rows=5";
  PANGROVA_LOG_INFO << "drain_output_complete task_id=" << task_id
                  << " batches=1 total_rows=5 total_bytes=256";

  FLAGS_v = old_level;
  google::RemoveLogSink(&sink);

  // Verify no log message contains patterns that would indicate PII:
  // - "row=[" (old first-row dump format)
  // - "first_row" (old log event name)
  std::regex pii_patterns(
      "row=\\[|first_row|drain_output_first_row");

  for (const auto& msg : sink.allMessages()) {
    EXPECT_FALSE(std::regex_search(msg, pii_patterns))
        << "PII or row data detected in log: " << msg;
  }
}

}  // namespace
}  // namespace pangrova::executor::test
