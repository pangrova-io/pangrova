#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/io/IOBuf.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "../TestMemoryHelper.h"
#include "columnar/CanonicalBatchStream.h"
#include "result/ResultBatchStreamReader.h"
#include "velox/plan/CanonicalPlanSchema.h"
#include "velox/runner/VeloxTaskRunner.h"
#include "velox/serde/CanonicalArrowSchema.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/SerializedPage.h"
#include "velox/exec/Task.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace pangrova::result::test {
namespace {
using namespace facebook::velox;
namespace declared = pangrova::velox::schema;

// Actual task/output-buffer/reader integration, not distributed SQL E2E.
class ResultBatchStreamReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> next{0};
    task_id_ = "typed-result-reader-" + std::to_string(next.fetch_add(1));
    pool_ = pangrova::test::testLeafPool(task_id_);
    allocator_ = std::make_shared<memory::ColumnarMemoryPool>(pool_);
    server_ = std::make_unique<FlightResultServer>(FlightResultServerConfig{});
    server_->start();
    buffers_ = exec::OutputBufferManager::getInstanceRef();
  }

  void TearDown() override {
    reader_.reset();
    if (runner_) {
      runner_->cancel();
      runner_->releaseTaskAndWaitForDeletion();
      runner_.reset();
    }
    if (task_) {
      buffers_->removeTask(task_id_);
      auto joined = task_->requestCancel().via(pangrova::jni::globalExecutor().get());
      joined.wait(std::chrono::seconds(10));
      ASSERT_TRUE(joined.isReady());
      std::move(joined).get();
      task_.reset();
    }
    server_->stop();
  }

  substrait::NamedStruct canonical() const {
    return declared::named({declared::computed("logical_value",
        declared::primitive("i64", substrait::Type::NULLABILITY_REQUIRED))});
  }

  protocol::v2::ExecutionCallbackContext authority() const {
    protocol::v2::ExecutionCallbackContext value;
    value.mutable_attempt()->set_task_id(task_id_);
    value.mutable_attempt()->set_query_or_job_id("typed-result-query");
    value.mutable_attempt()->mutable_execution_owner()->set_owner_id("coordinator");
    value.mutable_attempt()->mutable_execution_owner()->set_owner_incarnation("test-incarnation");
    value.mutable_security()->set_tenant_id("tenant");
    value.mutable_security()->set_principal("reader");
    value.set_deadline_epoch_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count() + 60000);
    return value;
  }

  protocol::v2::ResultReadGrant grant(const protocol::v2::ExecutionCallbackContext& admitted) const {
    protocol::v2::ResultReadGrant value;
    *value.mutable_attempt() = admitted.attempt();
    *value.mutable_security() = admitted.security();
    value.set_expires_at_epoch_ms(admitted.deadline_epoch_ms());
    value.set_scope(protocol::v2::RESULT_READ_SCOPE_RESULT_STREAM);
    return value;
  }

  FlightResultServer::TaskOutputInfo registration() const {
    FlightResultServer::TaskOutputInfo info;
    info.output_type = ROW({"__column_0"}, {BIGINT()});
    info.wire_schema = pangrova::velox::canonicalArrowSchema(canonical(), pool_.get());
    info.column_labels = {"client_alias"};
    info.pool = allocator_;
    info.ownership = std::make_shared<ResultOwnershipState>();
    info.authority = authority();
    return info;
  }

  void open(const protocol::v2::ExecutionCallbackContext& admitted) {
    auto claimed = FlightResultServer::claimTaskOutputInfo(grant(admitted));
    ASSERT_TRUE(claimed.ok()) << claimed.status().ToString();
    auto opened = resultBatchStreamReader(*claimed, grant(admitted), 4 * 1024 * 1024, 10);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    reader_ = *opened;
  }

  std::shared_ptr<arrow::RecordBatch> batch(
      const std::shared_ptr<arrow::Schema>& schema, std::optional<int64_t> value) {
    arrow::Int64Builder builder(allocator_.get());
    const auto appended = value ? builder.Append(*value) : builder.AppendNull();
    EXPECT_TRUE(appended.ok());
    auto array = builder.Finish();
    EXPECT_TRUE(array.ok()) << array.status().ToString();
    return arrow::RecordBatch::Make(schema, 1, {*array});
  }

  void stage(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    auto encoded = columnar::encodeBatchStream(batches, allocator_);
    ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
    auto page = std::make_unique<exec::SerializedPage>(
        folly::IOBuf::copyBuffer((*encoded)->data(), (*encoded)->size()), nullptr,
        static_cast<int64_t>(batches.size()));
    buffers_->enqueue(task_id_, 0, std::move(page), nullptr);
  }

  void prepareInjectedPages() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto source = std::make_shared<core::ValuesNode>("values",
        std::vector<RowVectorPtr>{maker.rowVector({"__column_0"},
            {maker.flatVector<int64_t>(std::vector<int64_t>{42})})});
    core::QueryConfig query({{"require_function_registry_snapshot", "true"}},
        exec::FunctionRegistrySnapshot::capture());
    task_ = exec::Task::create(task_id_, core::PlanFragment{source}, 0,
        core::QueryCtx::create(pangrova::jni::globalExecutor().get(), std::move(query)),
        exec::Task::ExecutionMode::kParallel);
    buffers_->initializeTask(task_, core::PartitionedOutputNode::Kind::kPartitioned, 1, 1);
  }

  void expectRejectedWithoutCommit(const FlightResultServer::TaskOutputInfo& info) {
    buffers_->noMoreData(task_id_);
    FlightResultServer::registerTaskOutputType(task_id_, info);
    open(info.authority);
    ASSERT_NE(reader_, nullptr);
    std::shared_ptr<arrow::RecordBatch> output;
    const auto first = reader_->ReadNext(&output);
    ASSERT_FALSE(first.ok());
    EXPECT_EQ(output, nullptr);
    auto stats = buffers_->stats(task_id_);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->bufferedPages, 2);
    EXPECT_EQ(info.ownership->evidence().retained_batch_owners, 0u);
    const auto repeated = reader_->ReadNext(&output);
    EXPECT_EQ(repeated.ToString(), first.ToString());
    EXPECT_EQ(output, nullptr);
    EXPECT_EQ(buffers_->stats(task_id_)->bufferedPages, 2);
  }

  std::string task_id_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<memory::ColumnarMemoryPool> allocator_;
  std::unique_ptr<FlightResultServer> server_;
  std::shared_ptr<exec::OutputBufferManager> buffers_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<pangrova::velox::VeloxTaskRunner> runner_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
};

TEST_F(ResultBatchStreamReaderTest, ActualRunnerKeepsLogicalWireNamesRequiredFlagsAndPresentationLabelsSeparate) {
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto values = maker.rowVector({"__column_0"},
      {maker.flatVector<int64_t>(std::vector<int64_t>{42, 73})});
  auto source = std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{values});
  auto output = core::PartitionedOutputNode::single("result-output", values->rowType(), "ArrowBatch", source);
  pangrova::velox::VeloxTaskRunnerConfig config;
  config.task_id = task_id_;
  config.function_registry = exec::FunctionRegistrySnapshot::capture();
  config.canonical_output_schema = std::make_shared<const substrait::NamedStruct>(canonical());
  config.serde_schema_bindings.emplace(output->id(), canonical());
  config.callback_context = authority().SerializeAsString();
  config.result_column_labels = {"client_alias"};
  config.is_result_task = true;
  config.serialized_output_buffer_threshold_bytes = 1024 * 1024;
  runner_ = std::make_shared<pangrova::velox::VeloxTaskRunner>(config, output,
      std::vector<split::SplitConfig>{}, nullptr);
  runner_->start();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  ASSERT_NE(info->wire_schema, nullptr);
  EXPECT_EQ(info->output_type->nameOf(0), "__column_0");
  EXPECT_EQ(info->wire_schema->field(0)->name(), "logical_value");
  EXPECT_FALSE(info->wire_schema->field(0)->nullable());
  auto completion = runner_->startAsync().via(pangrova::jni::globalExecutor().get());
  open(info->authority);
  ASSERT_NE(reader_, nullptr);
  EXPECT_EQ(reader_->schema()->field(0)->name(), "client_alias");
  EXPECT_FALSE(reader_->schema()->field(0)->nullable());
  std::vector<int64_t> actual;
  std::shared_ptr<arrow::RecordBatch> result;
  while (true) {
    const auto status = reader_->ReadNext(&result);
    ASSERT_TRUE(status.ok()) << status.ToString();
    if (!result) break;
    EXPECT_EQ(result->schema()->field(0)->name(), "client_alias");
    auto column = std::static_pointer_cast<arrow::Int64Array>(result->column(0));
    for (int64_t row = 0; row < column->length(); ++row) actual.push_back(column->Value(row));
  }
  EXPECT_EQ(actual, (std::vector<int64_t>{42, 73}));
  completion.wait(std::chrono::seconds(10));
  ASSERT_TRUE(completion.isReady());
  EXPECT_TRUE(std::move(completion).get()) << runner_->errorMessage();
  EXPECT_EQ(runner_->veloxTask()->numRunningDrivers(), 0);
}

TEST_F(ResultBatchStreamReaderTest, MissingAuthoritativeWireSchemaCannotBeRegisteredOrRead) {
  auto info = registration();
  info.wire_schema.reset();
  EXPECT_THROW(FlightResultServer::registerTaskOutputType(task_id_, info), std::invalid_argument);
  EXPECT_FALSE(resultBatchStreamReader(info, grant(info.authority), 1024, 10).ok());
}

TEST_F(ResultBatchStreamReaderTest, LatePageRequiredNullCannotPublishEarlierPageOrAdvanceAcknowledgement) {
  prepareInjectedPages();
  auto info = registration();
  stage({batch(info.wire_schema, 42)});
  stage({batch(info.wire_schema, std::nullopt)});
  expectRejectedWithoutCommit(info);
}

TEST_F(ResultBatchStreamReaderTest, LatePageSchemaDriftCannotPublishEarlierPageOrAdvanceAcknowledgement) {
  prepareInjectedPages();
  auto info = registration();
  stage({batch(info.wire_schema, 42)});
  auto drifted = arrow::schema({info.wire_schema->field(0)->WithNullable(true)});
  stage({batch(drifted, 73)});
  expectRejectedWithoutCommit(info);
}

TEST_F(ResultBatchStreamReaderTest, LateBatchRequiredNullRejectsTheEntireCanonicalPage) {
  auto info = registration();
  auto encoded = columnar::encodeBatchStream(
      {batch(info.wire_schema, 42), batch(info.wire_schema, std::nullopt)}, allocator_);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  auto parsed = detail::parseArrowBatchFrames(info.wire_schema,
      (*encoded)->data(), (*encoded)->size(), allocator_);
  EXPECT_FALSE(parsed.ok());
}

TEST_F(ResultBatchStreamReaderTest, RevokedLiveRegistrationCannotPublishOrAcknowledgeQueuedRows) {
  prepareInjectedPages();
  auto info = registration();
  stage({batch(info.wire_schema, 42)});
  buffers_->noMoreData(task_id_);
  FlightResultServer::registerTaskOutputType(task_id_, info);
  open(info.authority);
  ASSERT_NE(reader_, nullptr);
  FlightResultServer::unregisterTaskOutputType(task_id_);
  std::shared_ptr<arrow::RecordBatch> output;
  EXPECT_TRUE(reader_->ReadNext(&output).IsCancelled());
  EXPECT_EQ(output, nullptr);
  const auto stats = buffers_->stats(task_id_);
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->bufferedPages, 1);
}

}  // namespace
}  // namespace pangrova::result::test
