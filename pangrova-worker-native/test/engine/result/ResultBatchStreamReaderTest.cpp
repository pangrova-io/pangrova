#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/io/IOBuf.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <exception>
#include <memory>
#include <future>
#include <thread>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <openssl/evp.h>

#include "TestMemoryHelper.h"
#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/result/ResultBatchStreamReader.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "engine/velox/runner/VeloxTaskRunner.h"
#include "engine/velox/serde/CanonicalArrowSchema.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/SerializedPage.h"
#include "velox/exec/Task.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "engine/shuffle/ShuffleTicketKeys.h"
#include "engine/shuffle/TicketSigner.h"

namespace pangrova::result::test {
namespace {
using namespace facebook::velox;
namespace declared = pangrova::velox::schema;

class ActualProducerFailure final : public std::exception {
 public:
  const char* what() const noexcept override { return "actual producer failure"; }
};

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
    previous_current_key_ = shuffle::ShuffleTicketKeys::instance().currentKey();
    previous_old_key_ = shuffle::ShuffleTicketKeys::instance().previousKey();
    shuffle::ShuffleTicketKeys::instance().set(kTicketKey, "");
  }

  void TearDown() override {
    reader_.reset();
    if (runner_) {
      runner_->cancel();
      if (runner_pause_owned_) {
        // This fixture owns the explicit pause. The SDK leaves paused
        // off-thread drivers for that owner to close after termination.
        exec::Task::resume(runner_->veloxTask()->shared_from_this());
        runner_pause_owned_ = false;
      }
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
    if (server_) server_->stop();
    shuffle::ShuffleTicketKeys::instance().set(previous_current_key_, previous_old_key_);
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

  // Pause the real SDK Task before starting its actual drivers. Its actual
  // PartitionedOutputNode initializes the real route but cannot enqueue a page.
  folly::SemiFuture<bool> startPausedRunner() {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto values = maker.rowVector({"__column_0"},
        {maker.flatVector<int64_t>(std::vector<int64_t>{42})});
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
    auto pause = runner_->veloxTask()->requestPause();
    runner_pause_owned_ = true;
    auto paused = std::move(pause).via(pangrova::jni::globalExecutor().get());
    paused.wait(std::chrono::seconds(2));
    EXPECT_TRUE(paused.isReady());
    return runner_->startAsync();
  }

  template <typename Predicate>
  bool awaitActual(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
  }

  arrow::Result<std::unique_ptr<arrow::flight::FlightStreamReader>> rpcReader(
      const protocol::v2::ExecutionCallbackContext& admitted) {
    ARROW_ASSIGN_OR_RAISE(auto location,
        arrow::flight::Location::ForGrpcTcp("127.0.0.1", server_->boundPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location));
    clients_.push_back(std::move(client));
    const auto authority = grant(admitted);
    const auto bytes = authority.SerializeAsString();
    std::string encoded(4 * ((bytes.size() + 2) / 3) + 1, '\0');
    const int length = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()),
        reinterpret_cast<const unsigned char*>(bytes.data()), static_cast<int>(bytes.size()));
    encoded.resize(length);
    const auto signed_ticket = shuffle::TicketSigner(kTicketKey).signWithExpiry(
        "pangrova.result.read.v1:" + encoded, authority.expires_at_epoch_ms());
    return clients_.back()->DoGet(arrow::flight::Ticket{signed_ticket.ticket});
  }

  std::string task_id_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<memory::ColumnarMemoryPool> allocator_;
  std::unique_ptr<FlightResultServer> server_;
  std::shared_ptr<exec::OutputBufferManager> buffers_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<pangrova::velox::VeloxTaskRunner> runner_;
  bool runner_pause_owned_{false};
  std::shared_ptr<arrow::RecordBatchReader> reader_;
  std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients_;
  std::string previous_current_key_, previous_old_key_;
  inline static const std::string kTicketKey = "result-reader-real-ticket-key-012345678901";
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

TEST_F(ResultBatchStreamReaderTest, ActualSdkFailureAfterRouteRemovalWakesPendingReaderWithoutEof) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  open(info->authority);
  auto read = std::async(std::launch::async, [reader = reader_] {
    std::shared_ptr<arrow::RecordBatch> batch;
    const auto status = reader->ReadNext(&batch);
    return std::make_pair(status, batch);
  });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->hasPendingRead(); }));
  // This invokes SDK Task::terminate/maybeRemoveFromOutputBufferManager, not
  // a fabricated result callback. Defer the real runner completion until the
  // actual SDK route is already gone, reproducing the production failure race.
  runner_->veloxTask()->setError(std::make_exception_ptr(ActualProducerFailure{}));
  EXPECT_FALSE(buffers_->stats(task_id_).has_value());
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_FALSE(std::move(joined).get());
  EXPECT_NE(runner_->errorMessage().find("actual producer failure"), std::string::npos);
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  const auto result = read.get();
  EXPECT_FALSE(result.first.ok());
  EXPECT_EQ(result.second, nullptr);
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
  reader_.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
}

TEST_F(ResultBatchStreamReaderTest, ActualFlightCancellationWakesSdkFetchAndRetiresItsReaderLease) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto stream = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return stream->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->hasPendingRead(); }));
  stream->Cancel();
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_FALSE(read.get().ok());
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 0; }));
  EXPECT_TRUE(info->ownership->evidence().authority_revoked);
  EXPECT_FALSE(info->ownership->evidence().drained());
  runner_->cancel();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

TEST_F(ResultBatchStreamReaderTest, ActualFlightShutdownRevokesPendingFetchBeforeJoiningTransport) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto stream = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return stream->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->hasPendingRead(); }));
  auto stopped = std::async(std::launch::async, [&] { server_->stop(); });
  ASSERT_EQ(stopped.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_NO_THROW(stopped.get());
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_FALSE(read.get().ok());
  EXPECT_EQ(info->ownership->evidence().active_readers, 0u);
  EXPECT_EQ(info->ownership->evidence().retained_batch_owners, 0u);
  EXPECT_FALSE(FlightResultServer::lookupTaskOutputInfo(task_id_).has_value());
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  runner_->cancel();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

TEST_F(ResultBatchStreamReaderTest, ConcurrentSignedTicketReplayCannotReplaceTheActualPendingReader) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto first = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return first->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->hasPendingRead(); }));
  auto replay = rpcReader(info->authority);
  if (replay.ok()) {
    EXPECT_FALSE((*replay)->Next().ok());
  } else {
    EXPECT_FALSE(replay.status().ok());
  }
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
  EXPECT_TRUE(info->ownership->hasPendingRead());
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
  first->Cancel();
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_FALSE(read.get().ok());
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 0; }));
  runner_->cancel();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

TEST_F(ResultBatchStreamReaderTest, ReaderCanExitAfterItsActualServiceOwnerWithoutGlobalLookup) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  ASSERT_EQ(info->buffers, buffers_);
  open(info->authority);
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
  server_->stop();
  server_.reset();
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  runner_->cancel();
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  std::shared_ptr<arrow::RecordBatch> output;
  EXPECT_TRUE(reader_->ReadNext(&output).IsCancelled());
  EXPECT_EQ(output, nullptr);
  reader_.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

TEST_F(ResultBatchStreamReaderTest, ActualSdkDataCallbackRacingRevocationCannotCompleteTwice) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  open(info->authority);
  auto read = std::async(std::launch::async, [reader = reader_] {
    std::shared_ptr<arrow::RecordBatch> output;
    auto status = reader->ReadNext(&output);
    return std::make_pair(status, output);
  });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->hasPendingRead(); }));
  std::barrier start(3);
  auto data = std::async(std::launch::async, [&] {
    start.arrive_and_wait();
    stage({batch(info->wire_schema, 73)});
  });
  auto revoked = std::async(std::launch::async, [&] {
    start.arrive_and_wait();
    FlightResultServer::unregisterTaskOutputType(task_id_);
  });
  start.arrive_and_wait();
  EXPECT_NO_THROW(data.get());
  EXPECT_NO_THROW(revoked.get());
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  auto result = read.get();
  if (result.first.ok()) {
    ASSERT_NE(result.second, nullptr);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(result.second->column(0))->Value(0), 73);
  } else {
    EXPECT_EQ(result.second, nullptr);
  }
  runner_->cancel();
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  reader_.reset();
  result.second.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

}  // namespace
}  // namespace pangrova::result::test
