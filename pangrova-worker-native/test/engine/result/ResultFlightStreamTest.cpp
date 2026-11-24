#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

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
#include "engine/result/FlightResultServer.h"
#include "engine/result/ResultFlightStream.h"
#include "engine/result/ResultPayloadProducer.h"
#include "engine/velox/plan/CanonicalPlanSchema.h"
#include "engine/velox/runner/VeloxTaskRunner.h"
#include "engine/velox/serde/CanonicalArrowSchema.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
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

// Actual SDK task and signed gRPC Flight transport.
class ResultFlightStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> next{0};
    task_id_ = "typed-result-reader-" + std::to_string(next.fetch_add(1));
    pool_ = pangrova::test::testLeafPool(task_id_);
    allocator_ = std::make_shared<memory::ColumnarMemoryPool>(pool_);
    auto config = FlightResultServerConfig{};
    config.result_fetch_timeout_seconds = 10;
    server_ = std::make_unique<FlightResultServer>(config);
    server_->start();
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
    info.queue = std::make_shared<ResultPayloadQueue>(1024 * 1024);
    info.authority = authority();
    return info;
  }

  void open(const protocol::v2::ExecutionCallbackContext& admitted) {
    auto opened = server_->openTaskOutput(grant(admitted));
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    reader_ = std::move(*opened);
  }

  void registerReady(const FlightResultServer::TaskOutputInfo& info) {
    FlightResultServer::registerTaskOutputType(task_id_, info);
    ASSERT_TRUE(FlightResultServer::markTaskOutputReady(task_id_));
  }

  arrow::Result<bool> stage(const FlightResultServer::TaskOutputInfo& info, int64_t value) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    auto rows = maker.rowVector({"__column_0"},
        {maker.flatVector<int64_t>(std::vector<int64_t>{value})});
    ResultPayloadProducer producer(canonical(), info.wire_schema, {}, allocator_, info.ownership);
    ARROW_ASSIGN_OR_RAISE(auto frames, producer.prepare(rows, {}));
    return info.queue->tryPush(frames.front());
  }

  void prepareRunner(std::vector<RowVectorPtr> values, bool pause, int64_t windowBytes = 1024 * 1024) {
    auto source = std::make_shared<core::ValuesNode>("values", std::move(values));
    pangrova::velox::VeloxTaskRunnerConfig config;
    config.task_id = task_id_;
    config.function_registry = exec::FunctionRegistrySnapshot::capture();
    config.canonical_output_schema = std::make_shared<const substrait::NamedStruct>(canonical());
    config.callback_context = authority().SerializeAsString();
    config.result_column_labels = {"client_alias"};
    config.is_result_task = true;
    config.serialized_output_buffer_threshold_bytes = windowBytes;
    runner_ = std::make_shared<pangrova::velox::VeloxTaskRunner>(config, source,
        nullptr);
    runner_->start({});
    if (pause) {
      auto pending = runner_->veloxTask()->requestPause();
      runner_pause_owned_ = true;
      auto paused = std::move(pending).via(pangrova::jni::globalExecutor().get());
      paused.wait(std::chrono::seconds(2));
      EXPECT_TRUE(paused.isReady());
    }
  }

  std::vector<RowVectorPtr> rowBatches(const std::vector<std::vector<int64_t>>& values) {
    facebook::velox::test::VectorMaker maker(pool_.get());
    std::vector<RowVectorPtr> result;
    for (const auto& batch : values) {
      result.push_back(maker.rowVector({"__column_0"}, {maker.flatVector<int64_t>(batch)}));
    }
    return result;
  }

  void preparePausedRunner() {
    prepareRunner(rowBatches({{42}}), true);
  }

  folly::SemiFuture<bool> startPausedRunner() {
    preparePausedRunner();
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
  std::shared_ptr<pangrova::velox::VeloxTaskRunner> runner_;
  bool runner_pause_owned_{false};
  std::unique_ptr<arrow::flight::FlightDataStream> reader_;
  std::vector<std::unique_ptr<arrow::flight::FlightClient>> clients_;
  std::string previous_current_key_, previous_old_key_;
  inline static const std::string kTicketKey = "result-reader-real-ticket-key-012345678901";
};

TEST_F(ResultFlightStreamTest, SignedFlightWaitsForRealProducerReadiness) {
  preparePausedRunner();
  const auto registered = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(registered.has_value());
  ASSERT_TRUE(registered->pool);
  ASSERT_FALSE(registered->ready);
  EXPECT_FALSE(server_->openTaskOutput(grant(registered->authority)).ok());
  auto started = std::promise<void>();
  auto entered = started.get_future();
  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  using Clock = std::chrono::steady_clock;
  auto schemaRead = std::async(std::launch::async, [&] {
    started.set_value();
    auto opened = rpcReader(registered->authority);
    if (!opened.ok()) return std::pair{opened.status(), Clock::now()};
    stream = std::move(*opened);
    const auto schema = stream->GetSchema();
    return std::pair{schema.status(), Clock::now()};
  });
  ASSERT_EQ(entered.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  EXPECT_EQ(schemaRead.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
  int notifications = 0;
  auto completion = runner_->startAsync([&] {
    ++notifications;
    const auto readable = FlightResultServer::lookupTaskOutputInfo(task_id_);
    ASSERT_TRUE(readable.has_value());
    EXPECT_TRUE(readable->ready);
  }).via(pangrova::jni::globalExecutor().get());
  ASSERT_EQ(notifications, 1);
  ASSERT_TRUE(FlightResultServer::lookupTaskOutputInfo(task_id_)->ready);
  const auto ready = Clock::now();
  const auto [status, schemaReady] = schemaRead.get();
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_FALSE(server_->openTaskOutput(grant(registered->authority)).ok());
  RecordProperty("ready_to_schema_us", std::to_string(
      std::chrono::duration_cast<std::chrono::microseconds>(schemaReady - ready).count()));

  exec::Task::resume(runner_->veloxTask()->shared_from_this());
  runner_pause_owned_ = false;
  ASSERT_NE(stream, nullptr);
  auto data = stream->Next();
  ASSERT_TRUE(data.ok()) << data.status().ToString();
  ASSERT_NE(data->data, nullptr);
  const auto values = std::static_pointer_cast<arrow::Int64Array>(data->data->column(0));
  ASSERT_EQ(values->length(), 1);
  EXPECT_EQ(values->Value(0), 42);
  completion.wait(std::chrono::seconds(2));
  ASSERT_TRUE(completion.isReady());
  ASSERT_TRUE(std::move(completion).get());
  ASSERT_TRUE(runner_->commitResultOutput());
  data = stream->Next();
  ASSERT_TRUE(data.ok()) << data.status().ToString();
  EXPECT_EQ(data->data, nullptr);
  stream.reset();
  EXPECT_TRUE(awaitActual([&] { return runner_->resultDrainEvidence().drained(); }));
}

TEST_F(ResultFlightStreamTest, ActualRunnerKeepsLogicalWireNamesRequiredFlagsAndPresentationLabelsSeparate) {
  prepareRunner(rowBatches({{42, 73}}), false);
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  ASSERT_NE(info->wire_schema, nullptr);
  EXPECT_EQ(info->output_type->nameOf(0), "__column_0");
  EXPECT_EQ(info->wire_schema->field(0)->name(), "logical_value");
  EXPECT_FALSE(info->wire_schema->field(0)->nullable());
  auto completion = runner_->startAsync().via(pangrova::jni::globalExecutor().get());
  auto wrong = info->authority;
  wrong.mutable_attempt()->set_control_epoch(wrong.attempt().control_epoch() + 1);
  auto denied = rpcReader(wrong);
  const auto denial = denied.ok() ? (*denied)->GetSchema().status() : denied.status();
  EXPECT_FALSE(denial.ok());
  completion.wait(std::chrono::seconds(10));
  ASSERT_TRUE(completion.isReady());
  ASSERT_TRUE(std::move(completion).get()) << runner_->errorMessage();
  ASSERT_TRUE(runner_->commitResultOutput());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto stream = std::move(*opened);
  auto schema = stream->GetSchema();
  ASSERT_TRUE(schema.ok()) << schema.status().ToString();
  EXPECT_EQ((*schema)->field(0)->name(), "client_alias");
  EXPECT_FALSE((*schema)->field(0)->nullable());
  std::vector<int64_t> actual;
  while (true) {
    auto data = stream->Next();
    ASSERT_TRUE(data.ok()) << data.status().ToString();
    if (!data->data) break;
    EXPECT_EQ(data->data->schema()->field(0)->name(), "client_alias");
    auto column = std::static_pointer_cast<arrow::Int64Array>(data->data->column(0));
    for (int64_t row = 0; row < column->length(); ++row) actual.push_back(column->Value(row));
  }
  EXPECT_EQ(actual, (std::vector<int64_t>{42, 73}));
  EXPECT_EQ(runner_->veloxTask()->numRunningDrivers(), 0);
  stream.reset();
  EXPECT_TRUE(awaitActual([&] { return runner_->resultDrainEvidence().drained(); }));
}

TEST_F(ResultFlightStreamTest, FullQueueSuspendsRealDriverUntilTransportReleasesCredit) {
  prepareRunner(rowBatches({{41}, {42}, {43}}), false, 1);
  auto completion = runner_->startAsync().via(pangrova::jni::globalExecutor().get());
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  ASSERT_TRUE(awaitActual([&] {
    return info->queue->retainedBytes() > 1 && info->ownership->evidence().retained_batch_owners == 2;
  }));
  open(info->authority);
  ASSERT_NE(reader_, nullptr);
  auto result = reader_->Next();
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_NE(result->ipc_message.metadata, nullptr);
  EXPECT_FALSE(completion.isReady());
  auto first = std::move(*result);
  auto transportAlias = first.ipc_message.body_buffers.back();
  first = {};
  EXPECT_FALSE(completion.isReady());
  transportAlias.reset();
  for (int index = 0; index < 2; ++index) {
    auto payload = reader_->Next();
    ASSERT_TRUE(payload.ok()) << payload.status().ToString();
    ASSERT_NE(payload->ipc_message.metadata, nullptr);
  }
  completion.wait(std::chrono::seconds(2));
  ASSERT_TRUE(completion.isReady());
  ASSERT_TRUE(std::move(completion).get());
  ASSERT_TRUE(runner_->commitResultOutput());
  auto eof = reader_->Next();
  ASSERT_TRUE(eof.ok()) << eof.status().ToString();
  EXPECT_EQ(eof->ipc_message.metadata, nullptr);
  EXPECT_TRUE(reader_->Close().ok());
  reader_.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
}

TEST_F(ResultFlightStreamTest, ProducerSplitsOverCeilingWithoutPublishingPartialInvalidBatches) {
  auto info = registration();
  const auto longInput = rowBatches({std::vector<int64_t>(128, 123)});
  const auto one = rowBatches({{7}});
  ResultPayloadProducer probe(canonical(), info.wire_schema, {}, allocator_, info.ownership);
  auto measured = probe.prepare(one.front(), {}).ValueOrDie();
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  const int64_t ceiling = arrow::ipc::GetPayloadSize(measured.front().flightPayload().ipc_message, options);
  measured.clear();
  ResultPayloadProducer bounded(canonical(), info.wire_schema, {}, allocator_, info.ownership, ceiling * 2);
  auto frames = bounded.prepare(longInput.front(), {}).ValueOrDie();
  ASSERT_GT(frames.size(), 1);
  for (const auto& frame : frames) {
    EXPECT_LE(arrow::ipc::GetPayloadSize(frame.flightPayload().ipc_message, options), ceiling * 2);
  }
  frames.clear();
  ResultPayloadProducer unfit(canonical(), info.wire_schema, {}, allocator_, info.ownership, ceiling - 1);
  auto rejected = unfit.prepare(one.front(), {});
  EXPECT_TRUE(rejected.status().IsCapacityError());
  EXPECT_EQ(info.queue->retainedBytes(), 0);
  EXPECT_EQ(info.ownership->evidence().retained_batch_owners, 0u);
}

TEST_F(ResultFlightStreamTest, MissingAuthoritativeWireSchemaCannotBeRegisteredOrRead) {
  auto info = registration();
  info.wire_schema.reset();
  EXPECT_THROW(FlightResultServer::registerTaskOutputType(task_id_, info), std::invalid_argument);
  info.claimed = true;
  EXPECT_FALSE(resultFlightStream(std::move(info), 10, nullptr).ok());
}

TEST_F(ResultFlightStreamTest, LaterNullInRequiredBatchProducesNoPayload) {
  auto info = registration();
  facebook::velox::test::VectorMaker maker(pool_.get());
  auto rows = maker.rowVector({"__column_0"}, {maker.flatVectorNullable<int64_t>({42, std::nullopt})});
  ResultPayloadProducer producer(canonical(), info.wire_schema, {}, allocator_, info.ownership);
  auto result = producer.prepare(rows, {});
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(info.ownership->evidence().retained_batch_owners, 0u);
  EXPECT_EQ(info.queue->retainedBytes(), 0);
}

TEST_F(ResultFlightStreamTest, ConflictingPresentationTypeCannotBeServed) {
  auto info = registration();
  info.result_schema = arrow::schema({arrow::field("text", arrow::utf8(), false)});
  info.claimed = true;
  EXPECT_TRUE(resultFlightStream(std::move(info), 10, nullptr).status().IsTypeError());
}

TEST_F(ResultFlightStreamTest, EarlyCloseReportsFailureBeforeReturningCredits) {
  auto info = registration();
  ASSERT_TRUE(stage(info, 42).ValueOrDie());
  int reports = 0;
  info.report_stream_failure = [&](const std::string& failure) {
    ++reports;
    EXPECT_NE(failure.find("before committed EOF"), std::string::npos);
    EXPECT_GT(info.queue->retainedBytes(), 0);
  };
  registerReady(info);
  open(info.authority);
  ASSERT_NE(reader_, nullptr);
  reader_.reset();
  EXPECT_EQ(reports, 1);
  const auto ownership = info.ownership->evidence();
  EXPECT_TRUE(ownership.authority_revoked);
  EXPECT_TRUE(ownership.queue_cleared);
  EXPECT_EQ(ownership.active_readers, 0u);
  EXPECT_EQ(info.queue->retainedBytes(), 0);
  EXPECT_FALSE(ownership.drained());
}

TEST_F(ResultFlightStreamTest, OnlyExplicitTransportCloseCommitsPreparedEof) {
  auto info = registration();
  int commits = 0;
  int failures = 0;
  info.report_stream_finished = [&] { ++commits; };
  info.report_stream_failure = [&](const std::string& message) {
    ++failures;
    EXPECT_NE(message.find("closed before committed EOF"), std::string::npos);
  };
  info.claimed = true;
  ASSERT_TRUE(info.queue->seal().ok());
  auto stream = resultFlightStream(info, 10, nullptr).ValueOrDie();
  ASSERT_FALSE(stream->Next().ValueOrDie().ipc_message.metadata);
  EXPECT_EQ(commits, 0);
  ASSERT_TRUE(stream->Close().ok());
  stream.reset();
  EXPECT_EQ(commits, 1);
  EXPECT_EQ(failures, 0);

  auto abandoned = registration();
  abandoned.report_stream_finished = info.report_stream_finished;
  abandoned.report_stream_failure = info.report_stream_failure;
  abandoned.claimed = true;
  ASSERT_TRUE(abandoned.queue->seal().ok());
  stream = resultFlightStream(abandoned, 10, nullptr).ValueOrDie();
  ASSERT_FALSE(stream->Next().ValueOrDie().ipc_message.metadata);
  stream.reset();
  EXPECT_EQ(commits, 1);
  EXPECT_EQ(failures, 1);
}

TEST_F(ResultFlightStreamTest, RevocationDiscardsPayloadsAndPreservesTheFailureOwner) {
  auto info = registration();
  info.report_stream_failure = [](const std::string& failure) {
    ADD_FAILURE() << "Revoked result reported a second failure: " << failure;
  };
  ASSERT_TRUE(stage(info, 42).ValueOrDie());
  registerReady(info);
  open(info.authority);
  ASSERT_NE(reader_, nullptr);
  // A different attempt with the same task identifier cannot revoke this owner.
  FlightResultServer::unregisterTaskOutputType(task_id_, std::make_shared<ResultOwnershipState>());
  ASSERT_TRUE(FlightResultServer::lookupTaskOutputInfo(task_id_).has_value());
  FlightResultServer::unregisterTaskOutputType(task_id_, info.ownership);
  EXPECT_TRUE(reader_->Next().status().IsCancelled());
  EXPECT_EQ(info.queue->retainedBytes(), 0);
}

TEST_F(ResultFlightStreamTest, ActualSdkFailureWakesPendingReaderWithoutEof) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  open(info->authority);
  auto read = std::async(std::launch::async, [reader = reader_.get()] { return reader->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 1; }));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
  // Defer the real runner continuation until the SDK Task is already failed.
  runner_->veloxTask()->setError(std::make_exception_ptr(ActualProducerFailure{}));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_FALSE(std::move(joined).get());
  EXPECT_NE(runner_->errorMessage().find("actual producer failure"), std::string::npos);
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  const auto result = read.get();
  EXPECT_TRUE(result.status().IsExecutionError());
  EXPECT_NE(result.status().ToString().find("actual producer failure"), std::string::npos);
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
  reader_.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
}

TEST_F(ResultFlightStreamTest, ActualFlightCancellationWakesPendingReadAndRetiresItsLease) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto stream = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return stream->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 1; }));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
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

TEST_F(ResultFlightStreamTest, ActualFlightShutdownRevokesPendingFetchBeforeJoiningTransport) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto stream = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return stream->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 1; }));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
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

TEST_F(ResultFlightStreamTest, ConcurrentSignedTicketReplayCannotReplaceTheActualPendingReader) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  auto opened = rpcReader(info->authority);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto first = std::move(*opened);
  auto read = std::async(std::launch::async, [&] { return first->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 1; }));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
  auto replay = rpcReader(info->authority);
  if (replay.ok()) {
    EXPECT_FALSE((*replay)->Next().ok());
  } else {
    EXPECT_FALSE(replay.status().ok());
  }
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
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

TEST_F(ResultFlightStreamTest, ReaderCanExitAfterItsActualServiceOwnerWithoutGlobalLookup) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  open(info->authority);
  EXPECT_EQ(info->ownership->evidence().active_readers, 1u);
  server_->stop();
  server_.reset();
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  runner_->cancel();
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  EXPECT_TRUE(reader_->Next().status().IsCancelled());
  reader_.reset();
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

TEST_F(ResultFlightStreamTest, PublicationRacingRevocationCannotDeliverTwice) {
  auto completion = startPausedRunner();
  const auto info = FlightResultServer::lookupTaskOutputInfo(task_id_);
  ASSERT_TRUE(info.has_value());
  open(info->authority);
  auto read = std::async(std::launch::async, [reader = reader_.get()] { return reader->Next(); });
  ASSERT_TRUE(awaitActual([&] { return info->ownership->evidence().active_readers == 1; }));
  EXPECT_EQ(read.wait_for(std::chrono::milliseconds(17)), std::future_status::timeout);
  std::barrier start(3);
  auto data = std::async(std::launch::async, [&] {
    start.arrive_and_wait();
    auto published = stage(*info, 73);
    EXPECT_TRUE(published.ok() ? *published : published.status().IsCancelled());
  });
  auto revoked = std::async(std::launch::async, [&] {
    start.arrive_and_wait();
    FlightResultServer::unregisterTaskOutputType(task_id_, info->ownership);
  });
  start.arrive_and_wait();
  EXPECT_NO_THROW(data.get());
  EXPECT_NO_THROW(revoked.get());
  ASSERT_EQ(read.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  auto result = read.get();
  if (result.ok()) {
    ASSERT_NE(result->ipc_message.metadata, nullptr);
  } else {
    EXPECT_TRUE(result.status().IsCancelled());
  }
  EXPECT_TRUE(reader_->Next().status().IsCancelled());
  runner_->cancel();
  EXPECT_FALSE(runner_->resultDrainEvidence().drained());
  reader_.reset();
  result = arrow::Status::Cancelled("Test released the transport message");
  EXPECT_TRUE(runner_->resultDrainEvidence().drained());
  auto joined = std::move(completion).via(pangrova::jni::globalExecutor().get());
  joined.wait(std::chrono::seconds(2));
  ASSERT_TRUE(joined.isReady());
  EXPECT_TRUE(std::move(joined).get());
}

}  // namespace
}  // namespace pangrova::result::test
