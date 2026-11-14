// Protocol/state boundary tests use the real lifecycle, ingress, executor and
// serializer. They do not claim a distributed SQL or JNI execution proof.
#include "CanonicalTestAttempt.h"
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <atomic>
#include <chrono>
#include <new>
#include <thread>
#include <vector>

#include "engine/executor/driver/TaskExecutor.h"
#include "engine/executor/driver/TaskLifecycle.h"
#include "command/StatusSerializer.h"
#include "engine/velox/exchange/ExternalShuffleIngress.h"

namespace pangrova::executor {
namespace {
protocol::v2::ExecutionError corruption() {
  protocol::v2::ExecutionError error;
  error.set_error_class(protocol::v2::ERROR_CLASS_DATA_CORRUPTION);
  error.set_code("EXTERNAL_SHUFFLE_CORRUPTION");
  error.set_message("partition stream ended before its terminal frame");
  error.set_retryable(false);
  error.set_failed_component("external-shuffle-ingress");
  return error;
}

protocol::v2::TaskReport report(TaskExecutor& executor) {
  const auto bytes = jni::serializeTaskStatus(executor);
  protocol::v2::TaskReport value;
  EXPECT_TRUE(value.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
  return value;
}

TEST(CanonicalFailureEvidenceTest, ActualSerializerRetainsAcceptedFailureFields) {
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "failure-evidence-task";
  config.query_id = "failure-evidence-query";
  pangrova::test::bindTestAttempt(config);
  TaskExecutor executor(config);
  const auto failure = corruption();
  executor.recordExecutionError(failure);
  const auto pending = report(executor);
  EXPECT_EQ(pending.state(), protocol::v2::TASK_STATE_PENDING);
  EXPECT_FALSE(pending.has_error());
  executor.notifyTaskFailed("Velox propagated the ingress exception");
  const auto failed = report(executor);
  ASSERT_EQ(failed.state(), protocol::v2::TASK_STATE_FAILED);
  EXPECT_EQ(failed.error().SerializeAsString(), failure.SerializeAsString());
  EXPECT_EQ(failed.report_origin(), protocol::v2::TASK_REPORT_ORIGIN_NATIVE);
  EXPECT_EQ(failed.attempt().task_id(), config.task_id);
}

TEST(CanonicalFailureEvidenceTest, ProviderUnknownOutcomeRemainsTerminalAndNonRetryable) {
  TaskExecutorConfig config;
  config.function_registry =
      facebook::velox::exec::FunctionRegistrySnapshot::capture();
  config.task_id = "model-unknown-task";
  config.query_id = "model-unknown-query";
  pangrova::test::bindTestAttempt(config);
  TaskExecutor executor(config);
  protocol::v2::ExecutionError uncertain;
  uncertain.set_error_class(protocol::v2::ERROR_CLASS_PROVIDER_OUTCOME_UNCERTAIN);
  uncertain.set_code("AI_PROVIDER_OUTCOME_UNCERTAIN");
  uncertain.set_message("model outcome unknown after response loss");
  uncertain.set_retryable(false);
  uncertain.set_failed_component("ai-invocation");

  executor.recordExecutionError(uncertain);
  executor.notifyTaskFailed("Velox propagated model callback failure");

  const auto failed = report(executor);
  ASSERT_EQ(failed.state(), protocol::v2::TASK_STATE_FAILED);
  ASSERT_TRUE(failed.has_error());
  EXPECT_EQ(failed.error().SerializeAsString(), uncertain.SerializeAsString());
  EXPECT_FALSE(failed.error().retryable());
  EXPECT_EQ(failed.coverage().completed_work_units(), 0);
}

TEST(CanonicalFailureEvidenceTest, AcceptedIngressFailureRecordsBeforeConsumption) {
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count() + 10000);
  velox::ExternalShuffleIngress ingress(1024, 1024, deadline);
  TaskLifecycle lifecycle;
  const auto failure = corruption();
  ingress.fail(failure.message(), [&] { lifecycle.recordExecutionError(failure); });
  const auto consumed = ingress.take(std::chrono::milliseconds(1));
  ASSERT_EQ(consumed.kind, velox::ExternalShuffleIngress::ResultKind::kFailed);
  EXPECT_EQ(consumed.error, failure.message());
  ASSERT_TRUE(lifecycle.snapshot().error.has_value());
  EXPECT_EQ(lifecycle.snapshot().error->SerializeAsString(), failure.SerializeAsString());
  TaskLifecycle rejected;
  EXPECT_THROW(ingress.fail("duplicate", [&] {
    rejected.recordExecutionError(failure);
  }), std::logic_error);
  EXPECT_FALSE(rejected.snapshot().error.has_value());
}

TEST(CanonicalFailureEvidenceTest, AllocationFailureBeforeAcceptanceDoesNotCloseIngress) {
  const auto deadline = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count() + 10000);
  velox::ExternalShuffleIngress ingress(1024, 1024, deadline);
  // This is an acceptance-callback allocation boundary, not a JVM OOME oracle.
  EXPECT_THROW(ingress.fail("allocation failure", [] { throw std::bad_alloc(); }),
               std::bad_alloc);
  EXPECT_EQ(ingress.take(std::chrono::microseconds(1)).kind,
            velox::ExternalShuffleIngress::ResultKind::kPending);
  EXPECT_NO_THROW(ingress.finish(-1));
}

TEST(CanonicalFailureEvidenceTest, ConcurrentFirstFailureAndCleanupPreserveClassification) {
  TaskLifecycle lifecycle;
  const auto first = corruption();
  lifecycle.recordExecutionError(first);
  std::atomic<bool> inconsistent{false};
  std::thread observer([&] {
    for (int i = 0; i < 1000; ++i) {
      const auto snapshot = lifecycle.snapshot();
      if (snapshot.state == ExecutorState::FAILED &&
          (!snapshot.error.has_value() || snapshot.error->code() != first.code())) {
        inconsistent.store(true);
      }
    }
  });
  std::vector<std::thread> failures;
  for (int i = 0; i < 8; ++i) {
    failures.emplace_back([&] {
      auto later = first;
      later.set_error_class(protocol::v2::ERROR_CLASS_TRANSPORT);
      later.set_retryable(true);
      lifecycle.recordExecutionError(later);
      lifecycle.notifyTaskFailed("later native exception");
    });
  }
  for (auto& failure : failures) failure.join();
  observer.join();
  lifecycle.appendError("private stage cleanup failed");
  const auto snapshot = lifecycle.snapshot();
  ASSERT_TRUE(snapshot.error.has_value());
  EXPECT_FALSE(inconsistent.load());
  EXPECT_EQ(snapshot.state, ExecutorState::FAILED);
  EXPECT_EQ(snapshot.error->error_class(), first.error_class());
  EXPECT_EQ(snapshot.error->code(), first.code());
  EXPECT_EQ(snapshot.error->retryable(), first.retryable());
  EXPECT_EQ(snapshot.error->failed_component(), first.failed_component());
  EXPECT_EQ(snapshot.error->message(), first.message() + "; private stage cleanup failed");
}

TEST(CanonicalFailureEvidenceTest, CancelledCleanupRetainsActualTransportRetryPolicy) {
  TaskLifecycle lifecycle;
  auto transport = corruption();
  transport.set_error_class(protocol::v2::ERROR_CLASS_TRANSPORT);
  transport.set_code("EXTERNAL_SHUFFLE_READ_FAILED");
  transport.set_retryable(true);
  lifecycle.recordExecutionError(transport);
  ASSERT_TRUE(lifecycle.transitionTo(ExecutorState::CANCELLED));
  lifecycle.appendError("reader drain failed");
  const auto snapshot = lifecycle.snapshot();
  ASSERT_TRUE(snapshot.error.has_value());
  EXPECT_EQ(snapshot.state, ExecutorState::CANCELLED);
  EXPECT_EQ(snapshot.error->error_class(), transport.error_class());
  EXPECT_EQ(snapshot.error->code(), transport.code());
  EXPECT_TRUE(snapshot.error->retryable());
  EXPECT_EQ(snapshot.error->failed_component(), transport.failed_component());
}

TEST(CanonicalFailureEvidenceTest, MalformedFailureCannotTakeFirstErrorOwnership) {
  TaskLifecycle lifecycle;
  auto invalid = corruption();
  invalid.clear_failed_component();
  EXPECT_THROW(lifecycle.recordExecutionError(invalid), std::invalid_argument);
  EXPECT_FALSE(lifecycle.snapshot().error.has_value());
  invalid = corruption();
  invalid.GetReflection()->MutableUnknownFields(&invalid)->AddVarint(999, 1);
  EXPECT_THROW(lifecycle.recordExecutionError(invalid), std::invalid_argument);
  EXPECT_FALSE(lifecycle.snapshot().error.has_value());
  lifecycle.notifyTaskFailed("actual uncategorized native failure");
  const auto snapshot = lifecycle.snapshot();
  ASSERT_TRUE(snapshot.error.has_value());
  EXPECT_EQ(snapshot.error->error_class(), protocol::v2::ERROR_CLASS_INTERNAL);
  EXPECT_EQ(snapshot.error->code(), "NATIVE_TASK_FAILED");
  EXPECT_EQ(snapshot.error->failed_component(), "native-executor");
  EXPECT_FALSE(snapshot.error->retryable());
}
}  // namespace
}  // namespace pangrova::executor
