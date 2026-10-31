#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"
#include <chrono>

#include "engine/executor/driver/TaskExecutor.h"
#include "bridge/command/NativeCommandDispatcher.h"
#include "bridge/registry/HandleRegistry.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {
namespace {
namespace proto = protocol::v2;
class NativeResourceDrainControlTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto* identity = context_.mutable_attempt();
    identity->set_task_id("resource-owner");
    identity->set_query_or_job_id("resource-query");
    identity->set_stage_id(2);
    identity->set_partition_id(3);
    identity->set_plan_epoch(4);
    identity->set_attempt_id(5);
    identity->set_fencing_token(6);
    identity->set_control_epoch(7);
    identity->mutable_execution_owner()->set_owner_id("owner");
    identity->mutable_execution_owner()->set_owner_incarnation("incarnation");
    identity->mutable_execution_owner()->set_fencing_token(8);
    context_.mutable_security()->set_tenant_id("tenant");
    context_.mutable_security()->set_principal("principal");
    context_.mutable_security()->add_active_roles("reader");
    context_.mutable_security()->set_policy_context_hash("policy");
    executor::TaskExecutorConfig config;
    config.function_registry =
        facebook::velox::exec::FunctionRegistrySnapshot::capture();
    config.task_id = identity->task_id();
    config.query_id = identity->query_or_job_id();
    config.is_result_task = true;
    config.callback_context = context_.SerializeAsString();
    executor_ = std::make_shared<executor::TaskExecutor>(config);
    handle_ = HandleRegistry::instance().create(executor_);
  }
  void TearDown() override { HandleRegistry::instance().release(handle_); }
  proto::RuntimeCommand command() const {
    proto::RuntimeCommand value;
    value.mutable_protocol()->set_major(2);
    *value.mutable_attempt() = context_.attempt();
    value.set_command_type(proto::RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE);
    proto::ResourceDrainEvidenceRequest request;
    *request.mutable_security() = context_.security();
    request.set_resource(proto::RESOURCE_DRAIN_SCOPE_RESULT_STREAM);
    value.mutable_payload()->PackFrom(request);
    return value;
  }
  proto::RuntimeCommandResponse dispatch(const proto::RuntimeCommand& value) const {
    const auto wire = value.SerializeAsString();
    const auto result = dispatchCommandWithHandle({wire.begin(), wire.end()}, handle_);
    proto::RuntimeCommandResponse response;
    EXPECT_TRUE(response.ParseFromArray(result.data(), result.size()));
    return response;
  }
  proto::ExecutionCallbackContext context_;
  std::shared_ptr<executor::TaskExecutor> executor_;
  int64_t handle_ = 0;
};

TEST_F(NativeResourceDrainControlTest, ActualPendingExecutorIsNotDrainedAndCancellationStopsProducer) {
  auto response = dispatch(command());
  ASSERT_TRUE(response.success()) << response.error().message();
  proto::ResourceDrainEvidence evidence;
  ASSERT_TRUE(response.payload().UnpackTo(&evidence));
  EXPECT_EQ(evidence.attempt().SerializeAsString(), context_.attempt().SerializeAsString());
  EXPECT_EQ(evidence.security().SerializeAsString(), context_.security().SerializeAsString());
  EXPECT_FALSE(evidence.producer_stopped());
  EXPECT_FALSE(evidence.drained());
  executor_->cancel();
  response = dispatch(command());
  ASSERT_TRUE(response.success());
  ASSERT_TRUE(response.payload().UnpackTo(&evidence));
  EXPECT_TRUE(evidence.producer_stopped());
  EXPECT_TRUE(evidence.authority_revoked());
  EXPECT_TRUE(evidence.queue_cleared());
  EXPECT_EQ(evidence.active_readers(), 0);
  EXPECT_EQ(evidence.retained_batch_owners(), 0);
  EXPECT_TRUE(evidence.drained());
}

TEST_F(NativeResourceDrainControlTest, ExactLiveAttemptAndFullSecurityRequiredForResourceEvidence) {
  for (int changedField = 0; changedField < 8; ++changedField) {
    auto value = command();
    proto::ResourceDrainEvidenceRequest request;
    ASSERT_TRUE(value.payload().UnpackTo(&request));
    switch (changedField) {
      case 0: value.mutable_attempt()->set_task_id("other"); break;
      case 1: value.mutable_attempt()->set_attempt_id(9); break;
      case 2: value.mutable_attempt()->mutable_execution_owner()->set_fencing_token(9); break;
      case 3: request.mutable_security()->set_tenant_id("other"); break;
      case 4: request.mutable_security()->set_principal("other"); break;
      case 5: request.mutable_security()->clear_active_roles(); break;
      case 6: request.mutable_security()->set_policy_context_hash("other"); break;
      case 7: request.set_resource(proto::RESOURCE_DRAIN_SCOPE_UNSPECIFIED); break;
    }
    value.mutable_payload()->PackFrom(request);
    EXPECT_FALSE(dispatch(value).success()) << changedField;
  }
  auto missingAttempt = command();
  missingAttempt.clear_attempt();
  EXPECT_FALSE(dispatch(missingAttempt).success());
}
}  // namespace
}  // namespace pangrova::jni
