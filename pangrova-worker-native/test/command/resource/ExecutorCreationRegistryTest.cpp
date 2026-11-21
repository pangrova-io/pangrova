// Actual Native registry/config/executor ownership boundaries; not JNI OOME or SQL E2E proof.
#include <gtest/gtest.h>
#include "velox/expression/FunctionRegistrySnapshot.h"

#include <future>
#include <thread>
#include <stdexcept>

#include "TestMemoryHelper.h"
#include "capabilities/capability/FunctionCatalogAssembler.h"
#include "capabilities/capability/NativeCapabilityInventory.h"
#include "jni/RuntimeInitializer.h"
#include "command/NativeCommandDispatcher.h"
#include "command/resource/NativeCreationControl.h"
#include "command/task/TaskRequestParser.h"
#include "command/resource/HandleRegistry.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"

namespace pangrova::jni {
namespace {
namespace proto = protocol::v2;

proto::ProtocolVersion actualProtocol() {
  proto::RuntimeCommand command;
  command.mutable_protocol()->set_major(2);
  command.set_command_type(proto::RUNTIME_COMMAND_TYPE_GET_CAPABILITIES);
  const auto wire = command.SerializeAsString();
  const auto bytes = dispatchCommand({wire.begin(), wire.end()});
  proto::RuntimeCommandResponse response;
  proto::CapabilityResponse capabilities;
  if (!response.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())) || !response.success() ||
      !response.payload().UnpackTo(&capabilities) || !capabilities.has_inventory()) {
    throw std::runtime_error("Actual Native capability inventory unavailable");
  }
  return capabilities.inventory().protocol();
}

proto::ExecutorCreationRecoveryRequest identity(const std::string& task_id) {
  proto::ExecutorCreationRecoveryRequest request;
  auto* seal = request.mutable_admission_seal();
  *seal->mutable_protocol() = actualProtocol();
  seal->set_deadline_epoch_ms(4102444800000ULL);
  seal->mutable_canonical_request_checksum()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
  seal->mutable_canonical_request_checksum()->set_digest(std::string(32, 'd'));
  auto* attempt = seal->mutable_attempt();
  attempt->set_query_or_job_id("creation-query");
  attempt->set_task_id(task_id);
  attempt->set_plan_epoch(1);
  attempt->set_fencing_token(2);
  attempt->set_control_epoch(3);
  attempt->mutable_execution_owner()->set_owner_id("coordinator");
  attempt->mutable_execution_owner()->set_owner_incarnation("actual-process");
  attempt->mutable_execution_owner()->set_fencing_token(4);
  request.mutable_security()->set_tenant_id("tenant");
  request.mutable_security()->set_principal("principal");
  request.mutable_security()->add_active_roles("reader");
  request.mutable_security()->set_policy_context_hash("policy");
  return request;
}

class ExecutorCreationRegistryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    HandleRegistry::instance().destroyAll();
    NativeRuntimeConfig config;
    config.max_memory_bytes = 256L * 1024 * 1024;
    config.io_thread_count = 2;
    config.num_drivers = 1;
    config.cache_enabled = false;
    initializeVeloxRuntime(config);

    const auto registry = facebook::velox::exec::FunctionRegistrySnapshot::capture();
    const auto assembled = capability::FunctionCatalogAssembler::assemble(
        registry, capability::runtimeCapabilityInventory(*registry));
    const auto& inventory = assembled->inventory();
    proto::FunctionCatalogFinalizeRequest request;
    *request.mutable_protocol() = inventory.protocol();
    request.set_binding_declaration_sha256(inventory.function_catalog().binding_declaration_sha256());
    request.set_execution_selection_sha256(inventory.function_catalog().execution_selection_sha256());
    proto::RuntimeCommand command;
    *command.mutable_protocol() = request.protocol();
    command.set_command_type(proto::RUNTIME_COMMAND_TYPE_FINALIZE_FUNCTION_CATALOG);
    command.mutable_payload()->PackFrom(request);
    const auto wire = command.SerializeAsString();
    const auto bytes = dispatchCommand({wire.begin(), wire.end()});
    proto::RuntimeCommandResponse response;
    ASSERT_TRUE(response.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));
    ASSERT_TRUE(response.success()) << response.error().message();
    proto::FunctionCatalogFinalizeResult result;
    ASSERT_TRUE(response.payload().UnpackTo(&result));
    EXPECT_EQ(result.manifest_sha256(), inventory.function_catalog().manifest_sha256());
  }

  void TearDown() override {
    HandleRegistry::instance().destroyAll();
    shutdownVeloxRuntime();
    pangrova::test::detail::ensureRuntimeInitialized();
  }
};

TEST_F(ExecutorCreationRegistryTest, ActualOwnerRecoversAfterResponsePublicationIsLost) {
  auto request = identity("lost-response");
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.reserveCreation(request);
  executor::TaskExecutorConfig config;
  config.function_registry =
      NativeEngineLifecycle::instance().functionCatalogStartup()->requirePublished()->registry();
  config.task_id = request.admission_seal().attempt().task_id();
  config.query_id = request.admission_seal().attempt().query_or_job_id();
  auto task = std::make_shared<executor::TaskExecutor>(config);
  registry.attachCreation(handle, task);
  EXPECT_EQ(registry.recoverCreation(request).state(), proto::EXECUTOR_CREATION_STATE_CONSTRUCTING);
  EXPECT_THROW(registry.acquire(handle), std::runtime_error);
  registry.finishCreation(handle);
  const auto recovered = registry.recoverCreation(request);
  ASSERT_EQ(recovered.state(), proto::EXECUTOR_CREATION_STATE_OWNER_PRESENT);
  ASSERT_EQ(recovered.executor_handle(), static_cast<uint64_t>(handle));
  EXPECT_EQ(registry.acquire(handle), task);
  EXPECT_FALSE(task->isExecutionReleased());
  registry.release(handle);
  EXPECT_TRUE(task->isExecutionReleased());
  EXPECT_EQ(registry.size(), 0);
}

TEST_F(ExecutorCreationRegistryTest, NativeConfigFailureOnlyClosesAfterActualConstructionScopeExit) {
  auto request = identity("config-rejection");
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.reserveCreation(request);
  proto::TaskRequest invalid;
  // The real decoder rejects this configuration, not an injected business failure.
  EXPECT_THROW(taskConfigFromRequest(invalid), std::runtime_error);
  EXPECT_EQ(registry.recoverCreation(request).state(), proto::EXECUTOR_CREATION_STATE_CONSTRUCTING);
  registry.finishCreation(handle);
  const auto recovered = registry.recoverCreation(request);
  EXPECT_EQ(recovered.state(), proto::EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
  EXPECT_EQ(recovered.executor_handle(), 0);
  EXPECT_THROW(registry.reserveCreation(request), std::runtime_error);
}

TEST_F(ExecutorCreationRegistryTest, PreParseSealPreventsLaterNativeConstruction) {
  auto request = identity("before-parse");
  auto& registry = HandleRegistry::instance();
  const auto result = registry.recoverCreation(request);
  EXPECT_EQ(result.state(), proto::EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
  EXPECT_EQ(result.identity().SerializeAsString(), request.SerializeAsString());
  EXPECT_THROW(registry.reserveCreation(request), std::runtime_error);
  EXPECT_EQ(registry.size(), 0);
}

TEST_F(ExecutorCreationRegistryTest, ExactSecurityDigestAndDeadlineCannotReplaceRetainedOwner) {
  const auto request = identity("strict-owner");
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.reserveCreation(request);
  auto changed = request;
  changed.mutable_security()->set_tenant_id("other-tenant");
  EXPECT_THROW(registry.recoverCreation(changed), std::runtime_error);
  changed = request;
  changed.mutable_admission_seal()->mutable_canonical_request_checksum()->set_digest(std::string(32, 'x'));
  EXPECT_THROW(registry.recoverCreation(changed), std::runtime_error);
  changed = request;
  changed.mutable_admission_seal()->set_deadline_epoch_ms(request.admission_seal().deadline_epoch_ms() + 1);
  EXPECT_THROW(registry.recoverCreation(changed), std::runtime_error);
  EXPECT_EQ(registry.recoverCreation(request).state(), proto::EXECUTOR_CREATION_STATE_CONSTRUCTING);
  registry.finishCreation(handle);
}

TEST_F(ExecutorCreationRegistryTest, ConcurrentRecoverCannotInventExitBeforeConstructorBarrier) {
  const auto request = identity("actual-construction-barrier");
  auto& registry = HandleRegistry::instance();
  const auto handle = registry.reserveCreation(request);
  std::promise<void> entered;
  std::promise<void> exit;
  auto exit_signal = exit.get_future();
  auto entered_signal = entered.get_future();
  auto constructor = std::async(std::launch::async, [&] {
    try {
      executor::TaskExecutorConfig config;
      config.function_registry =
          NativeEngineLifecycle::instance().functionCatalogStartup()->requirePublished()->registry();
      config.task_id = request.admission_seal().attempt().task_id();
      auto task = std::make_shared<executor::TaskExecutor>(config);
      registry.attachCreation(handle, task);
    } catch (...) {
      entered.set_exception(std::current_exception());
      registry.finishCreation(handle);
      throw;
    }
    entered.set_value();
    exit_signal.get();
    registry.finishCreation(handle);
  });
  entered_signal.get();
  EXPECT_EQ(registry.recoverCreation(request).state(), proto::EXECUTOR_CREATION_STATE_CONSTRUCTING);
  EXPECT_THROW(registry.destroyAll(), std::runtime_error);
  exit.set_value();
  constructor.get();
  EXPECT_EQ(registry.recoverCreation(request).executor_handle(), static_cast<uint64_t>(handle));
  registry.release(handle);
}
}  // namespace
}  // namespace pangrova::jni
