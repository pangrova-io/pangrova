#include "jni/command/NativeResourceDrainControl.h"

#include <google/protobuf/util/message_differencer.h>
#include <stdexcept>
#include "jni/registry/HandleRegistry.h"
#include "velox/runner/VeloxTaskRunner.h"

namespace pangrova::jni {
protocol::v2::RuntimeCommandResponse resourceDrainEvidence(
    const protocol::v2::RuntimeCommand& command, int64_t executorHandle) {
  namespace proto = protocol::v2;
  proto::ResourceDrainEvidenceRequest request;
  if (!command.has_attempt() || !command.has_payload() ||
      !command.payload().UnpackTo(&request) ||
      request.resource() != proto::RESOURCE_DRAIN_SCOPE_RESULT_STREAM ||
      !request.has_security() ||
      command.GetReflection()->GetUnknownFields(command).field_count() != 0 ||
      request.GetReflection()->GetUnknownFields(request).field_count() != 0) {
    throw std::invalid_argument("Resource drain requires an exact canonical attempt, security and resource scope");
  }
  auto executor = HandleRegistry::instance().acquire(executorHandle);
  if (!executor->isResultTask()) throw std::invalid_argument("Executor has no admitted result resource scope");
  proto::ExecutionCallbackContext admitted;
  if (!admitted.ParseFromString(executor->callbackContext()) ||
      !admitted.has_security() || !admitted.has_attempt() ||
      !google::protobuf::util::MessageDifferencer::Equals(command.attempt(), admitted.attempt()) ||
      !google::protobuf::util::MessageDifferencer::Equals(request.security(), admitted.security())) {
    throw std::invalid_argument("Resource drain authority differs from the admitted executor");
  }
  auto* runner = executor->veloxTaskRunner();
  const auto state = executor->state();
  const bool terminal = state == pangrova::executor::ExecutorState::FINISHED ||
      state == pangrova::executor::ExecutorState::FAILED || state == pangrova::executor::ExecutorState::CANCELLED;
  auto ownership = runner && runner->hasResultOutput() ? runner->resultDrainEvidence() :
      result::ResultOwnershipState::Evidence{terminal, terminal, terminal, 0, 0};
  proto::ResourceDrainEvidence evidence;
  *evidence.mutable_attempt() = admitted.attempt();
  *evidence.mutable_security() = admitted.security();
  evidence.set_resource(request.resource());
  evidence.set_producer_stopped(ownership.producer_stopped);
  evidence.set_authority_revoked(ownership.authority_revoked);
  evidence.set_queue_cleared(ownership.queue_cleared);
  evidence.set_active_readers(ownership.active_readers);
  evidence.set_retained_batch_owners(ownership.retained_batch_owners);
  evidence.set_drained(ownership.drained());
  proto::RuntimeCommandResponse response;
  response.set_success(true);
  response.mutable_payload()->PackFrom(evidence);
  return response;
}
}  // namespace pangrova::jni
