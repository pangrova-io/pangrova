#include "command/resource/NativeCreationControl.h"

#include <stdexcept>
#include "command/task/TaskRequestParser.h"
#include "command/resource/HandleRegistry.h"

namespace pangrova::jni {
protocol::v2::RuntimeCommandResponse recoverExecutorCreation(
    const protocol::v2::RuntimeCommand& command, int64_t executor_handle) {
  namespace proto = protocol::v2;
  proto::ExecutorCreationRecoveryRequest request;
  if (executor_handle != 0 || !command.has_attempt() || !command.has_payload() ||
      command.worker_local_storage_bindings_size() != 0 ||
      !command.GetReflection()->GetUnknownFields(command).empty() ||
      !command.payload().GetReflection()->GetUnknownFields(command.payload()).empty() ||
      command.payload().type_url() != "type.googleapis.com/pangrova.protocol.v2.ExecutorCreationRecoveryRequest" ||
      !command.payload().UnpackTo(&request)) {
    throw std::invalid_argument("Creation recovery requires a canonical global command");
  }
  validateCreationIdentity(request);
  const auto& seal = request.admission_seal();
  if (command.protocol().SerializeAsString() != seal.protocol().SerializeAsString() ||
      command.attempt().SerializeAsString() != seal.attempt().SerializeAsString()) {
    throw std::invalid_argument("Creation recovery outer identity differs from its exact request");
  }
  auto result = HandleRegistry::instance().recoverCreation(request);
  proto::RuntimeCommandResponse response;
  response.set_success(true);
  response.mutable_payload()->PackFrom(result);
  return response;
}
}
