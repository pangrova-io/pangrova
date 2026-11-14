#pragma once

namespace pangrova::protocol::v2 {
class RuntimeCommand;
class RuntimeCommandResponse;
}

namespace pangrova::jni {

/** Thin metadata-only command boundary; no execution or storage types escape. */
class NativeDatasetControl final {
 public:
  static protocol::v2::RuntimeCommandResponse schemaFingerprint(
      const protocol::v2::RuntimeCommand& command);
  static protocol::v2::RuntimeCommandResponse prepareDistributedWrite(
      const protocol::v2::RuntimeCommand& command);
  static protocol::v2::RuntimeCommandResponse cleanupObjects(const protocol::v2::RuntimeCommand& command);
  static protocol::v2::RuntimeCommandResponse scanPartitions(const protocol::v2::RuntimeCommand& command);
};

}  // namespace pangrova::jni
