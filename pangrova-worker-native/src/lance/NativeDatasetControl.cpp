#include "lance/NativeDatasetControl.h"

#include <stdexcept>
#include <string>

#include <arrow/c/abi.h>

#include "capabilities/capability/CanonicalPhysicalTypes.h"
#include "engine/resource/WorkerStorageBinding.h"
#include "lance/LanceStorageContext.h"
#include "lance/LanceDatasetPublication.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "engine/velox/memory/VeloxMemoryGovernor.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace pangrova::jni {
namespace {
namespace proto = protocol::v2;
constexpr size_t kMaxMetadataBytes = 16 * 1024 * 1024;

struct SchemaGuard final {
  ArrowSchema schema{};
  ~SchemaGuard() {
    if (schema.release) schema.release(&schema);
  }
};

proto::RuntimeCommandResponse failure(const std::exception& error, bool invalid) {
  proto::RuntimeCommandResponse response;
  auto* detail = response.mutable_error();
  detail->set_error_class(invalid ? proto::ERROR_CLASS_INVALID_REQUEST : proto::ERROR_CLASS_INTERNAL);
  detail->set_code(invalid ? "INVALID_DATASET_CONTROL_REQUEST" : "DATASET_CONTROL_FAILED");
  detail->set_message(error.what());
  detail->set_failed_component("native-dataset-control");
  detail->set_retryable(false);
  return response;
}

template <typename Request>
Request unpack(const proto::RuntimeCommand& command) {
  Request request;
  if (!command.has_payload() || command.payload().value().size() > kMaxMetadataBytes ||
      !command.payload().UnpackTo(&request)) {
    throw std::invalid_argument("Dataset control requires the exact bounded typed command payload");
  }
  return request;
}

template <typename Result>
proto::RuntimeCommandResponse success(const Result& result) {
  proto::RuntimeCommandResponse response;
  response.set_success(true);
  response.mutable_payload()->PackFrom(result);
  if (response.ByteSizeLong() > kMaxMetadataBytes) throw std::invalid_argument("Dataset control response exceeds its canonical envelope bound");
  return response;
}
}  // namespace

proto::RuntimeCommandResponse NativeDatasetControl::schemaFingerprint(
    const proto::RuntimeCommand& command, velox::VeloxMemoryGovernor* governor) {
  try {
    const auto request = unpack<proto::DatasetSchemaFingerprintRequest>(command);
    lance::LanceDatasetPublication::validateSchemaRequest(request);
    lance::LanceStorageContext storage(pangrova::resource::workerLocalStorage(command.worker_local_storage_bindings(),
        request.catalog(), request.security().tenant_id(), ""));
    if (!governor) throw std::runtime_error("Dataset schema fingerprint requires an initialized Native runtime");
    auto pool = governor->veloxMemoryManager()->addLeafPool("dataset-schema-fingerprint");
    const auto type = capability::canonicalPhysicalSchema(request.schema());
    const auto empty = facebook::velox::BaseVector::create(type, 0, pool.get());
    SchemaGuard schema;
    facebook::velox::exportToArrow(empty, schema.schema);
    const auto fingerprint = lance::LanceDatasetPublication::schemaFingerprint(schema.schema);
    proto::DatasetSchemaFingerprintResult result;
    result.mutable_physical_schema_sha256()->set_algorithm(proto::Checksum::ALGORITHM_SHA256);
    result.mutable_physical_schema_sha256()->set_digest(fingerprint.data(), fingerprint.size());
    return success(result);
  } catch (const std::invalid_argument& error) {
    return failure(error, true);
  } catch (const std::exception& error) {
    return failure(error, false);
  }
}

proto::RuntimeCommandResponse NativeDatasetControl::prepareDistributedWrite(const proto::RuntimeCommand& command) {
  try {
    const auto request = unpack<proto::PrepareDistributedWriteRequest>(command);
    const auto storage = pangrova::resource::workerLocalStorage(command.worker_local_storage_bindings(),
        request.output_binding().lance().catalog(), request.security().tenant_id(),
        request.output_binding().lance().dataset_uri());
    return success(lance::LanceDatasetPublication::prepareWrite(request, storage));
  } catch (const std::invalid_argument& error) {
    return failure(error, true);
  } catch (const std::exception& error) {
    return failure(error, false);
  }
}

proto::RuntimeCommandResponse NativeDatasetControl::cleanupObjects(const proto::RuntimeCommand& command) {
  try {
    const auto request = unpack<proto::CleanupSourceObjectsRequest>(command);
    const auto storage = pangrova::resource::workerLocalStorage(command.worker_local_storage_bindings(), request.catalog(),
        request.security().tenant_id(), request.dataset_uri());
    return success(lance::LanceDatasetPublication::cleanupObjects(request, storage));
  } catch (const std::invalid_argument& error) { return failure(error, true); }
    catch (const std::exception& error) { return failure(error, false); }
}

proto::RuntimeCommandResponse NativeDatasetControl::scanPartitions(const proto::RuntimeCommand& command) {
  try {
    const auto request = unpack<proto::DatasetScanPartitionsRequest>(command);
    const auto storage = pangrova::resource::workerLocalStorage(command.worker_local_storage_bindings(), request.catalog(),
        request.security().tenant_id(), request.dataset_uri());
    return success(lance::LanceDatasetPublication::scanPartitions(request, storage));
  } catch (const std::invalid_argument& error) {
    return failure(error, true);
  } catch (const std::exception& error) {
    return failure(error, false);
  }
}

}  // namespace pangrova::jni
