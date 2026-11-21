#include "command/task/WorkerLocalCopyInput.h"

#include <algorithm>
#include <filesystem>
#include <stdexcept>

namespace pangrova::jni {

void validateWorkerCopyInput(const protocol::v2::TaskRequest& request) {
  const bool copySplit = std::any_of(request.splits().begin(), request.splits().end(),
      [](const auto& split) { return split.connector_id() == "copy"; });
  if (copySplit && !request.has_copy_source_staging_command_sha256()) {
    throw std::runtime_error("COPY source split requires its private staging command");
  }
  if (request.has_copy_source_staging_command_sha256() != request.has_worker_local_copy_input()) {
    throw std::runtime_error("COPY source task requires verified Worker-local split binding");
  }
  if (!request.has_worker_local_copy_input()) return;
  const auto& input = request.worker_local_copy_input();
  const auto& digest = request.copy_source_staging_command_sha256();
  const bool relational = request.task_kind() == protocol::v2::TASK_KIND_READ ||
      request.task_kind() == protocol::v2::TASK_KIND_WRITE;
  if (!relational || !copySplit || !input.GetReflection()->GetUnknownFields(input).empty() ||
      request.splits_size() != 1 ||
      digest.algorithm() != protocol::v2::Checksum::ALGORITHM_SHA256 || digest.digest().size() != 32) {
    throw std::runtime_error("COPY Worker-local input or command digest is incomplete");
  }
  const auto& split = request.splits(0);
  const std::filesystem::path path(input.local_file_path());
  if (input.split_id().empty() || input.split_id() != split.split_id() ||
      input.source_version_id().empty() || input.source_version_id() != split.source_snapshot() ||
      input.content_size() == 0 || input.content_size() != split.length() || split.start_offset() != 0 ||
      input.content_sha256().size() != 32 || input.pre_native_read_bytes() != input.content_size() ||
      input.pre_native_read_bytes() > request.resource_grant().object_store_read_bytes() ||
      input.local_file_path().find('\0') != std::string::npos || !path.is_absolute() ||
      path.lexically_normal().string() != input.local_file_path() || path.filename() != "source.parquet" ||
      !path.parent_path().filename().string().starts_with("copy-scan-")) {
    throw std::runtime_error("COPY Worker-local input differs from its frozen source or task allowance");
  }
}

void applyWorkerCopyInput(const protocol::v2::TaskRequest& request, executor::TaskExecutorConfig& config) {
  if (!request.has_worker_local_copy_input()) return;
  validateWorkerCopyInput(request);
  if (config.splits.size() != 1 || config.splits.front().file_format != split::FileFormatType::PARQUET ||
      config.splits.front().system_table || config.splits.front().paimon || request.splits(0).has_catalog()) {
    throw std::runtime_error("COPY Native input requires one complete external Parquet file");
  }
  const auto& input = request.worker_local_copy_input();
  config.splits.front().file_path = input.local_file_path();
  config.splits.front().expected_file_sha256 = input.content_sha256();
  config.resource_limit.max_object_store_read_bytes -= static_cast<int64_t>(input.pre_native_read_bytes());
}

}  // namespace pangrova::jni
