// Test-only executable using the formal C ABI. No catalog authority or reader bypass lives here.
#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#include "lance_c_ffi.h"
#include "pangrova/protocol/v2/task.pb.h"

namespace {
namespace proto = pangrova::protocol::v2;
constexpr size_t kMaxMetadata = 32 * 1024 * 1024;

[[noreturn]] void ffiError() {
  const char* error = lance_ffi_last_error();
  throw std::runtime_error(error ? error : "Actual Lance C ABI call failed");
}
void checked(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}
template<typename T> T checked(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
std::string metadata(const char* path) {
  const auto size = std::filesystem::file_size(path);
  if (size > kMaxMetadata) throw std::runtime_error("Fixture metadata exceeds its bound");
  std::ifstream input(path, std::ios::binary);
  std::string bytes((std::istreambuf_iterator<char>(input)), {});
  if (!input || bytes.size() != size) throw std::runtime_error("Fixture metadata file changed or could not be read");
  return bytes;
}
void save(const char* path, const void* data, size_t size) {
  if (size > kMaxMetadata) throw std::runtime_error("Fixture result exceeds its bound");
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  output.write(static_cast<const char*>(data), size);
  output.close();
  if (!output) throw std::runtime_error("Could not save actual fixture result");
}
using Storage = std::unique_ptr<LanceCStorage, decltype(&lance_ffi_storage_close)>;
Storage storage(const char* path) {
  const auto bytes = metadata(path);
  Storage result(lance_ffi_storage_open(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()), lance_ffi_storage_close);
  if (!result) ffiError();
  return result;
}
std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batches(const char* path) {
  return checked(arrow::ipc::RecordBatchStreamReader::Open(checked(arrow::io::ReadableFile::Open(path))));
}
void fingerprint(const char* stream, const char* output) {
  const auto input = batches(stream);
  ArrowSchema schema{};
  checked(arrow::ExportSchema(*input->schema(), &schema));
  std::array<uint8_t, 32> digest{};
  const auto result = lance_ffi_schema_fingerprint(&schema, digest.data(), digest.size());
  if (schema.release) schema.release(&schema);
  if (result != 0) ffiError();
  save(output, digest.data(), digest.size());
}

class Writer final {
 public:
  Writer(const LanceCStorage* store, const proto::LanceWriteBinding& binding, const std::string& attempt,
      const std::string& tenant, const char* prefix, const std::shared_ptr<arrow::Schema>& schema) {
    ArrowSchema cSchema{};
    checked(arrow::ExportSchema(*schema, &cSchema));
    const auto base = binding.has_base_snapshot() ? binding.base_snapshot().SerializeAsString() : "";
    const auto& sha = binding.schema_fence_sha256().digest();
    value_ = lance_ffi_writer_open(store, binding.dataset_uri().c_str(), &cSchema,
        binding.operation(), binding.expected_read_version(), reinterpret_cast<const uint8_t*>(base.data()), base.size(),
        reinterpret_cast<const uint8_t*>(sha.data()), sha.size(), binding.write_id().c_str(), tenant.c_str(),
        binding.generation().c_str(), prefix, reinterpret_cast<const uint8_t*>(attempt.data()), attempt.size());
    if (cSchema.release) cSchema.release(&cSchema);
    if (!value_) ffiError();
  }
  ~Writer() {
    if (value_) {
      LanceCWriteDrainEvidence evidence{};
      if (lance_ffi_writer_drain(value_, &evidence) == 0) lance_ffi_writer_destroy(value_);
    }
  }
  void write(const arrow::RecordBatch& batch) {
    ArrowArray array{};
    ArrowSchema schema{};
    checked(arrow::ExportRecordBatch(batch, &array, &schema));
    const auto result = lance_ffi_writer_write_batch(value_, &array, &schema);
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    if (result != 0) ffiError();
  }
  void finish(const char* path) {
    LanceCBuffer output{};
    if (lance_ffi_writer_finish(value_, &output) != 0) ffiError();
    const std::string bytes(reinterpret_cast<const char*>(output.data), output.len);
    lance_ffi_buffer_free(output);
    LanceCWriteDrainEvidence evidence{};
    if (lance_ffi_writer_drain(value_, &evidence) != 0
        || evidence.encoders_and_uploads_joined == 0 || evidence.private_stage_cleaned == 0) ffiError();
    if (lance_ffi_writer_destroy(value_) != 0) ffiError();
    value_ = nullptr;
    save(path, bytes.data(), bytes.size());
  }
 private:
  LanceCWriter* value_{};
};

void write(char** args) {
  auto store = storage(args[2]);
  auto attemptBytes = metadata(args[3]);
  proto::TaskAttemptId attempt;
  proto::WriteOutputBinding output;
  if (!attempt.ParseFromString(attemptBytes) || !attempt.has_execution_owner()
      || !output.ParseFromString(metadata(args[4])) || !output.has_lance()) {
    throw std::runtime_error("Fixture needs its genuine PG allocation and issued owner");
  }
  const auto input = batches(args[6]);
  proto::ObjectStoreExecutionBinding storageBinding;
  if (!storageBinding.ParseFromString(metadata(args[2]))) throw std::runtime_error("Malformed canonical storage binding");
  Writer writer(store.get(), output.lance(), attemptBytes, storageBinding.tenant_id(), args[5], input->schema());
  while (auto batch = checked(input->Next())) writer.write(*batch);
  writer.finish(args[7]);
}
void control(char** args, const std::string& operation) {
  auto store = storage(args[2]);
  const auto bytes = metadata(args[3]);
  LanceCBuffer output{};
  const auto result = operation == "scan"
      ? lance_ffi_scan_partitions(store.get(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &output)
      : operation == "cleanup"
          ? lance_ffi_cleanup_objects(store.get(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &output)
          : lance_ffi_prepare_write(store.get(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &output);
  if (result != 0) ffiError();
  const std::string saved(reinterpret_cast<const char*>(output.data), output.len);
  lance_ffi_buffer_free(output);
  save(args[4], saved.data(), saved.size());
}
void read(char** args) {
  auto store = storage(args[2]);
  const auto detail = metadata(args[5]);
  std::unique_ptr<LanceCReader, decltype(&lance_ffi_reader_close)> reader(lance_ffi_reader_open(store.get(), args[3],
      std::stoull(args[4]), reinterpret_cast<const uint8_t*>(detail.data()), detail.size(), 0), lance_ffi_reader_close);
  if (!reader) ffiError();
  ArrowSchema schema{};
  if (lance_ffi_reader_schema(reader.get(), &schema) != 0) ffiError();
  auto actualSchema = checked(arrow::ImportSchema(&schema));
  auto file = checked(arrow::io::FileOutputStream::Open(args[6]));
  auto stream = checked(arrow::ipc::MakeStreamWriter(file.get(), actualSchema));
  for (;;) {
    ArrowArray array{};
    ArrowSchema batchSchema{};
    const auto result = lance_ffi_reader_next(reader.get(), &array, &batchSchema, 1024);
    if (result < 0) {
      if (array.release) array.release(&array);
      if (batchSchema.release) batchSchema.release(&batchSchema);
      ffiError();
    }
    if (result == 0) break;
    const auto batch = checked(arrow::ImportRecordBatch(&array, &batchSchema));
    checked(stream->WriteRecordBatch(*batch));
  }
  checked(stream->Close());
  checked(file->Close());
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (lance_ffi_abi_version() != PANGROVA_LANCE_C_FFI_ABI_VERSION || lance_ffi_runtime_init() != 0) ffiError();
    const std::string command = argc > 1 ? argv[1] : "";
    if (command == "fingerprint" && argc == 4) fingerprint(argv[2], argv[3]);
    else if (command == "write" && argc == 8) write(argv);
    else if (command == "prepare" && argc == 5) control(argv, command);
    else if (command == "scan" && argc == 5) control(argv, command);
    else if (command == "cleanup" && argc == 5) control(argv, command);
    else if (command == "read" && argc == 7) read(argv);
    else throw std::runtime_error("Unknown fixture command or incomplete argument list");
    lance_ffi_runtime_shutdown();
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "Actual Lance fixture failed: %s\n", error.what());
    lance_ffi_runtime_shutdown();
    return 1;
  }
}
