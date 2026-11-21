#pragma once

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <folly/json.h>
#include <openssl/evp.h>

#include "LanceSdkPreparationActor.h"
#include "lance/LanceBatchWriter.h"
#include "lance/LanceStorageContext.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::test {

// Fixture transport only. Every source reference is issued by the production
// PostgreSQL catalog, never constructed from SDK candidate bytes in this client.
class LanceManagedSourceTestAuthority final {
 public:
  static protocol::v2::CatalogExecutionBinding catalog() {
    const auto result = rpc(folly::dynamic::object("operation", "catalog"));
    protocol::v2::CatalogExecutionBinding value;
    if (!value.ParseFromString(decode(result["catalog"].asString()))) fail("Malformed PG catalog binding");
    return value;
  }

  static std::filesystem::path warehouse() {
    const auto result = rpc(folly::dynamic::object("operation", "warehouse"));
    const std::filesystem::path path(result["warehouse"].asString());
    if (!path.is_absolute() || path.lexically_normal() != path) {
      fail("Actual PG fixture warehouse is not an absolute canonical local path");
    }
    // Do not resolve symlinks: authority identity uses the exact catalog URI.
    return path;
  }

  static void allocate(lance::LanceWriterConfig& config,
      const std::string& arrowSchemaIpc, int32_t partitionId) {
    const auto result = rpc(folly::dynamic::object("operation", "allocate")
        ("uri", config.outputPath)("writeId", config.writeId)
        ("writeOperation", static_cast<int32_t>(config.operation))
        ("version", config.expectedReadVersion)("partition", partitionId)
        ("schemaSha256", encode(std::string(reinterpret_cast<const char*>(
            config.schemaFenceSha256.data()), config.schemaFenceSha256.size())))
        ("schemaIpc", encode(arrowSchemaIpc)));
    const auto attempt = decode(result["attempt"].asString());
    config.taskAttempt.assign(attempt.begin(), attempt.end());
    config.transactionStagingPrefix = result["prefix"].asString();
    config.acceptedBaseSnapshot = result.count("base") ? decode(result["base"].asString()) : "";
  }

  static protocol::v2::AcceptedSourceSnapshotRef reference(const std::string& dataset, uint64_t version) {
    const auto result = rpc(folly::dynamic::object("operation", "reference")("uri", dataset)("version", version));
    return parseReference(decode(result["reference"].asString()));
  }

  static protocol::v2::WriteOutputBinding allocationBinding(const lance::LanceWriterConfig& config) {
    const auto value = rpc(folly::dynamic::object("operation", "allocation")("execution", execution(config)));
    protocol::v2::WriteOutputBinding binding;
    if (!binding.ParseFromString(decode(value["binding"].asString()))) fail("Malformed actual PG output binding");
    return binding;
  }

  static protocol::v2::PrepareDistributedWriteRequest reserve(
      const std::string& executionId, const std::vector<std::vector<uint8_t>>& fragments) {
    folly::dynamic winners = folly::dynamic::array;
    for (const auto& bytes : fragments) {
      winners.push_back(encode(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size())));
    }
    const auto value = rpc(folly::dynamic::object("operation", "reserve")("execution", executionId)("fragments", winners));
    if (value.count("accepted")) fail("Actual source was already accepted; use PG reference reconciliation");
    protocol::v2::PrepareDistributedWriteRequest request;
    if (!request.ParseFromString(decode(value["request"].asString()))) fail("Malformed actual PG preparation request");
    return request;
  }

  static protocol::v2::PrepareDistributedWriteRequest issue(protocol::v2::PrepareDistributedWriteRequest request,
      LanceSdkPreparationActor& actor) {
    request.clear_invocation_id();
    actor.expect(request);
    const auto response = rpc(folly::dynamic::object("operation", "issue")
        ("actor_socket", actor.endpoint())("request", encode(request.SerializeAsString())));
    if (!request.ParseFromString(decode(response["request"].asString()))) fail("Malformed actual PG SDK issuance");
    return request;
  }

  static protocol::v2::ExecutionOwnerIdentity handoff(const std::string& executionId) {
    const auto value = rpc(folly::dynamic::object("operation", "handoff")("execution", executionId));
    protocol::v2::ExecutionOwnerIdentity owner;
    if (!owner.ParseFromString(decode(value["owner"].asString()))) fail("Malformed actual PG recovery owner");
    return owner;
  }

  static std::optional<protocol::v2::AcceptedSourceSnapshotRef> registerPrepared(
      const std::string& executionId, const protocol::v2::PrepareDistributedWriteResult& prepared) {
    const auto value = rpc(folly::dynamic::object("operation", "register")("execution", executionId)
        ("result", encode(prepared.SerializeAsString())));
    if (!value.count("accepted")) return std::nullopt;
    return parseReference(decode(value["accepted"].asString()));
  }

  static uint64_t commit(const lance::LanceWriterConfig& config,
      const std::vector<std::vector<uint8_t>>& fragments, uint64_t knownVersion,
      const std::vector<std::string>& prefixes) {
    const auto identity = execution(config);
    validateConfig(config, identity);
    if (!prefixes.empty() && prefixes.size() != fragments.size()) fail("One exact prefix is required per winner");
    folly::dynamic winners = folly::dynamic::array;
    for (size_t i = 0; i < fragments.size(); ++i) {
      protocol::v2::LanceWriteFragment fragment;
      const auto& bytes = fragments[i];
      if (!fragment.ParseFromArray(bytes.data(), bytes.size())) fail("Malformed actual winner receipt");
      const auto& prefix = prefixes.empty() ? config.transactionStagingPrefix : prefixes[i];
      if (fragment.transaction().relative_object_key().rfind(prefix + "/transaction-", 0) != 0) {
        fail("Actual winner does not belong to its supplied attempt staging prefix");
      }
      winners.push_back(encode(std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size())));
    }
    auto response = rpc(folly::dynamic::object("operation", "reserve")("execution", identity)("fragments", winners));
    if (response.count("accepted")) {
      const auto accepted = parseReference(decode(response["accepted"].asString()));
      if (knownVersion != 0 && accepted.version() != knownVersion) fail("Recovery hint differs from actual PG publication");
      verifyManifest(config, accepted);
      return accepted.version();
    }
    protocol::v2::PrepareDistributedWriteRequest request;
    if (!request.ParseFromString(decode(response["request"].asString()))) fail("Malformed actual PG preparation reservation");
    for (uint32_t index = 0; index < 4096; ++index) {
      request.set_deadline_epoch_ms(deadline());
      LanceSdkPreparationActor actor;
      request = issue(request, actor);
      const auto actual = actor.prepare(request);
      protocol::v2::PrepareDistributedWriteResult prepared;
      if (!actual.success() || !actual.payload().UnpackTo(&prepared)
          || prepared.outcome() != protocol::v2::WRITE_PREPARATION_OUTCOME_PREPARED
          || !prepared.has_object_page()) {
        fail("Actual dispatcher SDK preparation remains UNKNOWN: " + actual.error().message());
      }
      response = rpc(folly::dynamic::object("operation", "register")("execution", identity)
          ("result", encode(prepared.SerializeAsString())));
      if (response.count("accepted")) {
        const auto accepted = parseReference(decode(response["accepted"].asString()));
        if (!prepared.object_page().is_last() || accepted.SerializeAsString() != prepared.publication().candidate().SerializeAsString()) {
          fail("PG acceptance differs from the complete actual SDK candidate");
        }
        verifyManifest(config, accepted);
        return accepted.version();
      }
      if (prepared.object_page().is_last() || prepared.object_page().objects_size() == 0) fail("PG did not accept sealed candidate");
      request.set_object_page_index(index + 1);
      request.set_after_object_path(prepared.object_page().objects(prepared.object_page().objects_size() - 1).relative_path());
    }
    fail("Managed fixture exceeded the production object page bound");
  }

  static void cleanup(const lance::LanceWriterConfig& config,
      const std::vector<std::vector<uint8_t>>& fragments, const std::vector<std::string>& prefixes) {
    const auto identity = execution(config); validateConfig(config, identity);
    if (fragments.empty() || (!prefixes.empty() && prefixes.size() != fragments.size())) fail("Exact cleanup winners are required");
    folly::dynamic winners = folly::dynamic::array;
    for (size_t index = 0; index < fragments.size(); ++index) {
      protocol::v2::LanceWriteFragment fragment;
      if (!fragment.ParseFromArray(fragments[index].data(), fragments[index].size())) fail("Malformed cleanup winner");
      const auto& prefix = prefixes.empty() ? config.transactionStagingPrefix : prefixes[index];
      if (fragment.transaction().relative_object_key().rfind(prefix + "/transaction-", 0) != 0) fail("Cleanup winner staging prefix differs");
      winners.push_back(encode(std::string(reinterpret_cast<const char*>(fragments[index].data()), fragments[index].size())));
    }
    lance::LanceStorageContext storage(config.storageBinding);
    for (uint32_t page = 0; page < 4096; ++page) {
      const auto selected = rpc(folly::dynamic::object("operation", "cleanup")("execution", identity)("fragments", winners));
      if (!selected.count("request")) return;
      protocol::v2::CleanupSourceObjectsRequest request;
      if (!request.ParseFromString(decode(selected["request"].asString()))) fail("Malformed actual PG cleanup reservation");
      const auto bytes = request.SerializeAsString(); LanceCBuffer output{};
      if (lance_ffi_cleanup_objects(storage.get(), reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), &output) != 0) {
        fail(std::string("Actual SDK cleanup remains held: ") + (lance_ffi_last_error() ? lance_ffi_last_error() : "missing evidence"));
      }
      protocol::v2::CleanupSourceObjectsResult result;
      const bool parsed = result.ParseFromArray(output.data, output.len); lance_ffi_buffer_free(output);
      if (!parsed || !result.sdk_joined() || result.cleanup_id() != request.cleanup_id()
          || result.deleted_count() != static_cast<uint32_t>(request.objects_size())
          || result.objects_sha256().SerializeAsString() != request.objects_sha256().SerializeAsString()) fail("SDK deletion differs from the PG batch");
      rpc(folly::dynamic::object("operation", "deleted")("execution", identity)("result", encode(result.SerializeAsString())));
    }
    fail("Cleanup exceeded the production page bound");
  }

  static folly::dynamic rpc(folly::dynamic request) {
    const char* endpoint = std::getenv("PANGROVA_LANCE_FIXTURE_SOCKET");
    const char* nonce = std::getenv("PANGROVA_LANCE_FIXTURE_NONCE");
    if (!endpoint || !nonce) fail("Real PG Lance fixture broker is required before Native SDK tests");
    request["nonce"] = nonce;
    const auto bytes = folly::toJson(request);
    if (bytes.empty() || bytes.size() > kMaxFrame) fail("PG fixture request exceeds its bound");
    Connection connection(endpoint);
    uint32_t length = htonl(static_cast<uint32_t>(bytes.size()));
    connection.write(&length, sizeof(length));
    connection.write(bytes.data(), bytes.size());
    connection.read(&length, sizeof(length));
    length = ntohl(length);
    if (length == 0 || length > kMaxFrame) fail("PG fixture response exceeds its bound");
    std::string result(length, '\0');
    connection.read(result.data(), result.size());
    auto value = folly::parseJson(result);
    if (!value["success"].asBool()) fail(std::string("Actual PG authority rejected fixture operation: ") + value["error"].asString());
    return value;
  }

 private:
  static constexpr uint32_t kMaxFrame = 32 * 1024 * 1024;
  class Connection final {
   public:
    explicit Connection(const char* endpoint) {
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      if (std::strlen(endpoint) >= sizeof(address.sun_path)) fail("PG fixture socket path exceeds its bound");
      std::strcpy(address.sun_path, endpoint);
      fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
      if (fd_ < 0) fail("Could not open PG fixture socket");
      timeval timeout{60, 0};
      if (::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0
          || ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
        closeAndFail("Could not bound PG fixture transport waits");
      }
#ifdef SO_NOSIGPIPE
      int noSignal = 1;
      if (::setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, sizeof(noSignal)) != 0) closeAndFail("Could not protect fixture transport");
#endif
      if (::connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) closeAndFail("Could not connect to actual PG fixture");
    }
    ~Connection() { if (fd_ >= 0) ::close(fd_); }
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    void write(const void* data, size_t size) {
      auto* position = static_cast<const uint8_t*>(data);
      while (size != 0) {
#ifdef MSG_NOSIGNAL
        auto count = ::send(fd_, position, size, MSG_NOSIGNAL);
#else
        auto count = ::write(fd_, position, size);
#endif
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) fail("PG fixture write failed or timed out");
        position += count; size -= count;
      }
    }
    void read(void* data, size_t size) {
      auto* position = static_cast<uint8_t*>(data);
      while (size != 0) {
        auto count = ::read(fd_, position, size);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) fail("PG fixture read failed or timed out");
        position += count; size -= count;
      }
    }
   private:
    [[noreturn]] void closeAndFail(const char* message) { ::close(fd_); fd_ = -1; fail(message); }
    int fd_{-1};
  };

  [[noreturn]] static void fail(const std::string& message) { throw std::runtime_error(message); }
  static uint64_t deadline() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() + 30'000;
  }
  static std::string encode(const std::string& bytes) {
    if (bytes.size() > kMaxFrame) fail("Fixture byte metadata exceeds its bound");
    const auto size = 4 * ((bytes.size() + 2) / 3);
    std::string result(size + 1, '\0');
    const int count = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(result.data()),
        reinterpret_cast<const unsigned char*>(bytes.data()), static_cast<int>(bytes.size()));
    if (count != static_cast<int>(size)) fail("Could not encode fixture metadata");
    result.resize(size);
    return result;
  }
  static std::string decode(const std::string& bytes) {
    if (bytes.empty() || bytes.size() % 4 != 0 || bytes.size() > kMaxFrame) fail("Malformed bounded fixture metadata");
    std::string result(bytes.size() / 4 * 3, '\0');
    const int count = EVP_DecodeBlock(reinterpret_cast<unsigned char*>(result.data()),
        reinterpret_cast<const unsigned char*>(bytes.data()), static_cast<int>(bytes.size()));
    if (count < 0) fail("Could not decode fixture metadata");
    const auto padding = (bytes.back() == '=') + (bytes.size() > 1 && bytes[bytes.size() - 2] == '=');
    result.resize(count - padding);
    return result;
  }
  static protocol::v2::AcceptedSourceSnapshotRef parseReference(const std::string& bytes) {
    protocol::v2::AcceptedSourceSnapshotRef result;
    if (!result.ParseFromString(bytes) || result.version() == 0 || result.membership_id().empty()) fail("Malformed actual PG accepted reference");
    return result;
  }
  static std::string execution(const lance::LanceWriterConfig& config) {
    protocol::v2::TaskAttemptId attempt;
    if (!attempt.ParseFromArray(config.taskAttempt.data(), config.taskAttempt.size())) fail("Malformed actual issued attempt");
    return attempt.query_or_job_id();
  }
  static void validateConfig(const lance::LanceWriterConfig& config, const std::string& executionId) {
    const auto result = rpc(folly::dynamic::object("operation", "allocation")("execution", executionId));
    protocol::v2::WriteOutputBinding output;
    if (!output.ParseFromString(decode(result["binding"].asString())) || !output.has_lance()) fail("Missing actual output allocation");
    const auto& binding = output.lance();
    if (binding.dataset_uri() != config.outputPath || binding.write_id() != config.writeId
        || binding.generation() != config.generation || binding.expected_read_version() != config.expectedReadVersion
        || binding.operation() != static_cast<protocol::v2::LanceWriteOperation>(config.operation)
        || binding.schema_fence_sha256().digest() != std::string(reinterpret_cast<const char*>(
            config.schemaFenceSha256.data()), config.schemaFenceSha256.size())
        || (binding.has_base_snapshot() ? binding.base_snapshot().SerializeAsString() : "") != config.acceptedBaseSnapshot) {
      fail("Test commit differs from its actual PG-issued immutable allocation");
    }
  }
  static void verifyManifest(const lance::LanceWriterConfig& config, const protocol::v2::AcceptedSourceSnapshotRef& accepted) {
    std::ifstream file(std::filesystem::path(config.outputPath) / accepted.manifest_relative_path(), std::ios::binary);
    if (!file) fail("Actual accepted manifest object is absent");
    const std::string bytes((std::istreambuf_iterator<char>(file)), {});
    std::array<uint8_t, 32> actual{};
    unsigned int size = 0;
    if (bytes.size() != accepted.size_bytes() || EVP_Digest(bytes.data(), bytes.size(), actual.data(), &size, EVP_sha256(), nullptr) != 1
        || size != actual.size() || accepted.manifest_sha256().digest() != std::string(reinterpret_cast<const char*>(actual.data()), actual.size())) {
      fail("PG acceptance must retain SHA-256 of actual SDK manifest bytes");
    }
  }
};

}  // namespace pangrova::test
