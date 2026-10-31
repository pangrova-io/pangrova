#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bridge/command/NativeCommandDispatcher.h"
#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::test {

// Real scoped transport actor. PREPARE uses the production command dispatcher,
// not a second SDK implementation. This is not a registered production Worker.
class LanceSdkPreparationActor final {
 public:
  LanceSdkPreparationActor() {
    const char* nonce = std::getenv("PANGROVA_LANCE_FIXTURE_NONCE");
    if (!nonce || std::strlen(nonce) < 32 || std::strlen(nonce) > 128) {
      throw std::runtime_error("Actual PG/SDK actor client nonce is required");
    }
    nonce_ = nonce;
    std::array<char, 32> pattern{};
    std::strcpy(pattern.data(), "/tmp/lance-native-XXXXXX");
    const auto* created = ::mkdtemp(pattern.data());
    if (!created) throw std::runtime_error("Cannot create owned Native SDK actor directory");
    directory_ = created;
    endpoint_ = directory_ + "/actor.sock";
    server_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    try {
      if (server_ < 0) throw std::runtime_error("Cannot create Native SDK actor socket");
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      if (endpoint_.size() >= sizeof(address.sun_path)) throw std::runtime_error("SDK actor socket path exceeds its bound");
      std::strcpy(address.sun_path, endpoint_.c_str());
      if (::bind(server_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0
          || ::chmod(endpoint_.c_str(), S_IRUSR | S_IWUSR) != 0 || ::listen(server_, 1) != 0) {
        throw std::runtime_error("Cannot bind actual Native SDK actor endpoint");
      }
      thread_ = std::thread([this] { serve(); });
    } catch (...) {
      if (server_ >= 0) ::close(server_);
      std::filesystem::remove_all(directory_);
      throw;
    }
  }

  ~LanceSdkPreparationActor() {
    stopping_.store(true);
    // A real connect wakes accept even on platforms where shutdown of a
    // listening socket reports ENOTCONN.
    const int wake = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (wake >= 0) {
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      std::strcpy(address.sun_path, endpoint_.c_str());
      ::connect(wake, reinterpret_cast<sockaddr*>(&address), sizeof(address));
      ::close(wake);
    }
    ::shutdown(server_, SHUT_RDWR);
    {
      std::lock_guard<std::mutex> lock(activeMutex_);
      if (active_ >= 0) ::shutdown(active_, SHUT_RDWR);
    }
    // Join the actual handler before releasing its endpoint. Exceptions/RPC
    // failures do not generate SDK_JOINED or any metadata drain receipt.
    if (thread_.joinable()) thread_.join();
    ::close(server_);
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  LanceSdkPreparationActor(const LanceSdkPreparationActor&) = delete;
  LanceSdkPreparationActor& operator=(const LanceSdkPreparationActor&) = delete;

  const std::string& endpoint() const { return endpoint_; }

  void expect(const protocol::v2::PrepareDistributedWriteRequest& request) {
    auto bare = request;
    bare.clear_invocation_id();
    std::lock_guard<std::mutex> lock(requestMutex_);
    expected_ = bare.SerializeAsString();
    probed_ = false;
  }

  bool matches(const protocol::v2::PrepareDistributedWriteRequest& request) {
    auto bare = request;
    bare.clear_invocation_id();
    std::lock_guard<std::mutex> lock(requestMutex_);
    return probed_ && bare.SerializeAsString() == expected_;
  }

  protocol::v2::RuntimeCommandResponse prepare(const protocol::v2::PrepareDistributedWriteRequest& request) {
    if (request.invocation_id().empty() || !matches(request)) {
      throw std::runtime_error("Native SDK actor requires its exact probed issuance");
    }
    const int client = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (client < 0) throw std::runtime_error("Cannot connect to actual Native SDK actor");
    try {
      configure(client);
      sockaddr_un address{};
      address.sun_family = AF_UNIX;
      std::strcpy(address.sun_path, endpoint_.c_str());
      if (::connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        throw std::runtime_error("Actual Native SDK actor is not listening");
      }
      writeWord(client, 2);
      writeFrame(client, nonce_);
      writeFrame(client, request.SerializeAsString());
      const auto success = readWord(client);
      const auto bytes = readFrame(client, kMaxFrame);
      if (success != 1) throw std::runtime_error("Actual Native SDK actor rejected request: " + bytes);
      protocol::v2::RuntimeCommandResponse response;
      if (!response.ParseFromString(bytes)) throw std::runtime_error("Malformed actual dispatcher result");
      ::close(client);
      return response;
    } catch (...) {
      ::close(client);
      throw;
    }
  }

 private:
  static constexpr uint32_t kMaxFrame = 32 * 1024 * 1024;

  static void configure(int fd) {
    timeval timeout{60, 0};
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0
        || ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
      throw std::runtime_error("Cannot bound actual SDK actor transport waits");
    }
#ifdef SO_NOSIGPIPE
    int noSignal = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, sizeof(noSignal)) != 0) {
      throw std::runtime_error("Cannot protect SDK actor transport");
    }
#endif
  }

  static void transfer(int fd, void* bytes, size_t size, bool writing) {
    auto* cursor = static_cast<char*>(bytes);
    while (size != 0) {
#ifdef MSG_NOSIGNAL
      const auto count = writing ? ::send(fd, cursor, size, MSG_NOSIGNAL) : ::recv(fd, cursor, size, 0);
#else
      const auto count = writing ? ::send(fd, cursor, size, 0) : ::recv(fd, cursor, size, 0);
#endif
      if (count < 0 && errno == EINTR) continue;
      if (count <= 0) throw std::runtime_error("Actual SDK actor transport failed or timed out");
      cursor += count;
      size -= count;
    }
  }

  static uint32_t readWord(int fd) {
    uint32_t value;
    transfer(fd, &value, sizeof(value), false);
    return ntohl(value);
  }

  static void writeWord(int fd, uint32_t value) {
    value = htonl(value);
    transfer(fd, &value, sizeof(value), true);
  }

  static std::string readFrame(int fd, uint32_t maximum) {
    const auto length = readWord(fd);
    if (length == 0 || length > maximum) throw std::runtime_error("Actual SDK actor frame exceeds its bound");
    std::string bytes(length, '\0');
    transfer(fd, bytes.data(), bytes.size(), false);
    return bytes;
  }

  static void writeFrame(int fd, const std::string& bytes) {
    if (bytes.empty() || bytes.size() > kMaxFrame) throw std::runtime_error("Actual SDK actor frame exceeds its bound");
    writeWord(fd, static_cast<uint32_t>(bytes.size()));
    transfer(fd, const_cast<char*>(bytes.data()), bytes.size(), true);
  }

  void serve() noexcept {
    while (!stopping_.load()) {
      const int client = ::accept(server_, nullptr, nullptr);
      if (client < 0) {
        if (errno == EINTR) continue;
        return;
      }
      if (stopping_.load()) {
        ::close(client);
        return;
      }
      {
        std::lock_guard<std::mutex> lock(activeMutex_);
        active_ = client;
      }
      try {
        configure(client);
        handle(client);
      } catch (const std::exception& failure) {
        try {
          writeWord(client, 0);
          writeFrame(client, failure.what());
        } catch (...) {
          // A lost transport reply is not physical SDK drain evidence.
        }
      }
      {
        std::lock_guard<std::mutex> lock(activeMutex_);
        active_ = -1;
        ::close(client);
      }
    }
  }

  void handle(int client) {
    const auto operation = readWord(client);
    const auto nonce = readFrame(client, 128);
    if (nonce.size() != nonce_.size() || CRYPTO_memcmp(nonce.data(), nonce_.data(), nonce_.size()) != 0) {
      throw std::runtime_error("Unknown actual SDK actor client");
    }
    const auto bytes = readFrame(client, kMaxFrame);
    protocol::v2::PrepareDistributedWriteRequest request;
    if (!request.ParseFromString(bytes)) throw std::runtime_error("Malformed actual SDK actor request");
    std::string result;
    if (operation == 1 && request.invocation_id().empty()) {
      std::lock_guard<std::mutex> lock(requestMutex_);
      if (bytes != expected_) throw std::runtime_error("SDK actor probe differs from its exact request");
      std::array<unsigned char, 32> digest{};
      unsigned int size = 0;
      if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &size, EVP_sha256(), nullptr) != 1
          || size != digest.size()) throw std::runtime_error("Cannot hash actual SDK actor probe");
      result.assign(reinterpret_cast<const char*>(digest.data()), digest.size());
      probed_ = true;
    } else if (operation == 2 && !request.invocation_id().empty() && matches(request)) {
      protocol::v2::RuntimeCommand command;
      command.mutable_protocol()->set_major(2);
      command.set_command_type(protocol::v2::RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE);
      command.mutable_payload()->PackFrom(request);
      auto* storage = command.add_worker_local_storage_bindings();
      *storage->mutable_catalog() = request.output_binding().lance().catalog();
      storage->set_tenant_id(request.security().tenant_id());
      storage->set_warehouse_uri(std::filesystem::path(request.output_binding().lance().dataset_uri()).parent_path().string());
      const auto serialized = command.SerializeAsString();
      const auto response = pangrova::jni::dispatchCommand({serialized.begin(), serialized.end()});
      result.assign(reinterpret_cast<const char*>(response.data()), response.size());
    } else {
      throw std::runtime_error("Actual SDK actor requires its exact probed durable issuance");
    }
    writeWord(client, 1);
    writeFrame(client, result);
  }

  std::string nonce_;
  std::string directory_;
  std::string endpoint_;
  int server_{-1};
  int active_{-1};
  std::atomic<bool> stopping_{false};
  std::thread thread_;
  std::mutex activeMutex_;
  std::mutex requestMutex_;
  std::string expected_;
  bool probed_{false};
};

}  // namespace pangrova::test
