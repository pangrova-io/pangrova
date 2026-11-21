#include "command/task/TaskRequestParser.h"

#include <array>
#include <memory>
#include <stdexcept>

#include <folly/ScopeGuard.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <openssl/evp.h>

namespace pangrova::jni {
namespace {

// Hash the canonical request without retaining another plan-sized encoding.
class RequestDigestStream final : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  RequestDigestStream() : digest_(EVP_MD_CTX_new(), EVP_MD_CTX_free) {
    if (!digest_ || EVP_DigestInit_ex(digest_.get(), EVP_sha256(), nullptr) != 1) {
      throw std::runtime_error("Cannot initialize creation identity digest");
    }
  }
  bool Next(void** data, int* size) override {
    flush();
    *data = buffer_.data();
    *size = static_cast<int>(buffer_.size());
    used_ = *size;
    return true;
  }
  void BackUp(int count) override {
    if (count < 0 || count > used_) throw std::logic_error("Invalid digest stream backup");
    used_ -= count;
  }
  int64_t ByteCount() const override { return written_ + used_; }
  std::string finish() {
    flush();
    std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
    unsigned int size = 0;
    if (EVP_DigestFinal_ex(digest_.get(), bytes.data(), &size) != 1 || size != 32) {
      throw std::runtime_error("Cannot complete creation identity digest");
    }
    return {reinterpret_cast<const char*>(bytes.data()), size};
  }
 private:
  void flush() {
    if (used_ > 0 && EVP_DigestUpdate(digest_.get(), buffer_.data(), static_cast<size_t>(used_)) != 1) {
      throw std::runtime_error("Cannot hash creation identity");
    }
    written_ += used_;
    used_ = 0;
  }
  std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> digest_;
  std::array<char, 4096> buffer_{};
  int used_{0};
  int64_t written_{0};
};

}  // namespace

protocol::v2::ExecutorCreationRecoveryRequest creationIdentity(protocol::v2::TaskRequest& request) {
  protocol::v2::ExecutorCreationRecoveryRequest identity;
  auto* seal = identity.mutable_admission_seal();
  *seal->mutable_protocol() = request.protocol();
  *seal->mutable_attempt() = request.attempt();
  seal->set_deadline_epoch_ms(request.deadline_epoch_ms());
  *identity.mutable_security() = request.security();
  // These bindings are Worker-local injection, not part of the NETWORK digest.
  google::protobuf::RepeatedPtrField<protocol::v2::ObjectStoreExecutionBinding> bindings;
  bindings.Swap(request.mutable_worker_local_storage_bindings());
  protocol::v2::WorkerLocalCopyInput copy;
  const bool hadCopy = request.has_worker_local_copy_input();
  if (hadCopy) copy.Swap(request.mutable_worker_local_copy_input());
  request.clear_worker_local_copy_input();
  auto restore = folly::makeGuard([&] {
    bindings.Swap(request.mutable_worker_local_storage_bindings());
    if (hadCopy) copy.Swap(request.mutable_worker_local_copy_input());
  });
  RequestDigestStream stream;
  {
    google::protobuf::io::CodedOutputStream coded(&stream);
    coded.SetSerializationDeterministic(true);
    if (!request.SerializeToCodedStream(&coded) || coded.HadError()) {
      throw std::runtime_error("Cannot encode creation identity");
    }
  }
  auto* checksum = seal->mutable_canonical_request_checksum();
  checksum->set_algorithm(protocol::v2::Checksum::ALGORITHM_SHA256);
  checksum->set_digest(stream.finish());
  validateCreationIdentity(identity);
  return identity;
}

}  // namespace pangrova::jni
