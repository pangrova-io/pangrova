#include "capability/FunctionContractEncoding.h"

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "openssl/evp.h"

#include <array>
#include <stdexcept>

namespace pangrova::capability {

std::string encodeFunctionContract(const google::protobuf::Message& message) {
  std::string bytes;
  {
    google::protobuf::io::StringOutputStream output(&bytes);
    google::protobuf::io::CodedOutputStream coded(&output);
    coded.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&coded) || coded.HadError()) {
      throw std::runtime_error("Canonical function contract serialization failed");
    }
  }
  return bytes;
}

std::string functionContractDigest(const google::protobuf::Message& message) {
  const auto bytes = encodeFunctionContract(message);
  std::array<unsigned char, 32> digest{};
  unsigned int length = 0;
  if (EVP_Digest(bytes.data(), bytes.size(), digest.data(), &length,
                 EVP_sha256(), nullptr) != 1 || length != digest.size()) {
    throw std::runtime_error("Canonical function contract digest failed");
  }
  return std::string(reinterpret_cast<const char*>(digest.data()), digest.size());
}

}  // namespace pangrova::capability
