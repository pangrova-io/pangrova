#include "result/ResultReadAuthority.h"

#include <limits>
#include <openssl/evp.h>
#include <google/protobuf/util/message_differencer.h>
#include "shuffle/ShuffleTicketKeys.h"
#include "shuffle/TicketVerifier.h"

namespace pangrova::result {
namespace {
constexpr std::string_view kDomain = "pangrova.result.read.v1:";
constexpr size_t kMaximumTicketBytes = 64 * 1024;

bool noUnknownFields(const google::protobuf::Message& message) {
  return message.GetReflection()->GetUnknownFields(message).field_count() == 0;
}

arrow::Result<std::string> decodePayload(std::string_view text) {
  if (text.empty() || text.size() % 4 != 0) return arrow::Status::Invalid("Invalid result authority");
  const size_t padding = text.ends_with("==") ? 2 : (text.ends_with("=") ? 1 : 0);
  for (size_t i = 0; i < text.size() - padding; ++i) {
    const auto c = text[i];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') || c == '+' || c == '/')) {
      return arrow::Status::Invalid("Invalid result authority");
    }
  }
  std::string decoded(text.size() / 4 * 3, '\0');
  const auto count = EVP_DecodeBlock(reinterpret_cast<unsigned char*>(decoded.data()),
      reinterpret_cast<const unsigned char*>(text.data()), static_cast<int>(text.size()));
  if (count < 0 || static_cast<size_t>(count) < padding) return arrow::Status::Invalid("Invalid result authority");
  decoded.resize(static_cast<size_t>(count) - padding);
  return decoded;
}
}  // namespace

arrow::Result<protocol::v2::ResultReadGrant> authenticateResultRead(std::string_view ticket) {
  auto& keys = shuffle::ShuffleTicketKeys::instance();
  const auto current = keys.currentKey();
  if (current.size() < 32 || ticket.size() > kMaximumTicketBytes) {
    return arrow::Status::Invalid("Invalid result authority");
  }
  shuffle::TicketVerifier verifier(current, keys.previousKey());
  ARROW_ASSIGN_OR_RAISE(auto verified, verifier.verify(ticket));
  if (!verified.payload.starts_with(kDomain)) return arrow::Status::Invalid("Invalid result authority");
  ARROW_ASSIGN_OR_RAISE(auto bytes, decodePayload(std::string_view(verified.payload).substr(kDomain.size())));
  protocol::v2::ResultReadGrant grant;
  if (!grant.ParseFromString(bytes) || !noUnknownFields(grant) ||
      grant.scope() != protocol::v2::RESULT_READ_SCOPE_RESULT_STREAM ||
      grant.expires_at_epoch_ms() != static_cast<uint64_t>(verified.expiry_epoch_ms)) {
    return arrow::Status::Invalid("Invalid result authority");
  }
  return grant;
}

arrow::Status validateResultRead(const protocol::v2::ResultReadGrant& grant,
    const protocol::v2::ExecutionCallbackContext& admitted) {
  using google::protobuf::util::MessageDifferencer;
  if (!grant.has_security() || !grant.has_attempt() || !grant.attempt().has_execution_owner() ||
      !noUnknownFields(grant.security()) || !noUnknownFields(grant.attempt()) ||
      !noUnknownFields(grant.attempt().execution_owner()) ||
      grant.scope() != protocol::v2::RESULT_READ_SCOPE_RESULT_STREAM ||
      grant.expires_at_epoch_ms() != admitted.deadline_epoch_ms() ||
      !MessageDifferencer::Equals(grant.security(), admitted.security()) ||
      !MessageDifferencer::Equals(grant.attempt(), admitted.attempt())) {
    return arrow::Status::Invalid("Invalid result authority");
  }
  return arrow::Status::OK();
}
}  // namespace pangrova::result
