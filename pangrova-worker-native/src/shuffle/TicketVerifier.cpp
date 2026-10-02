// TicketVerifier: HMAC-SHA256 ticket verification implementation.
//
// Uses OpenSSL EVP_MAC API for HMAC-SHA256 computation. The EVP_MAC API is
// the recommended interface since OpenSSL 3.0 (replaces deprecated HMAC()).
//
// Security properties:
//   - Constant-time signature comparison (prevents timing side-channels)
//   - No detail leakage on failure (generic "invalid ticket" status)
//   - Dual-key support for zero-downtime key rotation

#include "shuffle/TicketVerifier.h"

#include <chrono>
#include <charconv>
#include <cstring>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "observability/PangrovaLog.h"

namespace pangrova::shuffle {

namespace {

// Delimiter used in ticket wire format.
constexpr char kTicketDelimiter = '|';

// HMAC-SHA256 digest length in bytes.
constexpr size_t kHmacDigestLength = 32;

// Hex-encoded HMAC-SHA256 digest length.
constexpr size_t kHmacHexLength = kHmacDigestLength * 2;

// Converts a byte array to lowercase hex string.
std::string toHex(const unsigned char* data, size_t len) {
  std::string result;
  result.reserve(len * 2);
  static constexpr char kHexChars[] = "0123456789abcdef";
  for (size_t i = 0; i < len; ++i) {
    result.push_back(kHexChars[(data[i] >> 4) & 0x0F]);
    result.push_back(kHexChars[data[i] & 0x0F]);
  }
  return result;
}

// Splits a ticket string by the last two delimiters.
// Ticket format: "payload|expiry_epoch_ms|hmac_hex"
// The payload itself may contain '|' characters, so we split from the right.
bool splitTicket(std::string_view rawTicket,
                 std::string_view& payload,
                 std::string_view& expiryStr,
                 std::string_view& signature) {
  // Find the last delimiter (before hmac_hex).
  auto lastDelim = rawTicket.rfind(kTicketDelimiter);
  if (lastDelim == std::string_view::npos || lastDelim == 0) {
    return false;
  }

  // Find the second-to-last delimiter (before expiry_epoch_ms).
  auto secondDelim = rawTicket.rfind(kTicketDelimiter, lastDelim - 1);
  if (secondDelim == std::string_view::npos) {
    return false;
  }

  payload = rawTicket.substr(0, secondDelim);
  expiryStr = rawTicket.substr(secondDelim + 1, lastDelim - secondDelim - 1);
  signature = rawTicket.substr(lastDelim + 1);

  // Basic validation: all parts must be non-empty.
  if (payload.empty() || expiryStr.empty() || signature.empty()) {
    return false;
  }

  return true;
}

}  // namespace

TicketVerifier::TicketVerifier(std::string_view currentKey,
                               std::string_view previousKey)
    : currentKey_(currentKey), previousKey_(previousKey) {}

arrow::Result<TicketPayload> TicketVerifier::verify(
    std::string_view rawTicket) const {
  // Step 1: Parse ticket into components.
  std::string_view payload;
  std::string_view expiryStr;
  std::string_view signature;

  if (!splitTicket(rawTicket, payload, expiryStr, signature)) {
    return arrow::Status::Invalid("Invalid ticket");
  }

  // Validate signature length (must be hex-encoded HMAC-SHA256 = 64 chars).
  if (signature.size() != kHmacHexLength) {
    return arrow::Status::Invalid("Invalid ticket");
  }

  // Step 2: Parse and check expiry.
  int64_t expiryMs = 0;
  const auto parsed = std::from_chars(expiryStr.data(), expiryStr.data() + expiryStr.size(), expiryMs);
  if (parsed.ec != std::errc{} || parsed.ptr != expiryStr.data() + expiryStr.size()) {
    return arrow::Status::Invalid("Invalid ticket");
  }

  if (expiryMs <= currentTimeMillis()) {
    return arrow::Status::Invalid("Invalid ticket");
  }

  // Step 3: Compute HMAC over "payload|expiry" and compare with signature.
  std::string signedMessage;
  signedMessage.reserve(payload.size() + 1 + expiryStr.size());
  signedMessage.append(payload);
  signedMessage.push_back(kTicketDelimiter);
  signedMessage.append(expiryStr);

  std::string currentKey;
  std::string previousKey;
  {
    std::lock_guard<std::mutex> lock(mu_);
    currentKey = currentKey_;
    previousKey = previousKey_;
  }

  if (currentKey.empty()) return arrow::Status::Invalid("Invalid ticket");

  // Try current key first.
  std::string expectedHmac = computeHmac(currentKey, signedMessage);
  if (constantTimeEqual(expectedHmac, signature)) {
    return TicketPayload{std::string(payload), expiryMs};
  }

  // Step 4: Try previous key if available (rotation window).
  if (!previousKey.empty()) {
    std::string previousHmac = computeHmac(previousKey, signedMessage);
    if (constantTimeEqual(previousHmac, signature)) {
      return TicketPayload{std::string(payload), expiryMs};
    }
  }

  // Step 5: Both keys failed — reject.
  return arrow::Status::Invalid("Invalid ticket");
}

void TicketVerifier::rotateKey(std::string_view newCurrentKey) {
  std::lock_guard<std::mutex> lock(mu_);
  previousKey_ = std::move(currentKey_);
  currentKey_ = std::string(newCurrentKey);
  PANGROVA_LOG(INFO) << "event=ticket_key_rotated";
}

void TicketVerifier::setKeys(std::string_view currentKey,
                             std::string_view previousKey) {
  std::lock_guard<std::mutex> lock(mu_);
  currentKey_ = std::string(currentKey);
  previousKey_ = std::string(previousKey);
  PANGROVA_LOG(INFO) << "event=ticket_keys_replaced"
                    << " has_previous_key=" << !previousKey_.empty();
}

std::string TicketVerifier::computeHmac(std::string_view key,
                                        std::string_view message) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digestLen = 0;

  unsigned char* result = HMAC(
      EVP_sha256(),
      key.data(),
      static_cast<int>(key.size()),
      reinterpret_cast<const unsigned char*>(message.data()),
      message.size(),
      digest,
      &digestLen);

  if (result == nullptr || digestLen != kHmacDigestLength) {
    PANGROVA_LOG(ERROR) << "event=hmac_computation_failed";
    return "";
  }

  return toHex(digest, digestLen);
}

bool TicketVerifier::constantTimeEqual(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  return CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}

int64_t TicketVerifier::currentTimeMillis() {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

}  // namespace pangrova::shuffle
