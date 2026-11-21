// Tests for TicketVerifier: HMAC-SHA256 ticket verification for shuffle DoGet.
//
// Covers five scenarios:
//   1. Valid ticket — correctly signed, non-expired → verify() returns OK
//   2. Expired ticket — correctly signed but expiry in the past → error
//   3. Tampered ticket — modified payload but original signature → error
//   4. Key rotation — verify with previous key succeeds during rotation window
//   5. Invalid format — malformed ticket string → error

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <string>

#include <openssl/hmac.h>

#include "engine/shuffle/TicketVerifier.h"

namespace pangrova::shuffle {
namespace {

// Test helper: compute HMAC-SHA256 and return lowercase hex string.
// Mirrors TicketVerifier::computeHmac for constructing test tickets.
std::string computeTestHmac(std::string_view key, std::string_view message) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digestLen = 0;

  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(message.data()), message.size(),
       digest, &digestLen);

  static constexpr char kHexChars[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(digestLen * 2);
  for (unsigned int i = 0; i < digestLen; ++i) {
    hex.push_back(kHexChars[(digest[i] >> 4) & 0x0F]);
    hex.push_back(kHexChars[digest[i] & 0x0F]);
  }
  return hex;
}

// Returns current time in milliseconds since epoch.
int64_t nowMillis() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

// Constructs a well-formed ticket string: "payload|expiry_ms|hmac_hex".
std::string buildTicket(std::string_view payload, int64_t expiryMs,
                        std::string_view key) {
  std::string expiryStr = std::to_string(expiryMs);
  std::string signedMessage =
      std::string(payload) + "|" + expiryStr;
  std::string hmac = computeTestHmac(key, signedMessage);
  return std::string(payload) + "|" + expiryStr + "|" + hmac;
}

constexpr std::string_view kTestKey = "super-secret-key-256-bits-long!!";
constexpr std::string_view kTestPayload = "cXVlcnlJZD0xMjMmcGFydGl0aW9uPTQ=";

class TicketVerifierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    verifier_ = std::make_unique<TicketVerifier>(kTestKey);
  }

  std::unique_ptr<TicketVerifier> verifier_;
};

// --- Scenario 1: Valid ticket ---

TEST_F(TicketVerifierTest, ValidTicketReturnsOkWithPayload) {
  // Ticket expires 5 minutes from now.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  auto result = verifier_->verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->payload, kTestPayload);
  EXPECT_EQ(result->expiry_epoch_ms, expiry);
}

TEST_F(TicketVerifierTest, ValidTicketWithPipeInPayload) {
  // Payload containing '|' characters — verifier splits from the right.
  std::string_view payloadWithPipe = "query|id=42|partition=7";
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(payloadWithPipe, expiry, kTestKey);

  auto result = verifier_->verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->payload, payloadWithPipe);
}

// --- Scenario 2: Expired ticket ---

TEST_F(TicketVerifierTest, ExpiredTicketReturnsError) {
  // Ticket expired 10 seconds ago.
  int64_t expiry = nowMillis() - 10'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, ExpiryExactlyAtNowReturnsError) {
  // Edge case: expiry == now (should be rejected since check is <=).
  int64_t expiry = nowMillis();
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

// --- Scenario 3: Tampered ticket ---

TEST_F(TicketVerifierTest, TamperedPayloadReturnsError) {
  // Build a valid ticket, then modify the payload.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  // Replace the payload portion (before the second-to-last '|').
  std::string tampered = "TAMPERED_PAYLOAD" + ticket.substr(ticket.rfind('|',
      ticket.rfind('|') - 1));

  auto result = verifier_->verify(tampered);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, TamperedSignatureReturnsError) {
  // Build a valid ticket, then corrupt the HMAC signature.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  // Flip the last character of the signature.
  ticket.back() = (ticket.back() == 'a') ? 'b' : 'a';

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, WrongKeyReturnsError) {
  // Sign with a different key than what the verifier holds.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, "wrong-key-not-matching!!");

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

// --- Scenario 4: Key rotation ---

TEST_F(TicketVerifierTest, PreviousKeyAcceptedDuringRotation) {
  // Ticket signed with the original key.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  // Rotate to a new key — original becomes previousKey.
  std::string_view newKey = "brand-new-rotation-key-256bits!!";
  verifier_->rotateKey(newKey);

  // Ticket signed with old key should still verify (rotation window).
  auto result = verifier_->verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->payload, kTestPayload);
}

TEST_F(TicketVerifierTest, NewKeyAcceptedAfterRotation) {
  // Rotate to a new key.
  std::string_view newKey = "brand-new-rotation-key-256bits!!";
  verifier_->rotateKey(newKey);

  // Ticket signed with the new key should verify.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, newKey);

  auto result = verifier_->verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result->payload, kTestPayload);
}

TEST_F(TicketVerifierTest, DoubleRotationInvalidatesOriginalKey) {
  // Ticket signed with the original key.
  int64_t expiry = nowMillis() + 300'000;
  std::string ticket = buildTicket(kTestPayload, expiry, kTestKey);

  // Rotate twice: original → previous → evicted.
  verifier_->rotateKey("second-key-for-rotation-test!!");
  verifier_->rotateKey("third-key-for-rotation-testing!");

  // Original key is no longer in the rotation window.
  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, ExplicitKeyWindowReplacesBothKeysAtomically) {
  constexpr std::string_view newKey =
      "replacement-current-key-32-bytes!!";
  constexpr std::string_view previousKey =
      "replacement-previous-key-32bytes!";
  verifier_->setKeys(newKey, previousKey);
  int64_t expiry = nowMillis() + 300'000;

  auto currentResult = verifier_->verify(
      buildTicket(kTestPayload, expiry, newKey));
  auto previousResult = verifier_->verify(
      buildTicket(kTestPayload, expiry, previousKey));
  auto evictedResult = verifier_->verify(
      buildTicket(kTestPayload, expiry, kTestKey));

  EXPECT_TRUE(currentResult.ok());
  EXPECT_TRUE(previousResult.ok());
  EXPECT_FALSE(evictedResult.ok());
}

// --- Scenario 5: Invalid format ---

TEST_F(TicketVerifierTest, EmptyStringReturnsError) {
  auto result = verifier_->verify("");
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, NoDelimiterReturnsError) {
  auto result = verifier_->verify("nodelmiterhere");
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, SingleDelimiterReturnsError) {
  auto result = verifier_->verify("payload|signature");
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, NonNumericExpiryReturnsError) {
  // Construct a ticket with non-numeric expiry.
  std::string ticket = std::string(kTestPayload) + "|not_a_number|" +
                       std::string(64, 'a');  // 64-char fake signature
  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, TruncatedSignatureReturnsError) {
  // Valid payload and expiry but signature is too short.
  int64_t expiry = nowMillis() + 300'000;
  std::string expiryStr = std::to_string(expiry);
  std::string ticket =
      std::string(kTestPayload) + "|" + expiryStr + "|" + "abcdef";

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(TicketVerifierTest, EmptyPayloadReturnsError) {
  // Empty payload section.
  int64_t expiry = nowMillis() + 300'000;
  std::string expiryStr = std::to_string(expiry);
  std::string signedMessage = "|" + expiryStr;
  std::string hmac = computeTestHmac(kTestKey, signedMessage);
  std::string ticket = "|" + expiryStr + "|" + hmac;

  auto result = verifier_->verify(ticket);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalid());
}

}  // namespace
}  // namespace pangrova::shuffle
