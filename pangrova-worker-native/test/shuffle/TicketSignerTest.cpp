// Round-trip tests for the consumer-side TicketSigner against the server-side
// TicketVerifier. The two must agree byte-for-byte on the wire format, so the
// authoritative test is "sign then verify".

#include "shuffle/TicketSigner.h"

#include <gtest/gtest.h>

#include "shuffle/TicketVerifier.h"

namespace pangrova::shuffle {
namespace {

// A representative plaintext payload: "handle\nquery_id\nstage_id\npartition_id".
constexpr char kPayload[] = "42\nq-abc-123\n3\n7";

TEST(TicketSignerTest, SignedTicketVerifiesRoundTrip) {
  const std::string key = "test-secret-key-0123456789";
  TicketSigner signer(key);
  std::string ticket = signer.sign(kPayload);

  TicketVerifier verifier(key);
  auto result = verifier.verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result->payload, kPayload);
  EXPECT_GT(result->expiry_epoch_ms, 0);
}

TEST(TicketSignerTest, PayloadContainingDelimiterSurvives) {
  // The verifier splits from the right, so a '|' inside the payload is safe.
  const std::string key = "k";
  TicketSigner signer(key);
  const std::string payload = "a|b\n1\n2\n3";
  std::string ticket = signer.sign(payload);

  TicketVerifier verifier(key);
  auto result = verifier.verify(ticket);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result->payload, payload);
}

TEST(TicketSignerTest, EmptyKeyProducesPlaintextPassthrough) {
  TicketSigner signer("");
  EXPECT_FALSE(signer.enabled());
  EXPECT_EQ(signer.sign(kPayload), kPayload);
}

TEST(TicketSignerTest, TamperedSignatureRejected) {
  const std::string key = "k2";
  TicketSigner signer(key);
  std::string ticket = signer.sign(kPayload);
  ticket.back() ^= 0x1;  // flip a bit in the trailing hmac hex

  TicketVerifier verifier(key);
  EXPECT_FALSE(verifier.verify(ticket).ok());
}

TEST(TicketSignerTest, WrongKeyRejected) {
  TicketSigner signer("keyA");
  std::string ticket = signer.sign(kPayload);

  TicketVerifier verifier("keyB");
  EXPECT_FALSE(verifier.verify(ticket).ok());
}

TEST(TicketSignerTest, ExpiredTicketRejected) {
  const std::string key = "k3";
  TicketSigner signer(key, /*ttlMillis=*/-1000);  // expiry already in the past
  std::string ticket = signer.sign(kPayload);

  TicketVerifier verifier(key);
  EXPECT_FALSE(verifier.verify(ticket).ok());
}

TEST(TicketSignerTest, PreviousKeyAcceptedDuringRotation) {
  TicketSigner signer("oldKey");
  std::string ticket = signer.sign(kPayload);

  // Rotation window: server has rotated to "newKey" but still accepts "oldKey".
  TicketVerifier verifier("newKey", "oldKey");
  EXPECT_TRUE(verifier.verify(ticket).ok());
}

}  // namespace
}  // namespace pangrova::shuffle
