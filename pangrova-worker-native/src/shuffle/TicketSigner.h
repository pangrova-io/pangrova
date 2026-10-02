// TicketSigner: consumer-side HMAC-SHA256 signing for shuffle DoGet tickets.
//
// Symmetric counterpart to TicketVerifier. Produces tickets in the exact wire
// format the server expects:
//
//   "payload|expiry_epoch_ms|hmac_hex"
//     - payload:        shuffle partition routing info (the plaintext ticket)
//     - expiry_epoch_ms: now + ttl, in Unix epoch milliseconds
//     - hmac_hex:       lowercase hex HMAC-SHA256(key, "payload|expiry_epoch_ms")
//
// When constructed with an empty key, signing is a no-op: sign() returns the
// payload unchanged so unsecured deployments keep working (the server only
// verifies when it too has a key configured).
//
// @ThreadSafe — sign() is stateless apart from the immutable key/ttl.
//
// Placement: shuffle/ — Native shuffle security components.
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace pangrova::shuffle {

// Default ticket lifetime. Tickets are signed immediately before DoGet and
// verified on arrival, so a few minutes amply covers retry backoff and clock
// skew between producer and consumer while still bounding replay.
inline constexpr int64_t kDefaultTicketTtlMillis = 600'000;  // 10 minutes

class TicketSigner {
 public:
  struct SignedTicket {
    std::string ticket;
    int64_t expires_at_epoch_ms;
  };
  // @param key        Raw HMAC key. Empty disables signing (plaintext passthrough).
  // @param ttlMillis  Ticket lifetime added to the current time as the expiry.
  explicit TicketSigner(std::string key,
                        int64_t ttlMillis = kDefaultTicketTtlMillis);

  // Returns the signed ticket, or the plaintext payload when signing disabled.
  std::string sign(std::string_view payload) const;

  // Returns the opaque ticket together with the exact expiry embedded in its
  // signature, allowing the control plane to schedule proactive renewal.
  SignedTicket signWithMetadata(std::string_view payload) const;

  // The caller's canonical grant owns the exact finite expiry.
  SignedTicket signWithExpiry(std::string_view payload, int64_t expiryEpochMillis) const;

  // True when a key is configured (signing active).
  bool enabled() const { return !key_.empty(); }

 private:
  std::string key_;
  int64_t ttlMillis_;
};

}  // namespace pangrova::shuffle
