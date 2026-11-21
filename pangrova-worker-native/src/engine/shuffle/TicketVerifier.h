// TicketVerifier: HMAC-SHA256 ticket verification for shuffle DoGet requests.
//
// Validates that incoming shuffle tickets are authentically signed by the
// Coordinator and have not expired. Supports dual-key rotation: during key
// rollover, both current and previous keys are accepted.
//
// Ticket wire format: "payload|expiry_epoch_ms|hmac_hex"
//   - payload: base64-encoded shuffle partition routing info
//   - expiry_epoch_ms: Unix epoch milliseconds after which the ticket is invalid
//   - hmac_hex: lowercase hex HMAC-SHA256(key, "payload|expiry_epoch_ms")
//
// Thread safety: all public methods are thread-safe. Key rotation is protected
// by an internal mutex. Verification uses shared access (read path is lock-free
// when no rotation is in progress).
//
// Placement: shuffle/ — Native shuffle security components.
#pragma once

#include <mutex>
#include <string>
#include <string_view>

#include <arrow/result.h>
#include <arrow/status.h>

namespace pangrova::shuffle {

// Parsed ticket payload after successful verification.
struct TicketPayload {
  // Original payload content (the routing information).
  std::string payload;
  // Expiry timestamp in milliseconds since epoch.
  int64_t expiry_epoch_ms;
};

// Verifies HMAC-SHA256 signed shuffle tickets.
//
// @ThreadSafe — verify() and rotateKey() are thread-safe.
// Lock ordering: mu_ is the only lock; no nested locking.
class TicketSigner;  // Forward declaration for friend access.

class TicketVerifier {
  friend class TicketSigner;  // TicketSigner reuses computeHmac for signing.
 public:
  // Constructs a verifier with the current HMAC key and an optional previous
  // key for rotation support. Keys are raw bytes (not base64).
  //
  // @param currentKey  The active signing key (must not be empty).
  // @param previousKey Optional previous key accepted during rotation window.
  explicit TicketVerifier(std::string_view currentKey,
                          std::string_view previousKey = "");

  ~TicketVerifier() = default;

  // Non-copyable, non-movable (holds mutex).
  TicketVerifier(const TicketVerifier&) = delete;
  TicketVerifier& operator=(const TicketVerifier&) = delete;

  // Verifies a raw ticket string.
  //
  // Steps:
  //   1. Parse ticket into (payload, expiry, signature)
  //   2. Check expiry > now (reject expired tickets)
  //   3. Compute HMAC-SHA256(currentKey, "payload|expiry") and compare
  //   4. If mismatch and previousKey is non-empty, retry with previousKey
  //   5. Return TicketPayload on success, or Status error on failure
  //
  // Failure returns arrow::Status with code Invalid (no detail leakage in
  // production — caller maps to gRPC UNAUTHENTICATED).
  arrow::Result<TicketPayload> verify(std::string_view rawTicket) const;

  // Rotates the HMAC key: current becomes previous, newCurrentKey becomes
  // current. Thread-safe (acquires mu_).
  void rotateKey(std::string_view newCurrentKey);

  // Atomically replaces the current and previous rotation-window keys.
  void setKeys(std::string_view currentKey, std::string_view previousKey);

 private:
  // Computes HMAC-SHA256 of message using the given key.
  // Returns lowercase hex string of the digest.
  static std::string computeHmac(std::string_view key,
                                 std::string_view message);

  // Constant-time comparison of two hex strings.
  static bool constantTimeEqual(std::string_view a, std::string_view b);

  // Returns current time in milliseconds since epoch.
  static int64_t currentTimeMillis();

  mutable std::mutex mu_;
  std::string currentKey_;
  std::string previousKey_;
};

}  // namespace pangrova::shuffle
