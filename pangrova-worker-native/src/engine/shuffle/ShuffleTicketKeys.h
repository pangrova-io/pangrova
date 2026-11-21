// ShuffleTicketKeys: process-global holder for the shuffle ticket HMAC secret.
//
// Single source of truth shared by the producer-side verifier
// (FlightShuffleServer / TicketVerifier) and the consumer-side signer
// (PangrovaExchangeSource / TicketSigner). Populated once at worker startup via
// JNI nativeSetHmacKey. When the current key is empty, HMAC is disabled and
// tickets travel in plaintext (legacy / unsecured deployments).
//
// @ThreadSafe — all accessors are mutex-guarded.
//
// Placement: shuffle/ — Native shuffle security components.
#pragma once

#include <mutex>
#include <string>

namespace pangrova::shuffle {

class ShuffleTicketKeys {
 public:
  static ShuffleTicketKeys& instance();

  // Sets the active and (optional) previous rotation key. Keys are raw bytes.
  void set(std::string currentKey, std::string previousKey);

  // Returns the active signing/verification key (empty when HMAC disabled).
  std::string currentKey() const;

  // Returns the previous key accepted during a rotation window (may be empty).
  std::string previousKey() const;

  // True when a current key is configured (i.e. HMAC is enabled).
  bool enabled() const;

 private:
  ShuffleTicketKeys() = default;

  mutable std::mutex mu_;
  std::string currentKey_;
  std::string previousKey_;
};

}  // namespace pangrova::shuffle
