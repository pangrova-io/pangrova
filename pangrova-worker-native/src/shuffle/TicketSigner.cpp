// TicketSigner implementation.
//
// Reuses TicketVerifier::computeHmac so the signature is byte-identical to what
// the server recomputes during verification — any divergence here would break
// every shuffle DoGet, so the two paths deliberately share one HMAC routine.

#include "shuffle/TicketSigner.h"

#include <chrono>

#include "shuffle/TicketVerifier.h"

namespace pangrova::shuffle {

namespace {

constexpr char kTicketDelimiter = '|';

int64_t nowMillis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace

TicketSigner::TicketSigner(std::string key, int64_t ttlMillis)
    : key_(std::move(key)), ttlMillis_(ttlMillis) {}

std::string TicketSigner::sign(std::string_view payload) const {
  return signWithMetadata(payload).ticket;
}

TicketSigner::SignedTicket TicketSigner::signWithMetadata(
    std::string_view payload) const {
  return signWithExpiry(payload, nowMillis() + ttlMillis_);
}

TicketSigner::SignedTicket TicketSigner::signWithExpiry(
    std::string_view payload, int64_t expiry) const {
  if (key_.empty()) {
    return SignedTicket{std::string(payload), expiry};
  }

  std::string message;
  message.reserve(payload.size() + 24);
  message.append(payload);
  message.push_back(kTicketDelimiter);
  message.append(std::to_string(expiry));

  std::string hmac = TicketVerifier::computeHmac(key_, message);

  std::string ticket;
  ticket.reserve(message.size() + 1 + hmac.size());
  ticket.append(message);
  ticket.push_back(kTicketDelimiter);
  ticket.append(hmac);
  return SignedTicket{std::move(ticket), expiry};
}

}  // namespace pangrova::shuffle
