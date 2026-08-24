// End-to-end authentication tests for the shuffle data plane: a real
// FlightShuffleServer + a real Arrow Flight client DoGet, exercising the full
// sign (TicketSigner) → verify (TicketVerifier) round trip over the wire.
//
// This is the acceptance test for ADR-002 task #105 ("开启 HMAC 后 shuffle 端到
// 端正常"): before the consumer-side signing fix, enabling a server key broke
// every DoGet because the client only ever sent plaintext tickets.
//
// Scope note — only the *positive* (OK-status) DoGet paths are asserted here.
// This standalone test executable force-loads velox.a (which vendors its own
// Arrow) alongside the self-contained libpangrova_native.dylib, so two Arrow
// copies coexist (see the linker's "ignoring duplicate libraries" warnings).
// When a DoGet *handler returns a non-OK Status*, Arrow Flight's
// TransportStatus::FromStatus performs polymorphic dispatch on the Status's
// StatusDetail across that ODR boundary and crashes (SIGBUS) — for ANY error
// (MakeFlightError or a plain KeyError alike), independent of this fix. In
// production the JVM loads only the single dylib (one Arrow), so the error path
// is fine. The negative contract (unsigned/tampered/expired tickets rejected)
// is therefore covered by the unit tests in TicketSignerTest/TicketVerifierTest,
// not re-asserted over gRPC here.

#include <gtest/gtest.h>

#include <arrow/flight/client.h>

#include "shuffle/FlightShuffleServer.h"
#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/TicketSigner.h"

namespace pangrova::shuffle {
namespace {

constexpr char kHmacKey[] = "e2e-shuffle-secret-key-0123456789";
constexpr int64_t kHandle = 7;

class FlightShuffleHmacE2ETest : public ::testing::Test {
 protected:
  void startServer(bool withHmac) {
    FlightShuffleServerConfig config;
    config.port = 0;  // auto-assign
    config.reuse_port = true;
    if (withHmac) {
      config.hmac_current_key = kHmacKey;
    }
    server_ = std::make_unique<FlightShuffleServer>(config);
    port_ = server_->start();
    ASSERT_GT(port_, 0);
  }

  // Registers the handle and immediately closes the queue so the DoGet stream
  // drains to a clean EOF instead of blocking the test on an open partition.
  void registerEmptyHandle() {
    queue_ = std::make_unique<SerializedShuffleQueue>(16);
    server_->registerHandle(kHandle, queue_.get());
    queue_->close();
  }

  std::unique_ptr<arrow::flight::FlightClient> connect() {
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port_);
    EXPECT_TRUE(loc.ok()) << loc.status().ToString();
    auto client = arrow::flight::FlightClient::Connect(*loc);
    EXPECT_TRUE(client.ok()) << client.status().ToString();
    return client.ok() ? std::move(*client) : nullptr;
  }

  // payload wire format: "handle\nquery_id\nstage_id\npartition_id".
  static std::string payload() {
    return std::to_string(kHandle) + "\nq-e2e\n0\n0";
  }

  // Drives DoGet to completion and returns the terminal status (the verify
  // error may surface either on DoGet or on the first stream read).
  static arrow::Status doGetTerminalStatus(arrow::flight::FlightClient* client,
                                           const std::string& ticketBody) {
    arrow::flight::Ticket ticket{ticketBody};
    auto reader = client->DoGet(ticket);
    if (!reader.ok()) {
      return reader.status();
    }
    return (*reader)->ToTable().status();
  }

  void TearDown() override {
    if (server_ && server_->isRunning()) {
      server_->stop();
    }
  }

  std::unique_ptr<FlightShuffleServer> server_;
  std::unique_ptr<SerializedShuffleQueue> queue_;
  int port_ = 0;
};

TEST_F(FlightShuffleHmacE2ETest, SignedTicketAuthorizesDoGet) {
  startServer(/*withHmac=*/true);
  registerEmptyHandle();
  auto client = connect();
  ASSERT_NE(client, nullptr);

  TicketSigner signer(kHmacKey);
  arrow::Status status = doGetTerminalStatus(client.get(), signer.sign(payload()));
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FlightShuffleHmacE2ETest, PlaintextTicketWorksWhenHmacDisabled) {
  startServer(/*withHmac=*/false);
  registerEmptyHandle();
  auto client = connect();
  ASSERT_NE(client, nullptr);

  // Back-compat: when no key is configured the legacy plaintext path still works.
  arrow::Status status = doGetTerminalStatus(client.get(), payload());
  EXPECT_TRUE(status.ok()) << status.ToString();
}

}  // namespace
}  // namespace pangrova::shuffle
