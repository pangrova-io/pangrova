// End-to-end authentication tests for the shuffle data plane: a real
// FlightShuffleServer + a real Arrow Flight client DoGet, exercising the full
// producer sign (TicketSigner) → verify (TicketVerifier) round trip over the wire.
//
// This is the acceptance test for ADR-002 task #105 ("开启 HMAC 后 shuffle 端到
// 端正常"): consumers only forward opaque producer-issued tickets and never
// receive or reconstruct the producer's process-local signing key.
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

#include <filesystem>
#include <fstream>

#include <arrow/flight/client.h>

#include "engine/shuffle/FlightShuffleServer.h"
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/shuffle/TicketSigner.h"

namespace pangrova::shuffle {
namespace {

constexpr char kHmacKey[] = "e2e-shuffle-secret-key-0123456789";
constexpr char kRotatedHmacKey[] = "e2e-rotated-secret-key-012345678";
constexpr int64_t kHandle = 7;
constexpr uint64_t kPlanEpoch = 1;
constexpr uint32_t kAttemptId = 0;
constexpr uint64_t kFencingToken = 4'294'967'297ULL;

class FlightShuffleHmacE2ETest : public ::testing::Test {
 protected:
  static std::filesystem::path tlsResource(const char* filename) {
    return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() /
        "resources" / "tls" / filename;
  }

  static std::string readFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    EXPECT_TRUE(input.good()) << path;
    return std::string(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
  }

  void startServer(bool mutualTls = false) {
    FlightShuffleServerConfig config;
    config.port = 0;  // auto-assign
    // Authentication tests need an exclusive ephemeral listener. Reuse-port
    // could route a connection to an unrelated local server on the same port.
    config.reuse_port = false;
    config.hmac_current_key = kHmacKey;
    if (mutualTls) {
      config.tls_cert_chain_path = tlsResource("localhost-cert.pem").string();
      config.tls_private_key_path = tlsResource("localhost-key.pem").string();
      config.tls_root_ca_path = tlsResource("localhost-cert.pem").string();
    }
    server_ = std::make_unique<FlightShuffleServer>(config);
    port_ = server_->start();
    ASSERT_GT(port_, 0);
  }

  // Registers the handle and immediately closes the queue so the DoGet stream
  // drains to a clean EOF instead of blocking the test on an open partition.
  void registerEmptyHandle() {
    queue_ = std::make_unique<SerializedShuffleQueue>(16);
    server_->registerHandle(
        kHandle, queue_.get(), "q-e2e", 0, 1,
        kPlanEpoch, kAttemptId, kFencingToken);
    ASSERT_TRUE(queue_->commit());
  }

  std::unique_ptr<arrow::flight::FlightClient> connect(bool mutualTls = false) {
    auto loc = mutualTls
        ? arrow::flight::Location::ForGrpcTls("127.0.0.1", port_)
        : arrow::flight::Location::ForGrpcTcp("127.0.0.1", port_);
    EXPECT_TRUE(loc.ok()) << loc.status().ToString();
    auto options = arrow::flight::FlightClientOptions::Defaults();
    if (mutualTls) {
      options.tls_root_certs = readFile(tlsResource("localhost-cert.pem"));
      options.override_hostname = "localhost";
      options.cert_chain = readFile(tlsResource("localhost-cert.pem"));
      options.private_key = readFile(tlsResource("localhost-key.pem"));
    }
    auto client = arrow::flight::FlightClient::Connect(*loc, options);
    EXPECT_TRUE(client.ok()) << client.status().ToString();
    return client.ok() ? std::move(*client) : nullptr;
  }

  // Payload carries producer routing plus exact fenced attempt identity.
  static std::string payload() {
    return std::to_string(kHandle) + "\nq-e2e\n0\n0\n" +
        std::to_string(kPlanEpoch) + "\n" + std::to_string(kAttemptId) +
        "\n" + std::to_string(kFencingToken);
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
  startServer();
  registerEmptyHandle();
  auto client = connect();
  ASSERT_NE(client, nullptr);

  TicketSigner signer(kHmacKey);
  arrow::Status status = doGetTerminalStatus(client.get(), signer.sign(payload()));
  EXPECT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FlightShuffleHmacE2ETest, LiveRotationAcceptsCurrentAndPreviousKeys) {
  startServer();
  registerEmptyHandle();
  server_->setHmacKeys(kRotatedHmacKey, kHmacKey);
  auto client = connect();
  ASSERT_NE(client, nullptr);

  TicketSigner previousSigner(kHmacKey);
  EXPECT_TRUE(doGetTerminalStatus(
      client.get(), previousSigner.sign(payload())).ok());
  TicketSigner currentSigner(kRotatedHmacKey);
  EXPECT_TRUE(doGetTerminalStatus(
      client.get(), currentSigner.sign(payload())).ok());
}

TEST_F(FlightShuffleHmacE2ETest, SignedTicketAuthorizesDoGetOverMutualTls) {
  startServer(/*mutualTls=*/true);
  registerEmptyHandle();
  auto client = connect(/*mutualTls=*/true);
  ASSERT_NE(client, nullptr);

  TicketSigner signer(kHmacKey);
  arrow::Status status = doGetTerminalStatus(client.get(), signer.sign(payload()));
  EXPECT_TRUE(status.ok()) << status.ToString();
}

}  // namespace
}  // namespace pangrova::shuffle
