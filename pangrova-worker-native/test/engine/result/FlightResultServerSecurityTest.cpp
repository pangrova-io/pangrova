#include <gtest/gtest.h>

#include "engine/result/FlightResultServer.h"

namespace pangrova::result {
namespace {

TEST(FlightResultServerSecurityTest, PartialTlsConfigurationIsRejected) {
  FlightResultServerConfig config;
  config.tls_cert_chain_path = "cert.pem";
  FlightResultServer server(config);

  EXPECT_THROW(server.start(), std::invalid_argument);
}

TEST(FlightResultServerSecurityTest, UnreadableTlsCredentialsAreRejected) {
  FlightResultServerConfig config;
  config.tls_cert_chain_path = "/path/that/does/not/exist/cert.pem";
  config.tls_private_key_path = "/path/that/does/not/exist/key.pem";
  config.tls_root_ca_path = "/path/that/does/not/exist/ca.pem";
  FlightResultServer server(config);

  EXPECT_THROW(server.start(), std::runtime_error);
}

}  // namespace
}  // namespace pangrova::result
