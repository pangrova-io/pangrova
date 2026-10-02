// Fail-closed transport security decisions for the Native Flight client.
// Request wait-budget and cancellation behavior is exercised by the real
// Native exchange data-plane test because it depends on the asynchronous
// reader, lifecycle-owned I/O executor and gRPC cancellation working together.

#include "velox/exchange/PangrovaExchangeSource.h"

#include <gtest/gtest.h>

namespace pangrova::velox {
namespace {

using detail::flightTransportSecurityCompatible;

TEST(FlightTransportSecurityTest, LocalModeMustMatchEndpointIdentity) {
  EXPECT_TRUE(flightTransportSecurityCompatible(false, ""));
  EXPECT_TRUE(flightTransportSecurityCompatible(true, "worker-1.internal"));
  EXPECT_FALSE(flightTransportSecurityCompatible(true, ""));
  EXPECT_FALSE(flightTransportSecurityCompatible(false, "worker-1.internal"));
}

TEST(FlightTransportSecurityTest, PartialConfigurationIsRejected) {
  EXPECT_THROW(
      PangrovaExchangeSource::configureTransportSecurity(
          true, "cert.pem", "", "ca.pem", "worker-1.internal"),
      std::invalid_argument);
  EXPECT_THROW(
      PangrovaExchangeSource::configureTransportSecurity(
          false, "cert.pem", "", "", ""),
      std::invalid_argument);
}

TEST(FlightTransportSecurityTest, UnreadableCredentialIsRejected) {
  EXPECT_THROW(
      PangrovaExchangeSource::configureTransportSecurity(
          true,
          "/path/that/does/not/exist/cert.pem",
          "/path/that/does/not/exist/key.pem",
          "/path/that/does/not/exist/ca.pem",
          "worker-1.internal"),
      std::runtime_error);
}

}  // namespace
}  // namespace pangrova::velox
