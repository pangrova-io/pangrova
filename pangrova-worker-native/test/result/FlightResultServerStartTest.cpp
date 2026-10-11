#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <barrier>
#include <future>

#include <arrow/util/io_util.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "jni/command/FlightServerRegistry.h"
#include "jni/command/NativeCommandDispatcher.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "result/FlightResultServer.h"

namespace pangrova::result::test {
namespace {
namespace proto = pangrova::protocol::v2;

proto::RuntimeCommandResponse resultServerCommand(proto::RuntimeCommandType type, int port = 0) {
  proto::RuntimeCommand command;
  command.mutable_protocol()->set_major(2);
  command.set_command_type(type);
  if (type == proto::RUNTIME_COMMAND_TYPE_START_RESULT_SERVER) {
    proto::StartResultServerRequest request;
    request.set_preferred_port(port);
    command.mutable_payload()->PackFrom(request);
  }
  std::vector<uint8_t> bytes(command.ByteSizeLong());
  if (!command.SerializeToArray(bytes.data(), static_cast<int>(bytes.size()))) {
    throw std::runtime_error("Failed to serialize result server command");
  }
  const auto encoded = pangrova::jni::dispatchCommand(bytes);
  proto::RuntimeCommandResponse response;
  if (!response.ParseFromArray(encoded.data(), static_cast<int>(encoded.size()))) {
    throw std::runtime_error("Failed to parse actual result server response");
  }
  return response;
}

class FlightResultServerStartTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto& registry = pangrova::jni::FlightServerRegistry::instance();
    registry.shutdownAll();
    registry.configureTransportSecurity(false, "", "", "", "");
    ASSERT_EQ(FlightResultServer::instance(), nullptr);

    // Occupy both address families so gRPC cannot select an unoccupied IPv6
    // listener after the IPv4 bind fails. No SO_REUSEPORT sharing is enabled.
    // The SDK descriptor owner closes the socket even if setup assertions fail.
    occupied_ = arrow::internal::FileDescriptor(::socket(AF_INET6, SOCK_STREAM, 0));
    ASSERT_GE(occupied_.fd(), 0) << std::strerror(errno);
    const int dual_stack = 0;
    ASSERT_EQ(::setsockopt(occupied_.fd(), IPPROTO_IPV6, IPV6_V6ONLY,
                          &dual_stack, sizeof(dual_stack)), 0) << std::strerror(errno);
    int actual_v6_only = -1;
    socklen_t option_size = sizeof(actual_v6_only);
    ASSERT_EQ(::getsockopt(occupied_.fd(), IPPROTO_IPV6, IPV6_V6ONLY,
                          &actual_v6_only, &option_size), 0) << std::strerror(errno);
    ASSERT_EQ(option_size, sizeof(actual_v6_only));
    ASSERT_EQ(actual_v6_only, 0);
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_any;
    ASSERT_EQ(::bind(occupied_.fd(), reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0)
        << std::strerror(errno);
    ASSERT_EQ(::listen(occupied_.fd(), 1), 0) << std::strerror(errno);
    socklen_t size = sizeof(address);
    ASSERT_EQ(::getsockname(occupied_.fd(), reinterpret_cast<sockaddr*>(&address), &size), 0)
        << std::strerror(errno);
    ASSERT_EQ(size, sizeof(address));
    ASSERT_EQ(address.sin6_family, AF_INET6);
    ASSERT_TRUE(IN6_IS_ADDR_UNSPECIFIED(&address.sin6_addr));
    occupied_port_ = ntohs(address.sin6_port);
    ASSERT_GT(occupied_port_, 0);

    // Verify OS exclusivity for each family, not just the configured option.
    arrow::internal::FileDescriptor ipv4_probe(::socket(AF_INET, SOCK_STREAM, 0));
    ASSERT_GE(ipv4_probe.fd(), 0) << std::strerror(errno);
    sockaddr_in ipv4_address{};
    ipv4_address.sin_family = AF_INET;
    ipv4_address.sin_addr.s_addr = htonl(INADDR_ANY);
    ipv4_address.sin_port = htons(occupied_port_);
    const auto ipv4_bind = ::bind(ipv4_probe.fd(), reinterpret_cast<sockaddr*>(&ipv4_address),
                                sizeof(ipv4_address));
    const int ipv4_error = errno;
    ASSERT_EQ(ipv4_bind, -1);
    ASSERT_EQ(ipv4_error, EADDRINUSE) << std::strerror(ipv4_error);

    arrow::internal::FileDescriptor ipv6_probe(::socket(AF_INET6, SOCK_STREAM, 0));
    ASSERT_GE(ipv6_probe.fd(), 0) << std::strerror(errno);
    const int v6_only = 1;
    ASSERT_EQ(::setsockopt(ipv6_probe.fd(), IPPROTO_IPV6, IPV6_V6ONLY,
                          &v6_only, sizeof(v6_only)), 0) << std::strerror(errno);
    const auto ipv6_bind = ::bind(ipv6_probe.fd(), reinterpret_cast<sockaddr*>(&address),
                                sizeof(address));
    const int ipv6_error = errno;
    ASSERT_EQ(ipv6_bind, -1);
    ASSERT_EQ(ipv6_error, EADDRINUSE) << std::strerror(ipv6_error);
  }

  void TearDown() override {
    pangrova::jni::FlightServerRegistry::instance().shutdownAll();
    const auto status = occupied_.Close();
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(FlightResultServer::instance(), nullptr);
  }

  arrow::internal::FileDescriptor occupied_;
  int occupied_port_ = 0;
};

TEST_F(FlightResultServerStartTest, OccupiedListenerThrowsWithoutPublishingThenFreshServerStarts) {
  FlightResultServerConfig config;
  config.port = occupied_port_;
  {
    FlightResultServer rejected(config);
    EXPECT_THROW(rejected.start(), std::runtime_error);
    EXPECT_FALSE(rejected.isRunning());
    EXPECT_EQ(rejected.boundPort(), 0);
    EXPECT_EQ(FlightResultServer::instance(), nullptr);
    EXPECT_EQ(pangrova::jni::FlightServerRegistry::instance().resultServer(), nullptr);
    EXPECT_NO_THROW(rejected.stop());
  }

  FlightResultServer recovered(FlightResultServerConfig{});
  EXPECT_GT(recovered.start(), 0);
  EXPECT_TRUE(recovered.isRunning());
  EXPECT_EQ(FlightResultServer::instance(), &recovered);
  EXPECT_THROW(recovered.start(), std::logic_error);
  recovered.stop();
  EXPECT_FALSE(recovered.isRunning());
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  EXPECT_NO_THROW(recovered.stop());
}

TEST_F(FlightResultServerStartTest, DispatcherReturnsActualFailureWithoutRegistryLeakAndRetryCanDrain) {
  const auto rejected = resultServerCommand(
      proto::RUNTIME_COMMAND_TYPE_START_RESULT_SERVER, occupied_port_);
  EXPECT_FALSE(rejected.success());
  EXPECT_FALSE(rejected.has_payload());
  EXPECT_NE(rejected.error().message().find("Failed to init Flight result server:"), std::string::npos);
  auto& registry = pangrova::jni::FlightServerRegistry::instance();
  EXPECT_EQ(registry.resultServer(), nullptr);
  EXPECT_EQ(FlightResultServer::instance(), nullptr);

  const auto recovered = resultServerCommand(proto::RUNTIME_COMMAND_TYPE_START_RESULT_SERVER);
  ASSERT_TRUE(recovered.success()) << recovered.error().message();
  proto::StartResultServerResponse started;
  ASSERT_TRUE(recovered.payload().UnpackTo(&started));
  EXPECT_GT(started.bound_port(), 0);
  ASSERT_NE(registry.resultServer(), nullptr);
  EXPECT_EQ(registry.resultServer()->boundPort(), started.bound_port());
  EXPECT_TRUE(registry.resultServer()->isRunning());

  EXPECT_TRUE(resultServerCommand(proto::RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER).success());
  EXPECT_EQ(registry.resultServer(), nullptr);
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  EXPECT_TRUE(resultServerCommand(proto::RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER).success());
}

TEST_F(FlightResultServerStartTest, ConcurrentCommandsUseOneActualResultServiceAndOneTransportJoin) {
  std::barrier start(5);
  std::vector<std::future<proto::RuntimeCommandResponse>> starts;
  for (int caller = 0; caller < 4; ++caller) {
    starts.push_back(std::async(std::launch::async, [&] {
      start.arrive_and_wait();
      return resultServerCommand(proto::RUNTIME_COMMAND_TYPE_START_RESULT_SERVER);
    }));
  }
  start.arrive_and_wait();
  int port = 0;
  for (auto& call : starts) {
    const auto response = call.get();
    ASSERT_TRUE(response.success()) << response.error().message();
    proto::StartResultServerResponse started;
    ASSERT_TRUE(response.payload().UnpackTo(&started));
    if (port == 0) port = started.bound_port();
    EXPECT_EQ(started.bound_port(), port);
  }
  auto& registry = pangrova::jni::FlightServerRegistry::instance();
  const auto actual_owner = registry.resultServer();
  ASSERT_NE(actual_owner, nullptr);
  EXPECT_EQ(actual_owner->boundPort(), port);
  EXPECT_EQ(FlightResultServer::instance(), actual_owner.get());
  std::barrier stop(5);
  std::vector<std::future<proto::RuntimeCommandResponse>> stops;
  for (int caller = 0; caller < 4; ++caller) {
    stops.push_back(std::async(std::launch::async, [&] {
      stop.arrive_and_wait();
      return resultServerCommand(proto::RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER);
    }));
  }
  stop.arrive_and_wait();
  for (auto& call : stops) EXPECT_TRUE(call.get().success());
  EXPECT_EQ(registry.resultServer(), nullptr);
  EXPECT_EQ(FlightResultServer::instance(), nullptr);
  // Holding the actual snapshot cannot resurrect a service or perform a
  // second thread join after its authoritative registry owner has closed.
  EXPECT_FALSE(actual_owner->isRunning());
  EXPECT_NO_THROW(actual_owner->stop());
}

}  // namespace
}  // namespace pangrova::result::test
