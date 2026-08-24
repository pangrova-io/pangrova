// Tests for GET_CAPABILITIES command via NativeCommandDispatcher.
//
// Validates that the command returns the expected Rel types, Expr types,
// and velox_version. Exercises the full dispatch path without a JVM.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

#include "jni/command/NativeCommandDispatcher.h"

#if __has_include("pangrova/native/protocol/v1/command.pb.h")
#include "pangrova/native/protocol/v1/command.pb.h"
#include "pangrova/native/protocol/v1/task.pb.h"
#define PANGROVA_HAS_PROTO 1
#else
#define PANGROVA_HAS_PROTO 0
#endif

#if PANGROVA_HAS_PROTO

namespace pangrova::jni::test {
namespace {

namespace proto = pangrova::native::protocol::v1;

// Helper: build a serialized NativeCommand for GET_CAPABILITIES.
static std::vector<uint8_t> buildGetCapabilitiesCommand() {
  proto::NativeCommand cmd;
  cmd.set_executor_handle(0);  // global command
  cmd.set_command_type(proto::NATIVE_COMMAND_GET_CAPABILITIES);
  // No payload needed for GET_CAPABILITIES.

  std::vector<uint8_t> bytes(cmd.ByteSizeLong());
  cmd.SerializeToArray(bytes.data(), static_cast<int>(bytes.size()));
  return bytes;
}

// Helper: parse NativeCommandResponse from bytes.
static proto::NativeCommandResponse parseResponse(
    const std::vector<uint8_t>& bytes) {
  proto::NativeCommandResponse resp;
  EXPECT_TRUE(resp.ParseFromArray(bytes.data(),
                                  static_cast<int>(bytes.size())));
  return resp;
}

// Helper: parse NativeCapabilityResponse from result_payload.
static proto::NativeCapabilityResponse parseCapabilities(
    const proto::NativeCommandResponse& resp) {
  proto::NativeCapabilityResponse caps;
  EXPECT_TRUE(caps.ParseFromString(resp.result_payload()));
  return caps;
}

class NativeCommandGetCapabilitiesTest : public ::testing::Test {};

TEST_F(NativeCommandGetCapabilitiesTest, ReturnsSuccessResponse) {
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto respBytes = dispatchCommand(cmdBytes);

  ASSERT_FALSE(respBytes.empty());
  auto resp = parseResponse(respBytes);
  EXPECT_TRUE(resp.success());
  EXPECT_TRUE(resp.error_message().empty());
}

TEST_F(NativeCommandGetCapabilitiesTest, ContainsExpectedRelTypes) {
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto resp = parseResponse(dispatchCommand(cmdBytes));
  auto caps = parseCapabilities(resp);

  std::unordered_set<std::string> relTypes(
      caps.supported_rel_types().begin(),
      caps.supported_rel_types().end());

  // All Rel types aligned with VeloxPlanConverter::convertRel.
  EXPECT_TRUE(relTypes.count("ReadRel"));
  EXPECT_TRUE(relTypes.count("FilterRel"));
  EXPECT_TRUE(relTypes.count("ProjectRel"));
  EXPECT_TRUE(relTypes.count("AggregateRel"));
  EXPECT_TRUE(relTypes.count("HashJoinRel"));
  EXPECT_TRUE(relTypes.count("JoinRel"));
  EXPECT_TRUE(relTypes.count("ExchangeRel"));
  EXPECT_TRUE(relTypes.count("SortRel"));
  EXPECT_TRUE(relTypes.count("FetchRel"));
  EXPECT_TRUE(relTypes.count("ConsistentPartitionWindowRel"));
  EXPECT_TRUE(relTypes.count("SetRel"));
  EXPECT_TRUE(relTypes.count("ExtensionMultiRel"));

  EXPECT_EQ(relTypes.size(), 12u);
}

TEST_F(NativeCommandGetCapabilitiesTest, ContainsExpectedExprTypes) {
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto resp = parseResponse(dispatchCommand(cmdBytes));
  auto caps = parseCapabilities(resp);

  std::unordered_set<std::string> exprTypes(
      caps.supported_expr_types().begin(),
      caps.supported_expr_types().end());

  EXPECT_TRUE(exprTypes.count("FieldReference"));
  EXPECT_TRUE(exprTypes.count("Literal"));
  EXPECT_TRUE(exprTypes.count("ScalarFunction"));
  EXPECT_TRUE(exprTypes.count("Cast"));
  EXPECT_TRUE(exprTypes.count("IfThen"));
  EXPECT_TRUE(exprTypes.count("WindowFunction"));

  EXPECT_EQ(exprTypes.size(), 6u);
}

TEST_F(NativeCommandGetCapabilitiesTest, ContainsEngineVersion) {
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto resp = parseResponse(dispatchCommand(cmdBytes));
  auto caps = parseCapabilities(resp);

  EXPECT_FALSE(caps.velox_version().empty());
}

TEST_F(NativeCommandGetCapabilitiesTest, FunctionsListIsEmpty) {
  // Functions are plan-dependent; at capabilities-query time the list is empty.
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto resp = parseResponse(dispatchCommand(cmdBytes));
  auto caps = parseCapabilities(resp);

  EXPECT_EQ(caps.supported_functions_size(), 0);
}

TEST_F(NativeCommandGetCapabilitiesTest, HasNonEmptyResultPayload) {
  auto cmdBytes = buildGetCapabilitiesCommand();
  auto resp = parseResponse(dispatchCommand(cmdBytes));

  EXPECT_FALSE(resp.result_payload().empty());
}

}  // namespace
}  // namespace pangrova::jni::test

#else  // !PANGROVA_HAS_PROTO

// Proto not available — skip all tests.
TEST(NativeCommandGetCapabilitiesTest, DISABLED_ProtoNotAvailable) {
  GTEST_SKIP() << "Proto support not compiled in";
}

#endif  // PANGROVA_HAS_PROTO
