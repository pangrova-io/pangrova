#pragma once

// Real Native component boundary, not SQL/JNI/distributed E2E evidence.
#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "CanonicalTaskExecution.h"
#include "CanonicalTestPlan.h"
#include "capabilities/capability/FunctionCatalogAssembler.h"
#include "capabilities/capability/NativeCapabilityInventory.h"
#include "bridge/RuntimeInitializer.h"
#include "bridge/command/NativeCommandDispatcher.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "engine/velox/runtime/NativeEngineLifecycle.h"
#include "velox/type/Type.h"

namespace pangrova::velox::test {

// Both conversion and execution consume the one actually published runtime
// registry. No private registration, mock factory or alternate executor.
class CanonicalPublishedTaskTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pangrova::jni::NativeRuntimeConfig config;
    config.max_memory_bytes = 256L * 1024 * 1024;
    config.io_thread_count = 2;
    config.num_drivers = 1;
    config.cache_enabled = false;
    auto& runtime = pangrova::jni::NativeEngineLifecycle::instance();
    runtime.initialize(config);
    const auto actual = facebook::velox::exec::FunctionRegistrySnapshot::capture();
    const auto prepared = capability::FunctionCatalogAssembler::assemble(
        actual, capability::runtimeCapabilityInventory(*actual));
    const auto& inventory = prepared->inventory();
    protocol::v2::FunctionCatalogFinalizeRequest request;
    *request.mutable_protocol() = inventory.protocol();
    request.set_binding_declaration_sha256(inventory.function_catalog().binding_declaration_sha256());
    request.set_execution_selection_sha256(inventory.function_catalog().execution_selection_sha256());
    protocol::v2::RuntimeCommand command;
    *command.mutable_protocol() = request.protocol();
    command.set_command_type(protocol::v2::RUNTIME_COMMAND_TYPE_FINALIZE_FUNCTION_CATALOG);
    command.mutable_payload()->PackFrom(request);
    const auto encoded = command.SerializeAsString();
    const auto responseBytes = pangrova::jni::dispatchCommand({encoded.begin(), encoded.end()});
    protocol::v2::RuntimeCommandResponse response;
    ASSERT_TRUE(response.ParseFromArray(responseBytes.data(), static_cast<int>(responseBytes.size())));
    ASSERT_TRUE(response.success()) << response.error().message();
    protocol::v2::FunctionCatalogFinalizeResult published;
    ASSERT_TRUE(response.payload().UnpackTo(&published));
    EXPECT_EQ(published.manifest_sha256(), inventory.function_catalog().manifest_sha256());
    registry_ = runtime.functionCatalogStartup()->requirePublished()->registry();
    ASSERT_NE(registry_, nullptr);
  }

  void TearDown() override {
    // Test-local outputs/tasks must leave scope before retiring this incarnation.
    registry_.reset();
    pangrova::jni::NativeEngineLifecycle::instance().shutdown();
    pangrova::test::detail::ensureRuntimeInitialized();
  }

  facebook::velox::core::PlanNodePtr convert(const substrait::Plan& plan) const {
    VeloxPlanConverter converter(registry_);
    const auto bytes = pangrova::test::canonicalTestPlan(plan).SerializeAsString();
    auto node = converter.convert({bytes.begin(), bytes.end()}, {});
    EXPECT_EQ(node->outputType()->size(), converter.resultColumnLabels().size());
    return node;
  }

  CanonicalTaskOutput run(const substrait::Plan& plan) const {
    return executeCanonicalTask(convert(plan), registry_);
  }

  void rejectContract(const substrait::Plan& plan, const std::string& reason) const {
    try {
      convert(plan);
      ADD_FAILURE() << "Misdeclared canonical contract was accepted";
    } catch (const std::runtime_error& error) {
      EXPECT_NE(std::string(error.what()).find(reason), std::string::npos) << error.what();
    }
  }

  static substrait::Type decimalType(int precision, int scale) {
    substrait::Type type;
    auto* decimal = type.mutable_decimal();
    decimal->set_precision(precision);
    decimal->set_scale(scale);
    decimal->set_nullability(substrait::Type::NULLABILITY_NULLABLE);
    return type;
  }

  static substrait::Expression::Literal decimalValue(
      int precision, int scale, facebook::velox::int128_t value) {
    substrait::Expression::Literal literal;
    literal.set_nullable(true);
    auto* decimal = literal.mutable_decimal();
    decimal->set_precision(precision);
    decimal->set_scale(scale);
    const auto bits = static_cast<__uint128_t>(value);
    std::string bytes(16, '\0');
    for (size_t index = 0; index < bytes.size(); ++index) {
      bytes[index] = static_cast<char>((bits >> (index * 8)) & 0xff);
    }
    decimal->set_value(bytes);
    return literal;
  }

  static substrait::Expression field(int ordinal) {
    substrait::Expression expression;
    auto* reference = expression.mutable_selection();
    reference->mutable_root_reference();
    reference->mutable_direct_reference()->mutable_struct_field()->set_field(ordinal);
    return expression;
  }

  static void declare(substrait::Plan& plan, uint32_t anchor, const std::string& name) {
    auto* function = plan.add_extensions()->mutable_extension_function();
    function->set_function_anchor(anchor);
    function->set_name(name);
  }

  FunctionRegistryOwner registry_;
};
}  // namespace pangrova::velox::test
