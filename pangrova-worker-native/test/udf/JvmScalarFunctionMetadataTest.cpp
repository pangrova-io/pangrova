#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "../TestMemoryHelper.h"
#include "StructuralContractIdentities.h"
#include "udf/JvmScalarFunction.h"
#include "udf/NativeUdfRegistry.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Variant.h"

namespace pangrova::udf::test {
namespace {
using namespace facebook::velox;

ScalarUdfSpec declaredCallback(const std::string& name, size_t ordinal = 0) {
  const auto& signature = pangrova::capability::generated::runtimeCallbackSignatures(name)[ordinal];
  return makeJvmScalarFunctionSpec(signature.name,
      std::vector<std::string>(signature.arguments.begin(), signature.arguments.end()),
      signature.returnType, signature.deterministic);
}

// This registers the production JVM-backed function and examines real Velox
// compilation metadata; it does not install a Java callback or invoke a model.
class JvmScalarFunctionMetadataTest : public ::testing::TestWithParam<std::tuple<bool, bool>> {};

TEST_P(JvmScalarFunctionMetadataTest, BothRegistrationPathsPreserveExplicitDeterminism) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto [stateful, deterministic] = GetParam();
  auto spec = declaredCallback("file_from_url");
  spec.stateful = stateful;
  spec.deterministic = deterministic;
  NativeUdfRegistry registry;
  registry.registerScalar(spec);
  registry.registerAllWithVelox();

  const auto registered = exec::getVectorFunctionMetadata(spec.name);
  ASSERT_TRUE(registered.has_value());
  EXPECT_EQ(registered->deterministic, deterministic);
  EXPECT_TRUE(registered->defaultNullBehavior);
  const core::QueryConfig config({});
  const auto selected = exec::getVectorFunctionWithMetadata(spec.name, spec.inputTypes, {}, config);
  ASSERT_TRUE(selected.has_value());
  ASSERT_NE(selected->first, nullptr);
  EXPECT_EQ(selected->second.deterministic, deterministic);
}

INSTANTIATE_TEST_SUITE_P(StatefulnessAndDeterminism, JvmScalarFunctionMetadataTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()));

TEST(JvmScalarFunctionDeclarationTest, MixedOverloadDeterminismIsRejectedBeforePublication) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto first = declaredCallback("ai_complete", 0);
  const auto second = declaredCallback("ai_complete", 1);
  ASSERT_EQ(first.deterministic, second.deterministic);
  NativeUdfRegistry registry;
  registry.registerScalar(first);
  auto contradictory = second;
  contradictory.deterministic = !first.deterministic;
  try {
    registry.registerScalar(contradictory);
    FAIL() << "Mixed name-level execution metadata must fail before publication";
  } catch (const std::runtime_error& error) {
    EXPECT_NE(std::string(error.what()).find("overload determinism must be identical"), std::string::npos);
  }
  ASSERT_TRUE(registry.findScalar(first.name).has_value());
  EXPECT_EQ(registry.findScalar(first.name)->deterministic, first.deterministic);
  // Rejection must not poison the overload set or finalize the registry.
  registry.registerScalar(second);
  registry.registerAllWithVelox();
  const auto signatures = exec::getVectorFunctionSignatures(first.name);
  ASSERT_TRUE(signatures.has_value());
  EXPECT_EQ(signatures->size(), 2);
  const auto metadata = exec::getVectorFunctionMetadata(first.name);
  ASSERT_TRUE(metadata.has_value());
  EXPECT_EQ(metadata->deterministic, first.deterministic);
}

TEST(JvmScalarFunctionDeclarationTest, ConstantFileCallbackIsNotEvaluatedDuringRealExpressionCompilation) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto spec = declaredCallback("file_from_url");
  ASSERT_FALSE(spec.deterministic);
  NativeUdfRegistry registry;
  registry.registerScalar(spec);
  registry.registerAllWithVelox();
  const auto pool = pangrova::test::testLeafPool("constant_file_callback_compilation");
  const auto query = core::QueryCtx::create();
  core::ExecCtx context(pool.get(), query.get());
  const auto input = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant(std::string("s3://bucket/object")));
  const auto call = std::make_shared<core::CallTypedExpr>(spec.outputType,
      std::vector<core::TypedExprPtr>{input}, spec.name);

  // Folding is enabled. An incorrectly deterministic callback would attempt
  // real JvmScalarFunction::apply here, despite no JVM/callback being installed.
  exec::ExprSet expressions({call}, &context, /*enableConstantFolding=*/true);
  ASSERT_EQ(expressions.size(), 1);
  const auto& compiled = expressions.expr(0);
  EXPECT_EQ(compiled->name(), spec.name);
  EXPECT_FALSE(compiled->isDeterministic());
  EXPECT_FALSE(compiled->isConstantExpr());
  EXPECT_FALSE(compiled->isConstant());
  ASSERT_EQ(compiled->inputs().size(), 1);
  EXPECT_TRUE(compiled->inputs().front()->isConstant());
  EXPECT_TRUE(compiled->type()->equivalent(*spec.outputType));
}
}  // namespace
}  // namespace pangrova::udf::test
