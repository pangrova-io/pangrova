#include <gtest/gtest.h>

#include <cctype>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "TestMemoryHelper.h"
#include "StructuralContractIdentities.h"
#include "pangrova/protocol/v2/runtime.pb.h"
#include "udf/JvmScalarFunction.h"
#include "udf/NativeUdfRegistry.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Variant.h"

namespace pangrova::udf::test {
namespace {
using namespace facebook::velox;

protocol::v2::NativeFunctionDescriptor declaredDescriptor(
    const std::string& name, size_t ordinal = 0) {
  const auto& signature = pangrova::capability::generated::runtimeCallbackSignatures(name)[ordinal];
  protocol::v2::NativeFunctionDescriptor descriptor;
  descriptor.set_name(signature.name);
  descriptor.set_function_kind(protocol::v2::NATIVE_FUNCTION_KIND_SCALAR);
  descriptor.mutable_return_type()->set_canonical_sql(signature.returnType);
  for (const auto* argument : signature.arguments) {
    descriptor.add_argument_types()->set_canonical_sql(argument);
  }
  descriptor.set_deterministic(signature.deterministic);
  descriptor.set_default_null_behavior(signature.defaultNullBehavior);
  return descriptor;
}

ScalarUdfSpec declaredCallback(const std::string& name, size_t ordinal = 0) {
  return makeJvmScalarFunctionSpec(declaredDescriptor(name, ordinal));
}

// This registers the production JVM-backed function and examines real Velox
// compilation metadata; it does not install a Java callback or invoke a model.
class JvmScalarFunctionMetadataTest : public ::testing::TestWithParam<bool> {};

TEST_P(JvmScalarFunctionMetadataTest, BothRegistrationPathsPreserveTheActualCanonicalCallbackMetadata) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const bool stateful = GetParam();
  auto spec = declaredCallback("file_from_url");
  spec.stateful = stateful;
  ASSERT_FALSE(spec.deterministic);
  ASSERT_FALSE(spec.defaultNullBehavior);
  NativeUdfRegistry registry;
  registry.registerScalar(spec);
  registry.registerAllWithVelox();

  const auto registered = exec::getVectorFunctionMetadata(spec.name);
  ASSERT_TRUE(registered.has_value());
  EXPECT_EQ(registered->deterministic, spec.deterministic);
  EXPECT_EQ(registered->defaultNullBehavior, spec.defaultNullBehavior);
  const core::QueryConfig config({});
  const auto selected = exec::getVectorFunctionWithMetadata(spec.name, spec.inputTypes, {}, config);
  ASSERT_TRUE(selected.has_value());
  ASSERT_NE(selected->first, nullptr);
  EXPECT_EQ(selected->second.deterministic, spec.deterministic);
  EXPECT_EQ(selected->second.defaultNullBehavior, spec.defaultNullBehavior);
}

INSTANTIATE_TEST_SUITE_P(ActualCallbackRegistrationBranches, JvmScalarFunctionMetadataTest,
    ::testing::Bool());

TEST(JvmScalarFunctionDeclarationTest, ActualBuiltinEntriesRetainTheirDeclaredStrictNullBehavior) {
  pangrova::test::detail::ensureRuntimeInitialized();
  for (const auto& name : {"abs", "rand"}) {
    const auto entries = exec::simpleFunctions().getFunctionMetadataSnapshot(name);
    ASSERT_FALSE(entries.empty());
    for (const auto& entry : entries) {
      ASSERT_NE(entry, nullptr);
      EXPECT_TRUE(entry->defaultNullBehavior());
      EXPECT_EQ(entry->isDeterministic(), std::string(name) == "abs");
    }
  }
}

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

TEST(JvmScalarFunctionDeclarationTest, MalformedActualDescriptorsFailBeforeOwnerOrPublication) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto descriptor = declaredDescriptor("file_from_url");
  const auto beforeSignatures = exec::getVectorFunctionSignatures(descriptor.name());
  const auto spec = makeJvmScalarFunctionSpec(descriptor);
  EXPECT_FALSE(spec.deterministic);
  EXPECT_FALSE(spec.defaultNullBehavior);

  auto missing = descriptor;
  missing.clear_default_null_behavior();
  EXPECT_THROW(makeJvmScalarFunctionSpec(missing), std::invalid_argument);
  auto unknown = descriptor;
  unknown.GetReflection()->MutableUnknownFields(&unknown)->AddVarint(1001, 1);
  EXPECT_THROW(makeJvmScalarFunctionSpec(unknown), std::invalid_argument);
  auto wrongKind = descriptor;
  wrongKind.set_function_kind(protocol::v2::NATIVE_FUNCTION_KIND_AGGREGATE);
  EXPECT_THROW(makeJvmScalarFunctionSpec(wrongKind), std::invalid_argument);
  EXPECT_EQ(exec::getVectorFunctionSignatures(descriptor.name()), beforeSignatures);
}

TEST(JvmScalarFunctionDeclarationTest, CanonicalCallbackCaseAliasesCannotBypassDescriptorAdmission) {
  pangrova::test::detail::ensureRuntimeInitialized();
  for (const auto& name : {"file_from_url", "ai_complete"}) {
    const auto beforeSignatures = exec::getVectorFunctionSignatures(name);
    const auto beforeMetadata = exec::getVectorFunctionMetadata(name);
    const auto declared = pangrova::capability::generated::runtimeCallbackSignatures(name);
    for (size_t ordinal = 0; ordinal < declared.size(); ++ordinal) {
      auto alias = declaredDescriptor(name, ordinal);
      auto caseAlias = alias.name();
      caseAlias.front() = static_cast<char>(std::toupper(static_cast<unsigned char>(caseAlias.front())));
      alias.set_name(caseAlias);
      EXPECT_THROW(makeJvmScalarFunctionSpec(alias), std::invalid_argument);
      alias.set_default_null_behavior(!alias.default_null_behavior());
      EXPECT_THROW(makeJvmScalarFunctionSpec(alias), std::invalid_argument);
    }
    EXPECT_EQ(exec::getVectorFunctionSignatures(name), beforeSignatures);
    const auto afterMetadata = exec::getVectorFunctionMetadata(name);
    ASSERT_EQ(afterMetadata.has_value(), beforeMetadata.has_value());
    if (beforeMetadata) {
      EXPECT_EQ(afterMetadata->deterministic, beforeMetadata->deterministic);
      EXPECT_EQ(afterMetadata->defaultNullBehavior, beforeMetadata->defaultNullBehavior);
    }
  }
}

TEST(JvmScalarFunctionDeclarationTest, ActualCallbackAliasBatchesRejectDuplicatesAndSemanticDriftAtomically) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto beforeFile = exec::getVectorFunctionSignatures("file_from_url");
  const auto beforeAi = exec::getVectorFunctionSignatures("ai_complete");
  const auto beforeFileMetadata = exec::getVectorFunctionMetadata("file_from_url");
  const auto beforeAiMetadata = exec::getVectorFunctionMetadata("ai_complete");
  NativeUdfRegistry registry;
  const auto file = declaredCallback("file_from_url");
  registry.registerScalar(file);
  auto fileAlias = file;
  fileAlias.name = "FILE_FROM_URL";
  EXPECT_THROW(registry.withScalars({declaredCallback("ai_complete"), fileAlias}), std::runtime_error);
  EXPECT_EQ(registry.scalarCount(), 1);
  EXPECT_FALSE(registry.findScalar("AI_COMPLETE").has_value());
  ASSERT_TRUE(registry.findScalar("FILE_FROM_URL").has_value());
  EXPECT_EQ(registry.findScalar("FILE_FROM_URL")->name, file.name);

  for (const bool mutateNull : {false, true}) {
    auto aiAlias = declaredCallback("ai_complete", 1);
    aiAlias.name = "AI_COMPLETE";
    if (mutateNull) aiAlias.defaultNullBehavior = !aiAlias.defaultNullBehavior;
    else aiAlias.deterministic = !aiAlias.deterministic;
    EXPECT_THROW(registry.withScalars({declaredCallback("ai_complete"), aiAlias}), std::runtime_error);
    EXPECT_EQ(registry.scalarCount(), 1);
    EXPECT_FALSE(registry.findScalar("ai_complete").has_value());
  }
  EXPECT_EQ(exec::getVectorFunctionSignatures("file_from_url"), beforeFile);
  EXPECT_EQ(exec::getVectorFunctionSignatures("ai_complete"), beforeAi);
  const auto afterFileMetadata = exec::getVectorFunctionMetadata("file_from_url");
  const auto afterAiMetadata = exec::getVectorFunctionMetadata("ai_complete");
  ASSERT_EQ(afterFileMetadata.has_value(), beforeFileMetadata.has_value());
  ASSERT_EQ(afterAiMetadata.has_value(), beforeAiMetadata.has_value());
  if (beforeFileMetadata) {
    EXPECT_EQ(afterFileMetadata->deterministic, beforeFileMetadata->deterministic);
    EXPECT_EQ(afterFileMetadata->defaultNullBehavior, beforeFileMetadata->defaultNullBehavior);
  }
  if (beforeAiMetadata) {
    EXPECT_EQ(afterAiMetadata->deterministic, beforeAiMetadata->deterministic);
    EXPECT_EQ(afterAiMetadata->defaultNullBehavior, beforeAiMetadata->defaultNullBehavior);
  }
}

TEST(JvmScalarFunctionDeclarationTest, ActualAggregateAliasAndScalarCollisionsFailBeforePublication) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto* actualSum = exec::getAggregateFunctionEntry("sum");
  ASSERT_NE(actualSum, nullptr);
  const auto beforeSum = exec::getAggregateFunctionSignatures("sum");
  const auto beforeFile = exec::getVectorFunctionSignatures("file_from_url");
  AggregateUdfSpec sum;
  sum.name = "SUM";
  sum.inputTypes = {BIGINT()};
  sum.intermediateType = BIGINT();
  sum.outputType = BIGINT();
  sum.aggregateFactory = [factory = actualSum->factory](
      core::AggregationNode::Step step, const std::vector<TypePtr>& inputs, const TypePtr& output) {
    return factory(step, inputs, output, core::QueryConfig({}));
  };
  NativeUdfRegistry aggregates;
  aggregates.registerAggregate(sum);
  ASSERT_TRUE(aggregates.findAggregate("sum").has_value());
  EXPECT_EQ(aggregates.findAggregate("sum")->name, "SUM");
  auto sumAlias = sum;
  sumAlias.name = "sum";
  EXPECT_THROW(aggregates.registerAggregate(sumAlias), std::runtime_error);
  auto scalarCollision = declaredCallback("file_from_url");
  scalarCollision.name = "sUm";
  EXPECT_THROW(aggregates.withScalars({declaredCallback("ai_complete"), scalarCollision}), std::runtime_error);
  EXPECT_EQ(aggregates.aggregateCount(), 1);
  EXPECT_EQ(aggregates.scalarCount(), 0);

  NativeUdfRegistry scalars;
  scalars.registerScalar(declaredCallback("file_from_url"));
  auto aggregateCollision = sum;
  aggregateCollision.name = "FILE_FROM_URL";
  EXPECT_THROW(scalars.registerAggregate(aggregateCollision), std::runtime_error);
  EXPECT_EQ(scalars.aggregateCount(), 0);
  EXPECT_EQ(scalars.scalarCount(), 1);
  EXPECT_EQ(exec::getAggregateFunctionSignatures("sum"), beforeSum);
  EXPECT_EQ(exec::getVectorFunctionSignatures("file_from_url"), beforeFile);
}

TEST(JvmScalarFunctionDeclarationTest, NestedCanonicalTypePayloadCannotCarryIgnoredFields) {
  pangrova::test::detail::ensureRuntimeInitialized();
  for (const auto& name : {"file_from_url", "ai_complete"}) {
    const auto beforeSignatures = exec::getVectorFunctionSignatures(name);
    const auto declared = pangrova::capability::generated::runtimeCallbackSignatures(name);
    for (size_t ordinal = 0; ordinal < declared.size(); ++ordinal) {
      const auto original = declaredDescriptor(name, ordinal);
      for (int typeOrdinal = -1; typeOrdinal < original.argument_types_size(); ++typeOrdinal) {
        const auto typeAt = [typeOrdinal](auto& descriptor) {
          return typeOrdinal < 0 ? descriptor.mutable_return_type()
                                 : descriptor.mutable_argument_types(typeOrdinal);
        };
        auto absent = original;
        typeAt(absent)->clear_canonical_sql();
        EXPECT_THROW(makeJvmScalarFunctionSpec(absent), std::invalid_argument);
        auto unknown = original;
        auto* unknownType = typeAt(unknown);
        unknownType->GetReflection()->MutableUnknownFields(unknownType)->AddVarint(1001, 1);
        EXPECT_THROW(makeJvmScalarFunctionSpec(unknown), std::invalid_argument);
        auto extraPhysical = original;
        typeAt(extraPhysical)->mutable_substrait_type()->mutable_i64();
        EXPECT_THROW(makeJvmScalarFunctionSpec(extraPhysical), std::invalid_argument);
        auto extraConstraint = original;
        typeAt(extraConstraint)->add_constraints()->set_name("unsupported_extra_contract");
        EXPECT_THROW(makeJvmScalarFunctionSpec(extraConstraint), std::invalid_argument);
      }
    }
    EXPECT_EQ(exec::getVectorFunctionSignatures(name), beforeSignatures);
  }
}

TEST(JvmScalarFunctionDeclarationTest, CanonicalCallbackMetadataAndRawSignaturesCannotDrift) {
  pangrova::test::detail::ensureRuntimeInitialized();
  for (const auto& name : {"file_from_url", "ai_complete"}) {
    const auto beforeSignatures = exec::getVectorFunctionSignatures(name);
    const auto beforeMetadata = exec::getVectorFunctionMetadata(name);
    const auto declared = pangrova::capability::generated::runtimeCallbackSignatures(name);
    ASSERT_FALSE(declared.empty());
    for (size_t ordinal = 0; ordinal < declared.size(); ++ordinal) {
      const auto descriptor = declaredDescriptor(name, ordinal);
      auto wrongNullPolicy = descriptor;
      wrongNullPolicy.set_default_null_behavior(!descriptor.default_null_behavior());
      EXPECT_THROW(makeJvmScalarFunctionSpec(wrongNullPolicy), std::invalid_argument);
      auto wrongDeterminism = descriptor;
      wrongDeterminism.set_deterministic(!descriptor.deterministic());
      EXPECT_THROW(makeJvmScalarFunctionSpec(wrongDeterminism), std::invalid_argument);
      auto wrongReturnType = descriptor;
      wrongReturnType.mutable_return_type()->set_canonical_sql("BIGINT");
      EXPECT_THROW(makeJvmScalarFunctionSpec(wrongReturnType), std::invalid_argument);
      auto wrongArguments = descriptor;
      wrongArguments.clear_argument_types();
      EXPECT_THROW(makeJvmScalarFunctionSpec(wrongArguments), std::invalid_argument);
      auto wrongArgumentType = descriptor;
      wrongArgumentType.mutable_argument_types(0)->set_canonical_sql("BIGINT");
      EXPECT_THROW(makeJvmScalarFunctionSpec(wrongArgumentType), std::invalid_argument);
    }
    EXPECT_EQ(exec::getVectorFunctionSignatures(name), beforeSignatures);
    const auto afterMetadata = exec::getVectorFunctionMetadata(name);
    ASSERT_EQ(afterMetadata.has_value(), beforeMetadata.has_value());
    if (beforeMetadata) {
      EXPECT_EQ(afterMetadata->deterministic, beforeMetadata->deterministic);
      EXPECT_EQ(afterMetadata->defaultNullBehavior, beforeMetadata->defaultNullBehavior);
    }
  }
}

TEST(JvmScalarFunctionDeclarationTest, MixedOverloadNullPolicyRejectsTheEntireBatchBeforePublication) {
  pangrova::test::detail::ensureRuntimeInitialized();
  auto first = declaredCallback("ai_complete", 0);
  auto second = declaredCallback("ai_complete", 1);
  NativeUdfRegistry registry;
  const auto beforeSignatures = exec::getVectorFunctionSignatures(first.name);
  const auto beforeMetadata = exec::getVectorFunctionMetadata(first.name);
  auto contradictory = second;
  contradictory.defaultNullBehavior = !first.defaultNullBehavior;
  EXPECT_THROW(registry.withScalars({first, contradictory}), std::runtime_error);
  EXPECT_EQ(registry.scalarCount(), 0);
  const auto candidate = registry.withScalars({first, second});
  EXPECT_EQ(candidate.scalarCount(), 1);
  EXPECT_EQ(registry.scalarCount(), 0);
  EXPECT_EQ(exec::getVectorFunctionSignatures(first.name), beforeSignatures);
  const auto afterMetadata = exec::getVectorFunctionMetadata(first.name);
  ASSERT_EQ(afterMetadata.has_value(), beforeMetadata.has_value());
  if (beforeMetadata) {
    EXPECT_EQ(afterMetadata->deterministic, beforeMetadata->deterministic);
    EXPECT_EQ(afterMetadata->defaultNullBehavior, beforeMetadata->defaultNullBehavior);
  }
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
