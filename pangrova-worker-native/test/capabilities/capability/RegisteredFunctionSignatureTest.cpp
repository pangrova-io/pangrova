#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "capabilities/capability/BuiltinFunctionRegistration.h"
#include "capabilities/capability/RegisteredFunctionSignature.h"
#include "capabilities/capability/FunctionTypeConstraintCompiler.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"

namespace pangrova::test {
namespace {
using namespace facebook::velox;
using namespace pangrova::protocol::v2;

std::shared_ptr<const exec::FunctionRegistrySnapshot> actualSnapshot() {
  pangrova::capability::registerBuiltinFunctions();
  return exec::FunctionRegistrySnapshot::capture();
}

const FunctionTypeSlot& integerSlot(const FunctionTypeTemplate& type, const std::string& fieldName) {
  for (const auto& slot : type.slots()) {
    if (!slot.has_integer() || slot.path_size() != 2) continue;
    const auto* kind = type.skeleton().GetDescriptor()->FindFieldByNumber(slot.path(0).field_number());
    const auto* field = kind->message_type()->FindFieldByNumber(slot.path(1).field_number());
    if (field->name() == fieldName) return slot;
  }
  throw std::runtime_error("Actual canonical signature has no expected integer slot");
}

TEST(RegisteredFunctionSignatureTest, ActualRoundOwnsExactDecimalVariablesAndFormulaAst) {
  const auto owner = actualSnapshot();
  const auto selected = owner->bindScalar("round", {DECIMAL(12, 3)});
  ASSERT_TRUE(selected.has_value());
  EXPECT_TRUE(selected->resultType->equivalent(*DECIMAL(10, 0)));
  capability::RegisteredTypeTemplate types;
  const auto contract = capability::projectRegisteredSignature(*selected->implementation.signature, types);
  ASSERT_EQ(contract.arguments_size(), 1);
  ASSERT_TRUE(contract.arguments(0).skeleton().has_decimal());
  EXPECT_EQ(integerSlot(contract.arguments(0), "precision").integer().operation(), FUNCTION_FORMULA_OPERATION_VARIABLE);
  EXPECT_EQ(integerSlot(contract.arguments(0), "scale").integer().operation(), FUNCTION_FORMULA_OPERATION_VARIABLE);
  ASSERT_TRUE(contract.output().skeleton().has_decimal());
  EXPECT_EQ(contract.constant_arguments_size(), 1);
  EXPECT_FALSE(contract.constant_arguments(0));
  size_t formulas = 0;
  for (const auto& variable : contract.variables()) {
    if (variable.has_formula()) ++formulas;
  }
  EXPECT_GT(formulas, 0);
  EXPECT_FALSE(contract.derived_parameter_order().empty());
}

TEST(RegisteredFunctionSignatureTest, EveryActualOwnedRegistrationProjectsWithoutErasingCustomAst) {
  const auto owner = actualSnapshot();
  capability::RegisteredTypeTemplate types;
  size_t projected = 0;
  for (const auto& [name, entries] : owner->scalars()) {
    for (const auto& entry : entries) {
      if (!entry.signature) continue;  // Actual special forms have a separate structural contract.
      SCOPED_TRACE(name);
      SCOPED_TRACE(entry.signature->toString());
      FunctionSignatureContract contract;
      ASSERT_NO_THROW(contract = capability::projectRegisteredSignature(*entry.signature, types));
      EXPECT_TRUE(contract.has_output());
      EXPECT_EQ(contract.arguments_size(), entry.signature->argumentTypes().size());
      ++projected;
    }
  }
  for (const auto& [name, entry] : owner->aggregates()) {
    for (const auto& signature : entry->signatures) {
      SCOPED_TRACE(name);
      SCOPED_TRACE(signature->toString());
      FunctionSignatureContract contract;
      ASSERT_NO_THROW(contract = capability::projectRegisteredSignature(*signature, types));
      EXPECT_TRUE(contract.has_intermediate());
      ++projected;
    }
  }
  for (const auto& [name, entry] : owner->windows()) {
    for (const auto& signature : entry->signatures) {
      SCOPED_TRACE(name);
      SCOPED_TRACE(signature->toString());
      FunctionSignatureContract contract;
      ASSERT_NO_THROW(contract = capability::projectRegisteredSignature(*signature, types));
      EXPECT_TRUE(contract.has_output());
      ++projected;
    }
  }
  EXPECT_GT(projected, 0);
}

TEST(RegisteredFunctionSignatureTest, ActualMapEntriesKeepsNestedRowChildOrdinalsAndVariables) {
  const auto owner = actualSnapshot();
  const auto selected = owner->bindScalar("map_entries", {MAP(BIGINT(), VARCHAR())});
  ASSERT_TRUE(selected.has_value());
  capability::RegisteredTypeTemplate types;
  const auto contract = capability::projectRegisteredSignature(*selected->implementation.signature, types);
  ASSERT_TRUE(contract.output().skeleton().has_list());
  ASSERT_EQ(contract.output().slots_size(), 1);
  const auto& row = contract.output().slots(0).type();
  ASSERT_TRUE(row.skeleton().has_struct_());
  ASSERT_EQ(row.skeleton().struct_().types_size(), 2);
  ASSERT_EQ(row.slots_size(), 2);
  for (int ordinal = 0; ordinal < row.slots_size(); ++ordinal) {
    const auto& slot = row.slots(ordinal);
    ASSERT_EQ(slot.path_size(), 2);
    EXPECT_FALSE(slot.path(0).has_repeated_index());
    ASSERT_TRUE(slot.path(1).has_repeated_index());
    EXPECT_EQ(slot.path(1).repeated_index(), ordinal);
    EXPECT_TRUE(slot.has_type());
  }
  EXPECT_EQ(row.slots(0).type().type_variable(), "K");
  EXPECT_EQ(row.slots(1).type().type_variable(), "V");
}

TEST(RegisteredFunctionSignatureTest, ActualVarianceIntermediateKeepsAllThreeOrderedChildren) {
  const auto owner = actualSnapshot();
  const auto entry = owner->aggregate("variance");
  ASSERT_TRUE(entry);
  capability::RegisteredTypeTemplate types;
  const exec::AggregateFunctionSignature* actual = nullptr;
  for (const auto& signature : entry->signatures) {
    if (signature->argumentTypes().size() == 1 && signature->argumentTypes()[0].baseName() == "bigint") {
      actual = signature.get();
      break;
    }
  }
  ASSERT_NE(actual, nullptr);
  const auto contract = capability::projectRegisteredSignature(*actual, types);
  ASSERT_TRUE(contract.intermediate().skeleton().has_struct_());
  const auto& state = contract.intermediate();
  ASSERT_EQ(state.skeleton().struct_().types_size(), 3);
  ASSERT_EQ(state.slots_size(), 3);
  for (int ordinal = 0; ordinal < state.slots_size(); ++ordinal) {
    const auto& slot = state.slots(ordinal);
    ASSERT_EQ(slot.path_size(), 2);
    ASSERT_TRUE(slot.path(1).has_repeated_index());
    EXPECT_EQ(slot.path(1).repeated_index(), ordinal);
  }
  EXPECT_TRUE(state.slots(0).type().skeleton().has_i64());
  EXPECT_TRUE(state.slots(1).type().skeleton().has_fp64());
  EXPECT_TRUE(state.slots(2).type().skeleton().has_fp64());
  EXPECT_TRUE(contract.output().skeleton().has_fp64());
}

TEST(RegisteredFunctionSignatureTest, ActualTimestampSignatureHasNoInventedPrecisionParameter) {
  const auto owner = actualSnapshot();
  capability::RegisteredTypeTemplate types;
  size_t timestamps = 0;
  for (const auto& [name, entries] : owner->scalars()) {
    for (const auto& entry : entries) {
      if (!entry.signature) continue;
      for (const auto& argument : entry.signature->argumentTypes()) {
        if (argument.baseName() != "timestamp") continue;
        SCOPED_TRACE(name);
        ASSERT_TRUE(argument.parameters().empty());
        const auto type = types.convert(argument, *entry.signature);
        ASSERT_TRUE(type.skeleton().has_precision_timestamp());
        const auto& precision = integerSlot(type, "precision").integer();
        EXPECT_EQ(precision.operation(), FUNCTION_FORMULA_OPERATION_INTEGER);
        EXPECT_EQ(precision.integer_value(), 6);
        ++timestamps;
      }
    }
  }
  EXPECT_GT(timestamps, 0);
}

TEST(RegisteredFunctionSignatureTest, ActualCustomTypeAndAnyRemainDistinctFromConcreteCanonicalTypes) {
  const auto owner = actualSnapshot();
  const auto selected = owner->bindScalar("round", {DECIMAL(12, 3)});
  ASSERT_TRUE(selected.has_value());
  const auto signature = selected->implementation.signature;
  capability::RegisteredTypeTemplate types;
  const auto unknown = types.convert(exec::TypeSignature("custom_extension", {}), *signature);
  ASSERT_TRUE(unknown.has_opaque_registered_type());
  EXPECT_EQ(unknown.opaque_registered_type().name(), "custom_extension");
  EXPECT_FALSE(unknown.has_skeleton());
  const auto any = types.convert(exec::TypeSignature("any", {}), *signature);
  EXPECT_TRUE(any.has_any_type());
  EXPECT_FALSE(any.has_type_variable());
  EXPECT_FALSE(any.has_skeleton());
}

TEST(RegisteredFunctionSignatureTest, CalculatorGrammarPreservesPrecedenceAndRejectsMalformedInputs) {
  const auto formula = capability::compileRegisteredTypeConstraint("p - s + 1", {"p", "s"});
  ASSERT_EQ(formula.operation(), FUNCTION_FORMULA_OPERATION_ADD);
  ASSERT_EQ(formula.operands(0).operation(), FUNCTION_FORMULA_OPERATION_SUBTRACT);
  EXPECT_EQ(formula.operands(1).integer_value(), 1);
  EXPECT_THROW(capability::compileRegisteredTypeConstraint("p + undeclared", {"p"}), std::invalid_argument);
  EXPECT_THROW(capability::compileRegisteredTypeConstraint("p; + 1", {"p"}), std::invalid_argument);
  EXPECT_THROW(capability::compileRegisteredTypeConstraint("9223372036854775808", {}), std::invalid_argument);
  EXPECT_THROW(capability::compileRegisteredTypeConstraint("max(p,)", {"p"}), std::invalid_argument);
}
}  // namespace
}  // namespace pangrova::test
