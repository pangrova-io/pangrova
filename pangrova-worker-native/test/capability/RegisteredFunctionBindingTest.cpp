#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "../TestMemoryHelper.h"
#include "capability/RegisteredFunctionBinding.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace pangrova::capability::test {
namespace {
using namespace facebook::velox;

core::TypedExprPtr field(const TypePtr& type) {
  return std::make_shared<core::FieldAccessTypedExpr>(type, "input");
}

TEST(RegisteredFunctionBindingTest, EqualityBindsExactPhysicalKeyTypes) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  const std::vector<TypePtr> keys = {BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT(),
      REAL(), DOUBLE(), VARCHAR(), VARBINARY(), DATE(), TIMESTAMP(), DECIMAL(15, 2)};
  for (const auto& type : keys) {
    SCOPED_TRACE(type->toString());
    EXPECT_NO_THROW(bindRegisteredScalar(*registry, "eq", BOOLEAN(), {field(type), field(type)}));
  }
  EXPECT_THROW(bindRegisteredScalar(*registry, "eq", BOOLEAN(), {field(BIGINT()), field(VARCHAR())}),
      std::runtime_error);
}

TEST(RegisteredFunctionBindingTest, RoundUsesActualDecimalFormulaWithoutCoercion) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto unary = bindRegisteredScalar(*registry, "round", DECIMAL(10, 0), {field(DECIMAL(12, 3))});
  EXPECT_TRUE(unary->type()->equivalent(*DECIMAL(10, 0)));
  auto scale = std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(int32_t{2}));
  auto binary = bindRegisteredScalar(*registry, "round", DECIMAL(13, 3), {field(DECIMAL(12, 3)), scale});
  EXPECT_TRUE(binary->type()->equivalent(*DECIMAL(13, 3)));
  EXPECT_THROW(bindRegisteredScalar(*registry, "round", DECIMAL(12, 3), {field(DECIMAL(12, 3))}), std::runtime_error);
  EXPECT_THROW(bindRegisteredScalar(*registry, "round", DECIMAL(13, 3),
      {field(DECIMAL(12, 3)), field(BIGINT())}), std::runtime_error);
}

TEST(RegisteredFunctionBindingTest, RawAggregateChecksResultAndIntermediateSeparately) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto single = bindRegisteredRawAggregate(*registry, "sum", DECIMAL(38, 7), {field(DECIMAL(22, 7))}, false);
  EXPECT_TRUE(single.call->type()->equivalent(*DECIMAL(38, 7)));
  EXPECT_TRUE(single.intermediate->equivalent(*VARBINARY()));
  auto partial = bindRegisteredRawAggregate(*registry, "sum", VARBINARY(), {field(DECIMAL(22, 7))}, true);
  EXPECT_TRUE(partial.call->type()->equivalent(*VARBINARY()));
  EXPECT_THROW(bindRegisteredRawAggregate(*registry, "sum", DECIMAL(38, 0),
      {field(DECIMAL(22, 7))}, false), std::runtime_error);
  EXPECT_THROW(bindRegisteredRawAggregate(*registry, "sum", DECIMAL(38, 7),
      {field(DECIMAL(22, 7))}, true), std::runtime_error);
  EXPECT_THROW(bindRegisteredRawAggregate(*registry, "sum", BIGINT(), {field(VARBINARY())}, false), std::runtime_error);
}

TEST(RegisteredFunctionBindingTest, WindowPreservesExactInputsAndReturnType) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registry = exec::FunctionRegistrySnapshot::capture();
  auto lead = bindRegisteredWindow(*registry, "lead", DECIMAL(25, 9),
      {field(DECIMAL(25, 9)), field(BIGINT()), field(DECIMAL(25, 9))});
  EXPECT_TRUE(lead->type()->equivalent(*DECIMAL(25, 9)));
  EXPECT_THROW(bindRegisteredWindow(*registry, "lead", DECIMAL(25, 9),
      {field(DECIMAL(25, 9)), field(INTEGER())}), std::runtime_error);
  EXPECT_THROW(bindRegisteredWindow(*registry, "lead", DECIMAL(25, 9),
      {field(DECIMAL(25, 9)), field(BIGINT()), field(DOUBLE())}), std::runtime_error);
  EXPECT_THROW(bindRegisteredWindow(*registry, "row_number", INTEGER(), {}), std::runtime_error);
  EXPECT_THROW(bindRegisteredWindow(*registry, "not_registered_window", BIGINT(), {}), std::runtime_error);
}
}  // namespace
}  // namespace pangrova::capability::test
