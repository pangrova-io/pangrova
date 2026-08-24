// Tests for NativeUdfRegistry — UDF registration, lookup, and validation.
// Validates: Requirements 7.1, 7.2, 7.7
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "udf/NativeUdfRegistry.h"

namespace pangrova::udf::test {
namespace {

#if PANGROVA_HAS_VELOX_TYPE

ScalarUdfSpec makeScalarSpec(const std::string& name) {
  ScalarUdfSpec spec;
  spec.name = name;
  spec.inputTypes = {facebook::velox::BIGINT()};
  spec.outputType = facebook::velox::BIGINT();
  spec.stateful = true;
#if PANGROVA_HAS_VELOX_VECTOR_FUNC
  spec.vectorFunctionFactory =
      [](const std::string&,
         const std::vector<facebook::velox::exec::VectorFunctionArg>&)
          -> std::unique_ptr<facebook::velox::exec::VectorFunction> {
    return nullptr;
  };
#endif
  return spec;
}

AggregateUdfSpec makeAggregateSpec(const std::string& name) {
  AggregateUdfSpec spec;
  spec.name = name;
  spec.inputTypes = {facebook::velox::BIGINT()};
  spec.intermediateType = facebook::velox::BIGINT();
  spec.outputType = facebook::velox::BIGINT();

#if PANGROVA_HAS_VELOX_AGGREGATE
  spec.aggregateFactory =
      [](facebook::velox::core::AggregationNode::Step,
         const std::vector<TypePtr>&,
         const TypePtr&)
          -> std::unique_ptr<facebook::velox::exec::Aggregate> {
    return nullptr;
  };
#endif
  return spec;
}

TEST(NativeUdfRegistryTest, RegisterScalarUdf) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("my_add"));
  EXPECT_EQ(registry.scalarCount(), 1u);
  auto found = registry.findScalar("my_add");
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->name, "my_add");
}

TEST(NativeUdfRegistryTest, RegisterMultipleScalarUdfs) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("udf_a"));
  registry.registerScalar(makeScalarSpec("udf_b"));
  registry.registerScalar(makeScalarSpec("udf_c"));
  EXPECT_EQ(registry.scalarCount(), 3u);
}

TEST(NativeUdfRegistryTest, FindScalarNotRegisteredReturnsEmpty) {
  NativeUdfRegistry registry;
  EXPECT_FALSE(registry.findScalar("nonexistent").has_value());
}

TEST(NativeUdfRegistryTest, RegisterScalarEmptyNameThrows) {
  NativeUdfRegistry registry;
  EXPECT_THROW(registry.registerScalar(makeScalarSpec("")),
               std::invalid_argument);
}

TEST(NativeUdfRegistryTest, RegisterAggregateUdf) {
  NativeUdfRegistry registry;
  registry.registerAggregate(makeAggregateSpec("my_sum"));
  EXPECT_EQ(registry.aggregateCount(), 1u);
  auto found = registry.findAggregate("my_sum");
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->name, "my_sum");
}

TEST(NativeUdfRegistryTest, FindAggregateNotRegisteredReturnsEmpty) {
  NativeUdfRegistry registry;
  EXPECT_FALSE(registry.findAggregate("nonexistent").has_value());
}

TEST(NativeUdfRegistryTest, RegisterAggregateEmptyNameThrows) {
  NativeUdfRegistry registry;
  EXPECT_THROW(registry.registerAggregate(makeAggregateSpec("")),
               std::invalid_argument);
}

TEST(NativeUdfRegistryTest, CountsAreIndependent) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("s1"));
  registry.registerScalar(makeScalarSpec("s2"));
  registry.registerAggregate(makeAggregateSpec("a1"));
  EXPECT_EQ(registry.scalarCount(), 2u);
  EXPECT_EQ(registry.aggregateCount(), 1u);
}

#else
TEST(NativeUdfRegistryTest, SkippedWithoutVeloxTypes) {
  GTEST_SKIP() << "Velox type headers not available";
}
#endif

}  // namespace
}  // namespace pangrova::udf::test
