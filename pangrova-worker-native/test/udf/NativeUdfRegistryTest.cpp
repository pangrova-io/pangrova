// Tests for NativeUdfRegistry — UDF registration, lookup, and validation.
// Validates: Requirements 7.1, 7.2, 7.7
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "udf/NativeUdfRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace pangrova::udf::test {
namespace {

class TaggedVectorFunction final : public facebook::velox::exec::VectorFunction {
 public:
  explicit TaggedVectorFunction(int tag) : tag_(tag) {}

  int tag() const { return tag_; }

  void apply(
      const facebook::velox::SelectivityVector&,
      std::vector<facebook::velox::VectorPtr>&,
      const TypePtr&,
      facebook::velox::exec::EvalCtx&,
      facebook::velox::VectorPtr&) const override {
    throw std::runtime_error("registration test function is not executable");
  }

 private:
  int tag_;
};

ScalarUdfSpec makeScalarSpec(const std::string& name) {
  ScalarUdfSpec spec;
  spec.name = name;
  spec.inputTypes = {facebook::velox::BIGINT()};
  spec.outputType = facebook::velox::BIGINT();
  spec.stateful = true;
  spec.vectorFunctionFactory =
      [](const std::string&,
         const std::vector<facebook::velox::exec::VectorFunctionArg>&)
          -> std::unique_ptr<facebook::velox::exec::VectorFunction> {
    return nullptr;
  };
  return spec;
}

AggregateUdfSpec makeAggregateSpec(const std::string& name) {
  AggregateUdfSpec spec;
  spec.name = name;
  spec.inputTypes = {facebook::velox::BIGINT()};
  spec.intermediateType = facebook::velox::BIGINT();
  spec.outputType = facebook::velox::BIGINT();

  spec.aggregateFactory =
      [](facebook::velox::core::AggregationNode::Step,
         const std::vector<TypePtr>&,
         const TypePtr&)
          -> std::unique_ptr<facebook::velox::exec::Aggregate> {
    return nullptr;
  };
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

TEST(NativeUdfRegistryTest, RegisterScalarOverloadsUnderOneName) {
  NativeUdfRegistry registry;
  auto bigint = makeScalarSpec("overloaded");
  auto varchar = makeScalarSpec("overloaded");
  varchar.inputTypes = {facebook::velox::VARCHAR()};
  registry.registerScalar(bigint);
  registry.registerScalar(varchar);
  EXPECT_EQ(registry.scalarCount(), 1u);
  EXPECT_TRUE(registry.findScalar("overloaded").has_value());
}

TEST(NativeUdfRegistryTest, DistinctOverloadsUseTheirOwnFactories) {
  NativeUdfRegistry registry;
  auto bigint = makeScalarSpec("pangrova_test_routed_overload_udf");
  bigint.vectorFunctionFactory =
      [](const std::string&,
         const std::vector<facebook::velox::exec::VectorFunctionArg>&) {
        return std::make_unique<TaggedVectorFunction>(1);
      };
  auto varchar = makeScalarSpec(bigint.name);
  varchar.inputTypes = {facebook::velox::VARCHAR()};
  varchar.vectorFunctionFactory =
      [](const std::string&,
         const std::vector<facebook::velox::exec::VectorFunctionArg>&) {
        return std::make_unique<TaggedVectorFunction>(2);
      };
  registry.registerScalar(bigint);
  registry.registerScalar(varchar);
  registry.registerAllWithVelox();

  facebook::velox::core::QueryConfig config({});
  auto bigintFunction = facebook::velox::exec::getVectorFunction(
      bigint.name, {facebook::velox::BIGINT()}, {}, config);
  auto varcharFunction = facebook::velox::exec::getVectorFunction(
      bigint.name, {facebook::velox::VARCHAR()}, {}, config);
  ASSERT_NE(bigintFunction, nullptr);
  ASSERT_NE(varcharFunction, nullptr);
  auto bigintTagged =
      std::dynamic_pointer_cast<TaggedVectorFunction>(bigintFunction);
  auto varcharTagged =
      std::dynamic_pointer_cast<TaggedVectorFunction>(varcharFunction);
  ASSERT_NE(bigintTagged, nullptr);
  ASSERT_NE(varcharTagged, nullptr);
  EXPECT_EQ(bigintTagged->tag(), 1);
  EXPECT_EQ(varcharTagged->tag(), 2);
}

TEST(NativeUdfRegistryTest, DuplicateScalarSignatureThrows) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("duplicate"));
  EXPECT_THROW(
      registry.registerScalar(makeScalarSpec("duplicate")),
      std::runtime_error);
}

TEST(NativeUdfRegistryTest, InvalidRegistrationBatchDoesNotStageItsEarlierItems) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("batch_existing"));
  const auto first = makeScalarSpec("batch_new");
  const auto duplicate = makeScalarSpec("batch_existing");
  EXPECT_THROW(registry.withScalars({first, duplicate}), std::runtime_error);
  EXPECT_EQ(registry.scalarCount(), 1);
  EXPECT_FALSE(registry.findScalar(first.name).has_value());
  EXPECT_TRUE(registry.findScalar(duplicate.name).has_value());
  auto candidate = registry.withScalars({first});
  EXPECT_EQ(candidate.scalarCount(), 2);
  EXPECT_EQ(registry.scalarCount(), 1);
}

TEST(NativeUdfRegistryTest, FinalizedRegistryRejectsBatchBeforeCallingAnyFactory) {
  NativeUdfRegistry registry;
  registry.registerScalar(makeScalarSpec("batch_finalized_existing"));
  registry.registerAllWithVelox();
  auto candidate = makeScalarSpec("batch_finalized_rejected");
  int factoryCalls = 0;
  candidate.stateful = false;
  candidate.vectorFunctionFactory = [&factoryCalls](const auto&, const auto&) {
    ++factoryCalls;
    return std::make_unique<TaggedVectorFunction>(42);
  };
  EXPECT_THROW(registry.withScalars({candidate}), std::runtime_error);
  EXPECT_EQ(factoryCalls, 0);
  EXPECT_EQ(registry.scalarCount(), 1);
  EXPECT_FALSE(facebook::velox::exec::getVectorFunctionSignatures(candidate.name).has_value());
  EXPECT_THROW(registry.registerAllWithVelox(), std::runtime_error);
}

TEST(NativeUdfRegistryTest, EagerFactoryFailureDoesNotPublishAnyPreparedFunction) {
  NativeUdfRegistry registry;
  auto first = makeScalarSpec("batch_eager_first_unpublished");
  first.stateful = false;
  first.vectorFunctionFactory = [](const auto&, const auto&) {
    return std::make_unique<TaggedVectorFunction>(1);
  };
  auto second = makeScalarSpec("batch_eager_failed_unpublished");
  second.stateful = false;
  second.vectorFunctionFactory = [](const auto&, const auto&)
      -> std::unique_ptr<facebook::velox::exec::VectorFunction> {
    throw std::runtime_error("actual eager initialization failure");
  };
  registry.registerScalar(first);
  registry.registerScalar(second);
  EXPECT_THROW(registry.registerAllWithVelox(), std::runtime_error);
  EXPECT_FALSE(facebook::velox::exec::getVectorFunctionSignatures(first.name).has_value());
  EXPECT_FALSE(facebook::velox::exec::getVectorFunctionSignatures(second.name).has_value());
  // Preparation failure must not finalize the registry.
  EXPECT_NO_THROW(registry.registerScalar(makeScalarSpec("batch_after_eager_failure")));
}

TEST(NativeUdfRegistryTest, DuplicateRowSignatureIgnoresFieldNames) {
  NativeUdfRegistry registry;
  auto first = makeScalarSpec("duplicate_row");
  first.inputTypes = {facebook::velox::ROW(
      {"left"}, {facebook::velox::BIGINT()})};
  auto second = makeScalarSpec("duplicate_row");
  second.inputTypes = {facebook::velox::ROW(
      {"right"}, {facebook::velox::BIGINT()})};
  registry.registerScalar(first);
  EXPECT_THROW(registry.registerScalar(second), std::runtime_error);
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

TEST(NativeUdfRegistryTest, RegistersConcreteFileRowAndVectorSignatures) {
  NativeUdfRegistry registry;
  auto fileFunction = makeScalarSpec("pangrova_test_file_row_udf");
  fileFunction.inputTypes = {
      facebook::velox::VARCHAR(),
      facebook::velox::ROW(
          {"uri", "mime_type", "size", "checksum", "created_at"},
          {facebook::velox::VARCHAR(), facebook::velox::VARCHAR(),
           facebook::velox::BIGINT(), facebook::velox::VARCHAR(),
           facebook::velox::TIMESTAMP()})};
  fileFunction.outputType = facebook::velox::VARCHAR();
  registry.registerScalar(fileFunction);

  auto vectorFunction = makeScalarSpec("pangrova_test_vector_return_udf");
  vectorFunction.inputTypes = {
      facebook::velox::VARCHAR(), facebook::velox::VARCHAR()};
  vectorFunction.outputType =
      facebook::velox::ARRAY(facebook::velox::REAL());
  registry.registerScalar(vectorFunction);

  EXPECT_NO_THROW(registry.registerAllWithVelox());
}

}  // namespace
}  // namespace pangrova::udf::test
