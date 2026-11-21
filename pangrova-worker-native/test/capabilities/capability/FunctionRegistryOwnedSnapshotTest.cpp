#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TestMemoryHelper.h"
#include "capabilities/capability/BuiltinFunctionRegistration.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateCompanionSignatures.h"
#include "velox/exec/ColumnStatsCollector.h"
#include "velox/exec/Task.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/ExprConstants.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace pangrova::capability::test {
namespace {
using namespace facebook::velox;
using Snapshot = exec::FunctionRegistrySnapshot;

// A real registered vector function used to distinguish selected factories.
// These tests execute SDK Tasks, not planning-time factory availability probes.
class SnapshotMarker final : public exec::VectorFunction {
 public:
  explicit SnapshotMarker(std::shared_ptr<int64_t> marker) : marker_(std::move(marker)) {}
  void apply(const SelectivityVector& rows, std::vector<VectorPtr>&,
      const TypePtr& type, exec::EvalCtx& context, VectorPtr& result) const override {
    auto value = BaseVector::create(type, rows.end(), context.pool());
    auto* output = value->asFlatVector<int64_t>();
    rows.applyToSelected([&](vector_size_t row) { output->set(row, *marker_); });
    context.moveOrCopyResult(value, rows, result);
  }
 private:
  const std::shared_ptr<int64_t> marker_;
};

std::vector<exec::FunctionSignaturePtr> scalarSignatures() {
  return {exec::FunctionSignatureBuilder().argumentType("bigint")
      .argumentType("bigint").returnType("bigint").build()};
}

void registerMarker(const std::string& name, std::shared_ptr<int64_t> marker, bool deterministic,
    std::shared_ptr<const exec::FunctionRegistrationOwner> registrationOwner = nullptr) {
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name, scalarSignatures(),
      [marker = std::move(marker)](const auto&, const auto&, const auto&) {
        return std::make_shared<SnapshotMarker>(marker);
      }, exec::VectorFunctionMetadataBuilder().deterministic(deterministic).build(),
      true, std::move(registrationOwner)));
}

core::PlanNodePtr values() {
  const auto pool = pangrova::test::testLeafPool("owned-registry-values");
  auto vector = BaseVector::create(ROW({"x", "y"}, {BIGINT(), BIGINT()}), 3, pool.get());
  auto rows = std::dynamic_pointer_cast<RowVector>(vector);
  for (vector_size_t row = 0; row < 3; ++row) {
    rows->childAt(0)->asFlatVector<int64_t>()->set(row, row + 1);
    rows->childAt(1)->asFlatVector<int64_t>()->set(row, 10);
  }
  return std::make_shared<core::ValuesNode>("values", std::vector<RowVectorPtr>{rows});
}

core::CallTypedExprPtr scalarCall(const std::string& name) {
  return std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "y")}, name);
}

core::PlanNodePtr scalarProject(const std::string& name) {
  return std::make_shared<core::ProjectNode>("project", std::vector<std::string>{"result"},
      std::vector<core::TypedExprPtr>{scalarCall(name)}, values());
}

std::vector<int64_t> execute(const core::PlanNodePtr& plan,
    std::shared_ptr<const Snapshot> owner, int ordinal = 0) {
  static std::atomic<uint64_t> sequence{0};
  auto config = core::QueryConfig(
      std::unordered_map<std::string, std::string>{{"require_function_registry_snapshot", "true"}},
      std::move(owner));
  auto query = core::QueryCtx::create(pangrova::jni::globalExecutor().get(), std::move(config));
  auto task = exec::Task::create("owned-registry-task-" + std::to_string(sequence.fetch_add(1)),
      core::PlanFragment{plan}, 0, query, exec::Task::ExecutionMode::kSerial);
  std::vector<int64_t> output;
  while (auto batch = task->next()) {
    DecodedVector decoded(*batch->childAt(ordinal));
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      EXPECT_FALSE(decoded.isNullAt(row));
      output.push_back(decoded.valueAt<int64_t>(row));
    }
  }
  VELOX_CHECK(!task->error(), "{}", task->errorMessage());
  EXPECT_EQ(task->state(), exec::TaskState::kFinished);
  return output;
}

TEST(FunctionRegistryOwnedSnapshotTest, VectorBeforeSimpleSelectionExecutesTheSelectedEntry) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_vector_before_simple";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  registerMarker(name, std::make_shared<int64_t>(71), false);
  const auto owner = Snapshot::capture();
  const auto bound = owner->bindScalar(name, {BIGINT(), BIGINT()});
  ASSERT_TRUE(bound.has_value());
  EXPECT_EQ(bound->implementation.family(), 2);
  EXPECT_FALSE(bound->implementation.vector->metadata.deterministic);
  EXPECT_EQ(execute(scalarProject(name), owner), (std::vector<int64_t>{71, 71, 71}));
}

TEST(FunctionRegistryOwnedSnapshotTest, OverwriteCannotChangeAnOwnedVectorFactoryOrMetadata) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_vector_overwrite";
  auto marker = std::make_shared<int64_t>(19);
  std::weak_ptr<int64_t> oldFactoryOwner = marker;
  registerMarker(name, marker, false);
  marker.reset();
  auto old = Snapshot::capture();
  registerMarker(name, std::make_shared<int64_t>(97), true);
  const auto current = Snapshot::capture();
  ASSERT_TRUE(old->bindScalar(name, {BIGINT(), BIGINT()}).has_value());
  EXPECT_FALSE(old->bindScalar(name, {BIGINT(), BIGINT()})->implementation.vector->metadata.deterministic);
  EXPECT_TRUE(current->bindScalar(name, {BIGINT(), BIGINT()})->implementation.vector->metadata.deterministic);
  EXPECT_FALSE(oldFactoryOwner.expired());
  EXPECT_EQ(execute(scalarProject(name), old), (std::vector<int64_t>{19, 19, 19}));
  EXPECT_EQ(execute(scalarProject(name), current), (std::vector<int64_t>{97, 97, 97}));
  old.reset();
  EXPECT_TRUE(oldFactoryOwner.expired());
}

TEST(FunctionRegistryOwnedSnapshotTest, RemovingLiveSimpleEntryRetainsActualFactoryAndSignature) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_removed_simple";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  const auto owner = Snapshot::capture();
  exec::mutableSimpleFunctions().removeFunction(name);
  const auto bound = owner->bindScalar(name, {BIGINT(), BIGINT()});
  ASSERT_TRUE(bound.has_value());
  EXPECT_EQ(bound->implementation.family(), 3);
  EXPECT_EQ(bound->implementation.signature->argumentTypes().size(), 2);
  EXPECT_TRUE(bound->implementation.simple->getMetadata().isDeterministic());
  EXPECT_FALSE(Snapshot::capture()->bindScalar(name, {BIGINT(), BIGINT()}).has_value());
  EXPECT_EQ(execute(scalarProject(name), owner), (std::vector<int64_t>{11, 12, 13}));
}

TEST(FunctionRegistryOwnedSnapshotTest, OverwritingLiveSimpleEntryKeepsTheAdmittedValueAndFactoryOwner) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_overwritten_simple";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  auto old = Snapshot::capture();
  std::weak_ptr<const exec::FunctionEntry> originalFactory;
  {
    const auto original = old->bindScalar(name, {BIGINT(), BIGINT()});
    ASSERT_TRUE(original.has_value());
    ASSERT_EQ(original->implementation.family(), 3);
    originalFactory = original->implementation.simple;
  }
  ASSERT_TRUE((facebook::velox::registerFunction<functions::MinusFunction,
      int64_t, int64_t, int64_t>({name})));
  const auto current = Snapshot::capture();
  EXPECT_EQ(execute(scalarProject(name), old), (std::vector<int64_t>{11, 12, 13}));
  EXPECT_EQ(execute(scalarProject(name), current), (std::vector<int64_t>{-9, -8, -7}));
  EXPECT_FALSE(originalFactory.expired());
  old.reset();
  EXPECT_TRUE(originalFactory.expired());
}

TEST(FunctionRegistryOwnedSnapshotTest, ClearingLiveSimpleRegistryKeepsAdmittedValuesAndActualSignature) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_cleared_simple";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  const auto owner = Snapshot::capture();
  exec::mutableSimpleFunctions().clearRegistry();
  EXPECT_FALSE(Snapshot::capture()->bindScalar(name, {BIGINT(), BIGINT()}).has_value());
  const auto admitted = owner->bindScalar(name, {BIGINT(), BIGINT()});
  ASSERT_TRUE(admitted.has_value());
  EXPECT_EQ(admitted->implementation.family(), 3);
  EXPECT_EQ(admitted->implementation.signature->argumentTypes().size(), 2);
  EXPECT_EQ(execute(scalarProject(name), owner), (std::vector<int64_t>{11, 12, 13}));
  pangrova::capability::registerBuiltinFunctions();
}

TEST(FunctionRegistryOwnedSnapshotTest, FailedSelectedVectorInitializationNeverFallsBackToSimple) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_no_initialization_fallback";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name, scalarSignatures(),
      [](const auto&, const auto&, const auto&) -> std::shared_ptr<exec::VectorFunction> {
        throw std::runtime_error("selected actual vector initialization failed");
      }));
  const auto owner = Snapshot::capture();
  EXPECT_EQ(owner->bindScalar(name, {BIGINT(), BIGINT()})->implementation.family(), 2);
  EXPECT_THROW(execute(scalarProject(name), owner), std::exception);
}

TEST(FunctionRegistryOwnedSnapshotTest, EngagedNullVectorFactoryDoesNotSelectSimple) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_null_factory";
  ASSERT_TRUE((facebook::velox::registerFunction<functions::PlusFunction,
      int64_t, int64_t, int64_t>({name})));
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name, scalarSignatures(),
      [](const auto&, const auto&, const auto&) -> std::shared_ptr<exec::VectorFunction> { return nullptr; }));
  const auto owner = Snapshot::capture();
  EXPECT_EQ(owner->bindScalar(name, {BIGINT(), BIGINT()})->implementation.family(), 2);
  EXPECT_THROW(execute(scalarProject(name), owner), std::exception);
}

TEST(FunctionRegistryOwnedSnapshotTest, RequiredTaskOwnerCannotFallThroughToStockRegistry) {
  pangrova::test::detail::ensureRuntimeInitialized();
  EXPECT_THROW(execute(scalarProject("plus"), nullptr), std::exception);
}

TEST(FunctionRegistryOwnedSnapshotTest, AggregateExecutionRetainsTheAdmittedFactoryAndMetadata) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_aggregate";
  const auto base = Snapshot::capture()->aggregate("sum");
  ASSERT_NE(base, nullptr);
  exec::registerAggregateFunction(name, base->signatures, base->factory, base->metadata,
      /*registerCompanionFunctions=*/false, /*overwrite=*/true);
  const auto old = Snapshot::capture();
  auto modified = base->metadata;
  modified.companionFunction = !modified.companionFunction;
  exec::registerAggregateFunction(name, base->signatures,
      [](const auto&, const auto&, const auto&, const auto&) -> std::unique_ptr<exec::Aggregate> {
        throw std::runtime_error("new aggregate factory must not run for admitted owner");
      }, modified, /*registerCompanionFunctions=*/false, /*overwrite=*/true);
  ASSERT_NE(old->aggregate(name), nullptr);
  EXPECT_EQ(old->aggregate(name)->metadata.companionFunction, base->metadata.companionFunction);
  EXPECT_NE(Snapshot::capture()->aggregate(name)->metadata.companionFunction,
      old->aggregate(name)->metadata.companionFunction);
  const auto field = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x");
  const auto call = std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{field}, name);
  const core::AggregationNode::Aggregate aggregate{call, {BIGINT()}};
  const auto plan = std::make_shared<core::AggregationNode>("aggregate",
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"result"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, values());
  EXPECT_EQ(execute(plan, old), (std::vector<int64_t>{6}));
  EXPECT_THROW(execute(plan, Snapshot::capture()), std::exception);
}

TEST(FunctionRegistryOwnedSnapshotTest, WindowProcessMetadataAndFactoryUseTheSameOwner) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_snapshot_window";
  const auto base = Snapshot::capture()->window("row_number");
  ASSERT_NE(base, nullptr);
  exec::registerWindowFunction(name, base->signatures, base->metadata, base->factory);
  const auto old = Snapshot::capture();
  const exec::WindowFunction::Metadata modified{
      base->metadata.processMode == exec::WindowFunction::ProcessMode::kRows
          ? exec::WindowFunction::ProcessMode::kPartition : exec::WindowFunction::ProcessMode::kRows,
      !base->metadata.isAggregate};
  exec::registerWindowFunction(name, base->signatures, modified,
      [](const auto&, const auto&, bool, auto*, auto*, const auto&)
          -> std::unique_ptr<exec::WindowFunction> {
        throw std::runtime_error("new window factory must not run for admitted owner");
      });
  const auto config = core::QueryConfig(std::unordered_map<std::string, std::string>{}, old);
  EXPECT_EQ(exec::getWindowFunctionMetadata(name, config).processMode, base->metadata.processMode);
  EXPECT_EQ(exec::getWindowFunctionMetadata(name, config).isAggregate, base->metadata.isAggregate);
  EXPECT_NE(exec::getWindowFunctionMetadata(name).processMode, base->metadata.processMode);
  const auto call = std::make_shared<core::CallTypedExpr>(BIGINT(), std::vector<core::TypedExprPtr>{}, name);
  const core::WindowNode::Frame frame{core::WindowNode::WindowType::kRows,
      core::WindowNode::BoundType::kUnboundedPreceding, nullptr,
      core::WindowNode::BoundType::kCurrentRow, nullptr};
  const core::WindowNode::Function function{call, frame, false};
  const auto plan = std::make_shared<core::WindowNode>("window",
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<core::FieldAccessTypedExprPtr>{std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x")},
      std::vector<core::SortOrder>{core::SortOrder(true, true)},
      std::vector<std::string>{"result"}, std::vector<core::WindowNode::Function>{function}, false, values());
  EXPECT_EQ(execute(plan, old, 2), (std::vector<int64_t>{1, 2, 3}));
  EXPECT_THROW(execute(plan, Snapshot::capture(), 2), std::exception);
}

TEST(FunctionRegistryOwnedSnapshotTest, UnknownOrUnsupportedSignatureProducesNoInventedEntry) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto owner = Snapshot::capture();
  EXPECT_FALSE(owner->bindScalar("owned_snapshot_unknown", {BIGINT()}).has_value());
  EXPECT_FALSE(owner->bindScalar("round", {MAP(BIGINT(), BIGINT())}).has_value());
  const auto decimal = owner->bindScalar("round", {DECIMAL(12, 3)});
  ASSERT_TRUE(decimal.has_value());
  EXPECT_TRUE(decimal->resultType->equivalent(*DECIMAL(10, 0)));
}

TEST(FunctionRegistryOwnedSnapshotTest, SignatureLessRowCompanionRetainsActualFactoryAndMetadata) {
  pangrova::test::detail::ensureRuntimeInitialized();
  auto owner = Snapshot::capture();
  const auto companion = owner->vectorCompanion(expression::kRowConstructor);
  ASSERT_NE(companion, nullptr);
  EXPECT_TRUE(companion->signatures.empty());
  const auto type = ROW({"first", "second"}, {BIGINT(), BIGINT()});
  const auto row = std::make_shared<core::CallTypedExpr>(type,
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "y")}, "row_constructor");
  const auto first = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), row, "first");
  const auto plan = std::make_shared<core::ProjectNode>("owned-row",
      std::vector<std::string>{"first"}, std::vector<core::TypedExprPtr>{first}, values());
  ASSERT_TRUE(exec::registerStatefulVectorFunction(expression::kRowConstructor, {},
      [](const auto&, const auto&, const auto&) -> std::shared_ptr<exec::VectorFunction> {
        throw std::runtime_error("new ROW companion must not run for admitted owner");
      }, exec::VectorFunctionMetadataBuilder().deterministic(false).build()));
  EXPECT_EQ(owner->vectorCompanion(expression::kRowConstructor), companion);
  EXPECT_EQ(execute(plan, owner), (std::vector<int64_t>{1, 2, 3}));
  EXPECT_THROW(execute(plan, Snapshot::capture()), std::exception);
  ASSERT_TRUE(exec::registerStatefulVectorFunction(expression::kRowConstructor,
      companion->signatures, companion->factory, companion->metadata));
}

TEST(FunctionRegistryOwnedSnapshotTest, SpecialBindingUsesActualNamedRowEqualityAndDeclaredCastProgramme) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto owner = Snapshot::capture();
  const auto left = ROW({"left"}, {BIGINT()});
  const auto right = ROW({"right"}, {BIGINT()});
  ASSERT_TRUE(left->equivalent(*right));
  ASSERT_FALSE(*left == *right);
  EXPECT_THROW(owner->bindScalar("coalesce", {left, right}), std::exception);
  ASSERT_TRUE(owner->bindScalar("coalesce", {left, left}).has_value());
  const auto cast = owner->bindScalar("cast", {BIGINT()}, DOUBLE());
  ASSERT_TRUE(cast.has_value());
  EXPECT_TRUE(cast->resultType->equivalent(*DOUBLE()));
  EXPECT_THROW(owner->bindScalar("cast", {BIGINT()}, VARBINARY()), std::exception);
  EXPECT_THROW(owner->bindScalar("cast", {BIGINT()}), std::exception);
  ASSERT_TRUE(owner->bindScalar("try_cast", {BIGINT()}, VARBINARY()).has_value());
}

TEST(FunctionRegistryOwnedSnapshotTest, ExactRetirementKeepsAdmittedValuesAndDoesNotRemoveLaterRegistration) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const auto registration = std::make_shared<const exec::FunctionRegistrationOwner>();
  const std::string retiredName = "owned_runtime_retired_vector";
  const std::string overwrittenName = "owned_runtime_overwritten_vector";
  registerMarker(retiredName, std::make_shared<int64_t>(29), false, registration);
  registerMarker(overwrittenName, std::make_shared<int64_t>(31), false, registration);
  const auto admitted = Snapshot::capture();
  registerMarker(overwrittenName, std::make_shared<int64_t>(37), true);
  exec::unregisterOwnedFunctionEntries(registration);
  const auto current = Snapshot::capture();
  EXPECT_FALSE(current->bindScalar(retiredName, {BIGINT(), BIGINT()}).has_value());
  ASSERT_TRUE(current->bindScalar(overwrittenName, {BIGINT(), BIGINT()}).has_value());
  EXPECT_EQ(execute(scalarProject(retiredName), admitted), (std::vector<int64_t>{29, 29, 29}));
  EXPECT_EQ(execute(scalarProject(overwrittenName), admitted), (std::vector<int64_t>{31, 31, 31}));
  EXPECT_EQ(execute(scalarProject(overwrittenName), current), (std::vector<int64_t>{37, 37, 37}));
}

TEST(FunctionRegistryOwnedSnapshotTest, ActualAggregateCompanionsNeverRereadTheLiveOriginalFactory) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_companion_sum";
  const auto registration = std::make_shared<const exec::FunctionRegistrationOwner>();
  const auto base = Snapshot::capture()->aggregate("sum");
  ASSERT_NE(base, nullptr);
  exec::registerAggregateFunction(name, base->signatures, base->factory, base->metadata,
      /*registerCompanionFunctions=*/true, /*overwrite=*/true, registration);
  const auto admitted = Snapshot::capture();
  const auto partial = exec::CompanionSignatures::partialFunctionName(name);
  const exec::AggregateFunctionSignature* selected = nullptr;
  const std::vector<TypePtr> rawInputTypes{BIGINT()};
  for (const auto& signature : base->signatures) {
    exec::SignatureBinder binder(*signature, rawInputTypes);
    if (binder.tryBind()) { selected = signature.get(); break; }
  }
  ASSERT_NE(selected, nullptr);
  ASSERT_TRUE(exec::CompanionSignatures::hasSameIntermediateTypesAcrossSignatures(base->signatures));
  const auto extract = exec::CompanionSignatures::extractFunctionNameWithSuffix(name, selected->returnType());
  EXPECT_FALSE(admitted->bindScalar(exec::CompanionSignatures::extractFunctionName(name), {BIGINT()}).has_value());
  ASSERT_NE(admitted->aggregate(partial), nullptr);
  ASSERT_TRUE(admitted->bindScalar(extract, {BIGINT()}).has_value());
  EXPECT_EQ(admitted->aggregate(partial)->registrationOwner, registration);
  EXPECT_EQ(admitted->window(name)->registrationOwner, registration);
  exec::registerAggregateFunction(name, base->signatures,
      [](const auto&, const auto&, const auto&, const auto&) -> std::unique_ptr<exec::Aggregate> {
        throw std::runtime_error("live original must not run for admitted companion");
      }, base->metadata, false, true);
  const auto field = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x");
  const auto call = std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{field}, partial);
  const core::AggregationNode::Aggregate aggregate{call, {BIGINT()}};
  const auto input = values();
  const auto plan = std::make_shared<core::AggregationNode>("companion-partial",
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{}, std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"result"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, input);
  EXPECT_EQ(execute(plan, admitted), (std::vector<int64_t>{6}));
  const auto extractCall = std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{field}, extract);
  const auto extractPlan = std::make_shared<core::ProjectNode>("companion-extract",
      std::vector<std::string>{"result"}, std::vector<core::TypedExprPtr>{extractCall}, input);
  EXPECT_EQ(execute(extractPlan, admitted), (std::vector<int64_t>{1, 2, 3}));
  EXPECT_THROW(execute(extractPlan, Snapshot::capture()), std::exception);
  exec::unregisterOwnedFunctionEntries(registration);
  EXPECT_EQ(Snapshot::capture()->aggregate(partial), nullptr);
  EXPECT_FALSE(Snapshot::capture()->bindScalar(extract, {BIGINT()}).has_value());
  EXPECT_EQ(execute(plan, admitted), (std::vector<int64_t>{6}));
}

class AggregateMetadataOwnedSnapshotTest : public ::testing::TestWithParam<bool> {};

TEST_P(AggregateMetadataOwnedSnapshotTest, DistinctExecutionUsesTheAdmittedMetadataAfterOverwrite) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = GetParam() ? "owned_distinct_streaming_sum" : "owned_distinct_hash_sum";
  const auto base = Snapshot::capture()->aggregate("sum");
  ASSERT_NE(base, nullptr);
  ASSERT_FALSE(base->metadata.ignoreDuplicates);
  const auto registration = std::make_shared<const exec::FunctionRegistrationOwner>();
  exec::registerAggregateFunction(name, base->signatures, base->factory, base->metadata,
      false, true, registration);
  const auto admitted = Snapshot::capture();
  auto changed = base->metadata;
  changed.ignoreDuplicates = true;
  exec::registerAggregateFunction(name, base->signatures, base->factory, changed, false, true);
  const auto current = Snapshot::capture();
  const auto field = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "y");
  const auto call = std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{field}, name);
  core::AggregationNode::Aggregate aggregate{call, {BIGINT()}};
  aggregate.distinct = true;
  const std::vector<core::FieldAccessTypedExprPtr> keys{field};
  const auto plan = std::make_shared<core::AggregationNode>("owned-distinct",
      core::AggregationNode::Step::kSingle, keys,
      GetParam() ? keys : std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"result"}, std::vector<core::AggregationNode::Aggregate>{aggregate},
      false, false, values());
  EXPECT_EQ(execute(plan, admitted, 1), (std::vector<int64_t>{10}));
  EXPECT_EQ(execute(plan, current, 1), (std::vector<int64_t>{30}));
}

INSTANTIATE_TEST_SUITE_P(HashAndStreaming, AggregateMetadataOwnedSnapshotTest, ::testing::Bool());

TEST(FunctionRegistryOwnedSnapshotTest, ColumnStatsExecutionRetainsRetiredAggregateAndIntermediateType) {
  pangrova::test::detail::ensureRuntimeInitialized();
  const std::string name = "owned_column_stats_retired_sum";
  const auto base = Snapshot::capture()->aggregate("sum");
  ASSERT_NE(base, nullptr);
  const auto registration = std::make_shared<const exec::FunctionRegistrationOwner>();
  exec::registerAggregateFunction(name, base->signatures, base->factory, base->metadata,
      false, true, registration);
  const auto admitted = Snapshot::capture();
  exec::unregisterOwnedFunctionEntries(registration);
  ASSERT_EQ(Snapshot::capture()->aggregate(name), nullptr);
  const auto field = std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "x");
  const auto call = std::make_shared<core::CallTypedExpr>(BIGINT(),
      std::vector<core::TypedExprPtr>{field}, name);
  const core::ColumnStatsSpec stats{{}, core::AggregationNode::Step::kSingle,
      {"result"}, {{call, {BIGINT()}}}};
  const auto input = std::dynamic_pointer_cast<const core::ValuesNode>(values());
  ASSERT_NE(input, nullptr);
  core::QueryConfig config(
      std::unordered_map<std::string, std::string>{{"require_function_registry_snapshot", "true"}}, admitted);
  const auto query = core::QueryCtx::create(pangrova::jni::globalExecutor().get(), std::move(config));
  const auto pool = query->pool()->addLeafChild("owned-column-stats");
  ASSERT_TRUE(pool->trackUsage());
  tsan_atomic<bool> nonReclaimable{false};
  exec::ColumnStatsCollector collector(stats, input->outputType(), &query->queryConfig(), pool.get(), &nonReclaimable);
  collector.initialize();
  for (const auto& batch : input->values()) {
    collector.addInput(batch);
  }
  collector.noMoreInput();
  auto output = collector.getOutput();
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->size(), 1);
  DecodedVector decoded(*output->childAt(0));
  ASSERT_FALSE(decoded.isNullAt(0));
  EXPECT_EQ(decoded.valueAt<int64_t>(0), 6);
  EXPECT_EQ(collector.getOutput(), nullptr);
  EXPECT_TRUE(collector.finished());
  collector.close();
  const core::QueryConfig missing(
      std::unordered_map<std::string, std::string>{{"require_function_registry_snapshot", "true"}});
  exec::ColumnStatsCollector rejected(stats, input->outputType(), &missing, pool.get(), &nonReclaimable);
  try {
    rejected.initialize();
    ADD_FAILURE() << "Column stats accepted a missing required registry owner";
  } catch (const std::exception& error) {
    EXPECT_NE(std::string(error.what()).find("A required owned function registry snapshot was not supplied"),
        std::string::npos) << error.what();
  }
}

}  // namespace
}  // namespace pangrova::capability::test
