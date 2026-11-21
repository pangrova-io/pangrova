#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "TestMemoryHelper.h"
#include "capabilities/capability/BuiltinFunctionRegistration.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionRegistryMutation.h"
#include "velox/expression/FunctionRegistrySnapshot.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"

namespace pangrova::capability::test {
namespace {
using namespace facebook::velox;

struct RetirementRecord {
  bool destroyed{false};
  bool mutationUnlocked{false};
  std::optional<bool> mapUnlocked;
  std::thread::id destructorThread;
};

// This is owned by an actual registered factory, never by a capability stub.
// Cross-thread try-locks detect both recursive mutation and map lock leaks.
class RetirementObserver final {
 public:
  RetirementObserver(RetirementRecord& record, std::function<bool()> mapProbe)
      : record_(record), mapProbe_(std::move(mapProbe)) {}
  ~RetirementObserver() {
    record_.destructorThread = std::this_thread::get_id();
    std::thread probe([&] {
      auto& mutation = exec::functionRegistryMutationMutex();
      record_.mutationUnlocked = mutation.try_lock();
      if (record_.mutationUnlocked) mutation.unlock();
      if (mapProbe_) record_.mapUnlocked = mapProbe_();
    });
    probe.join();
    record_.destroyed = true;
  }
 private:
  RetirementRecord& record_;
  const std::function<bool()> mapProbe_;
};

bool vectorMapUnlocked() {
  return static_cast<bool>(exec::vectorFunctionFactories().tryRLock());
}

void expectReleased(const RetirementRecord& record, bool hasMapProbe = true) {
  EXPECT_TRUE(record.destroyed);
  EXPECT_TRUE(record.mutationUnlocked);
  if (hasMapProbe) {
    ASSERT_TRUE(record.mapUnlocked.has_value());
    EXPECT_TRUE(record.mapUnlocked.value());
  } else {
    EXPECT_FALSE(record.mapUnlocked.has_value());
  }
  EXPECT_EQ(record.destructorThread, std::this_thread::get_id());
}

void registerObservedVector(const std::string& name, RetirementRecord& record) {
  const auto base = exec::vectorFunctionFactories().withRLock(
      [](const auto& map) { return map.at("plus"); });
  auto observer = std::make_shared<RetirementObserver>(record, vectorMapUnlocked);
  auto factory = base.factory;
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name,
      base.signatures,
      [factory, observer](const auto& name, const auto& arguments, const auto& config) {
        return factory(name, arguments, config);
      }, base.metadata));
}

void overwriteVector(const std::string& name) {
  const auto entry = exec::vectorFunctionFactories().withRLock(
      [](const auto& map) { return map.at("plus"); });
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name, entry.signatures, entry.factory, entry.metadata));
}

TEST(FunctionRegistryRetirementTest, VectorOverwriteReleasesLastFactoryOutsideBothLocks) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  registerObservedVector("owned_retirement_vector_overwrite", record);
  overwriteVector("owned_retirement_vector_overwrite");
  expectReleased(record);
}

TEST(FunctionRegistryRetirementTest, VectorRemoveReleasesLastFactoryOutsideBothLocks) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  registerObservedVector("owned_retirement_vector_remove", record);
  facebook::velox::removeFunction("owned_retirement_vector_remove");
  expectReleased(record);
}

TEST(FunctionRegistryRetirementTest, ExactRuntimeRetirementReleasesFactoriesOutsideBothLocksOnCallerThread) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  const auto ownership = std::make_shared<const exec::FunctionRegistrationOwner>();
  const auto base = exec::vectorFunctionFactories().withRLock(
      [](const auto& map) { return map.at("plus"); });
  {
    auto observer = std::make_shared<RetirementObserver>(record, vectorMapUnlocked);
    ASSERT_TRUE(exec::registerStatefulVectorFunction("owned_runtime_retirement",
        base.signatures,
        [factory = base.factory, observer](const auto& name, const auto& arguments, const auto& config) {
          return factory(name, arguments, config);
        }, base.metadata, true, ownership));
  }
  exec::unregisterOwnedFunctionEntries(ownership);
  expectReleased(record);
  EXPECT_FALSE(exec::FunctionRegistrySnapshot::capture()->bindScalar(
      "owned_runtime_retirement", {BIGINT(), BIGINT()}).has_value());
}

TEST(FunctionRegistryRetirementTest, VectorRejectedCandidateReleasesOutsideBothLocks) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  const std::string name = "owned_retirement_vector_rejected";
  const auto base = exec::vectorFunctionFactories().withRLock(
      [](const auto& map) { return map.at("plus"); });
  ASSERT_TRUE(exec::registerStatefulVectorFunction(name, base.signatures, base.factory, base.metadata));
  EXPECT_FALSE(exec::registerStatefulVectorFunction(name, base.signatures,
      [factory = base.factory,
       owner = std::make_shared<RetirementObserver>(record, vectorMapUnlocked)](
          const auto& functionName, const auto& arguments, const auto& config) {
        return factory(functionName, arguments, config);
      }, base.metadata, false));
  expectReleased(record);
}

TEST(FunctionRegistryRetirementTest, VectorClearReleasesLastFactoryOutsideBothLocks) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  registerObservedVector("owned_retirement_vector_clear", record);
  facebook::velox::clearFunctionRegistry();
  expectReleased(record);
  pangrova::capability::registerBuiltinFunctions();
}

TEST(FunctionRegistryRetirementTest, AggregateOverwriteReleasesLastFactoryOutsideBothLocks) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  const auto base = exec::FunctionRegistrySnapshot::capture()->aggregate("sum");
  ASSERT_NE(base, nullptr);
  {
    auto observer = std::make_shared<RetirementObserver>(record, [] {
      return static_cast<bool>(exec::aggregateFunctions().tryRLock());
    });
    const auto factory = base->factory;
    exec::registerAggregateFunction("owned_retirement_aggregate", base->signatures,
        [factory, observer](const auto& step, const auto& arguments, const auto& type, const auto& config) {
          return factory(step, arguments, type, config);
        }, base->metadata, false, true);
  }
  exec::registerAggregateFunction("owned_retirement_aggregate", base->signatures,
      base->factory, base->metadata, false, true);
  expectReleased(record);
}

TEST(FunctionRegistryRetirementTest, WindowOverwriteReleasesLastFactoryOutsideMutationLock) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  const auto base = exec::FunctionRegistrySnapshot::capture()->window("row_number");
  ASSERT_NE(base, nullptr);
  {
    auto observer = std::make_shared<RetirementObserver>(record, std::function<bool()>{});
    const auto factory = base->factory;
    exec::registerWindowFunction("owned_retirement_window", base->signatures, base->metadata,
        [factory, observer](const auto& arguments, const auto& type, bool ignoreNulls,
            auto* pool, auto* strings, const auto& config) {
          return factory(arguments, type, ignoreNulls, pool, strings, config);
        });
  }
  exec::registerWindowFunction("owned_retirement_window", base->signatures, base->metadata, base->factory);
  expectReleased(record, false);
}

class RetainedSpecialForm final : public exec::FunctionCallToSpecialForm {
 public:
  explicit RetainedSpecialForm(std::shared_ptr<RetirementObserver> observer) : observer_(std::move(observer)) {}
  TypePtr resolveType(const std::vector<TypePtr>& arguments) override {
    VELOX_CHECK_EQ(arguments.size(), 1);
    return arguments.front();
  }
  exec::ExprPtr constructSpecialForm(const TypePtr& type, std::vector<exec::ExprPtr>&& arguments,
      bool, const core::QueryConfig&) override {
    VELOX_CHECK_EQ(arguments.size(), 1);
    VELOX_CHECK(type->equivalent(*arguments.front()->type()));
    return std::move(arguments.front());
  }
 private:
  const std::shared_ptr<RetirementObserver> observer_;
};

TEST(FunctionRegistryRetirementTest, SpecialOverwriteReleasesLastOwnerOutsideMutationLock) {
  pangrova::test::detail::ensureRuntimeInitialized();
  RetirementRecord record;
  exec::SpecialFormRegistry registry;
  registry.registerFunctionCallToSpecialForm("owned_retirement_special",
      std::make_unique<RetainedSpecialForm>(std::make_shared<RetirementObserver>(
          record, std::function<bool()>{})));
  registry.registerFunctionCallToSpecialForm("owned_retirement_special", std::make_unique<RetainedSpecialForm>(nullptr));
  expectReleased(record, false);
  ASSERT_NE(registry.getSpecialForm("owned_retirement_special"), nullptr);
  EXPECT_TRUE(registry.getSpecialForm("owned_retirement_special")->resolveType({BIGINT()})->equivalent(*BIGINT()));
}

TEST(FunctionRegistryRetirementTest, SpecialClearReleasesLastOwnerOutsideMutationLock) {
  RetirementRecord record;
  exec::SpecialFormRegistry registry;
  registry.registerFunctionCallToSpecialForm("owned_retirement_special_clear",
      std::make_unique<RetainedSpecialForm>(std::make_shared<RetirementObserver>(
          record, std::function<bool()>{})));
  registry.unregisterAllFunctionCallToSpecialForm();
  expectReleased(record, false);
  EXPECT_TRUE(registry.getSpecialFormNames().empty());
}

}  // namespace
}  // namespace pangrova::capability::test
