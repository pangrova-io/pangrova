#include "capability/FunctionCatalogPublication.h"

#include <gtest/gtest.h>

#include <exception>
#include <latch>
#include <stdexcept>
#include <thread>
#include <utility>

namespace pangrova::capability::test {

TEST(FunctionCatalogStartupFailureTest, NoLazyOwnerBeforeExplicitFinalize) {
  FunctionCatalogStartup startup;
  EXPECT_THROW(startup.requirePublished(), std::logic_error);
}

TEST(FunctionCatalogStartupFailureTest, FailedActualRegistrationCannotFinalizePriorCatalog) {
  FunctionCatalogStartup startup;
  {
    auto registration = startup.beginRegistration();
    EXPECT_THROW(startup.beginRegistration(), std::logic_error);
    EXPECT_THROW(startup.finalize([]() -> std::shared_ptr<const FunctionCatalogPublication> {
      throw std::runtime_error("Preparation must not execute during registration");
    }), std::logic_error);
  }
  EXPECT_THROW(startup.beginRegistration(), std::logic_error);
  EXPECT_THROW(startup.finalize([]() -> std::shared_ptr<const FunctionCatalogPublication> {
    throw std::runtime_error("Preparation must not execute after failed registration");
  }), std::logic_error);
  EXPECT_THROW(startup.requirePublished(), std::logic_error);
}

TEST(FunctionCatalogStartupFailureTest, CompletedRegistrationRemainsExclusiveUntilScopeExit) {
  FunctionCatalogStartup startup;
  {
    auto registration = startup.beginRegistration();
    registration.complete();
    EXPECT_THROW(registration.complete(), std::logic_error);
    EXPECT_THROW(startup.beginRegistration(), std::logic_error);
    EXPECT_THROW(startup.close(), std::logic_error);
  }
  EXPECT_THROW(startup.finalize([]() -> std::shared_ptr<const FunctionCatalogPublication> {
    throw std::runtime_error("Actual preparation failure");
  }), std::runtime_error);
  EXPECT_THROW(startup.beginRegistration(), std::logic_error);
  EXPECT_THROW(startup.requirePublished(), std::logic_error);
}

TEST(FunctionCatalogStartupFailureTest, MovedRegistrationOwnsExactlyOneCompletion) {
  FunctionCatalogStartup startup;
  {
    auto original = startup.beginRegistration();
    auto registration = std::move(original);
    EXPECT_THROW(original.complete(), std::logic_error);
    registration.complete();
  }
  EXPECT_THROW(startup.finalize([]() -> std::shared_ptr<const FunctionCatalogPublication> {
    throw std::runtime_error("Actual preparation failure");
  }), std::runtime_error);
}

TEST(FunctionCatalogStartupFailureTest, CloseDoesNotReopenThisIncarnation) {
  FunctionCatalogStartup startup;
  startup.close();
  startup.close();
  EXPECT_THROW(startup.beginRegistration(), std::logic_error);
  EXPECT_THROW(startup.requirePublished(), std::logic_error);
}

TEST(FunctionCatalogStartupFailureTest, ConcurrentFinalizeCannotCaptureDuringActualRegistration) {
  FunctionCatalogStartup startup;
  std::latch entered(1);
  std::latch finish(1);
  std::exception_ptr failure;
  std::thread registration([&] {
    bool announced = false;
    try {
      auto lease = startup.beginRegistration();
      announced = true;
      entered.count_down();
      finish.wait();
      lease.complete();
    } catch (...) {
      failure = std::current_exception();
      if (!announced) entered.count_down();
    }
  });
  entered.wait();
  EXPECT_THROW(startup.finalize([]() -> std::shared_ptr<const FunctionCatalogPublication> {
    throw std::runtime_error("Concurrent preparation must not execute");
  }), std::logic_error);
  EXPECT_THROW(startup.close(), std::logic_error);
  finish.count_down();
  registration.join();
  if (failure) std::rethrow_exception(failure);
  EXPECT_THROW(startup.requirePublished(), std::logic_error);
}

}  // namespace pangrova::capability::test
