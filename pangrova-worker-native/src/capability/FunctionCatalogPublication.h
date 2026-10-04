#pragma once

#include <functional>
#include <memory>
#include <mutex>

#include "pangrova/protocol/v2/capability.pb.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

namespace pangrova::capability {

class FunctionCatalogAssembler;

// Construction belongs to the actual SDK-entry assembler, never a transport
// caller or a test-only manifest. The assembler validates the complete typed
// inventory before creating this immutable execution owner.
class FunctionCatalogPublication final {
 public:
  using Registry = facebook::velox::exec::FunctionRegistrySnapshot;

  class ConstructionAuthority final {
   private:
    friend class FunctionCatalogAssembler;
    ConstructionAuthority() = default;
  };

  FunctionCatalogPublication(std::shared_ptr<const Registry> registry,
      protocol::v2::CapabilityInventory inventory, ConstructionAuthority);

  const std::shared_ptr<const Registry>& registry() const { return registry_; }
  const protocol::v2::CapabilityInventory& inventory() const { return inventory_; }

 private:
  const std::shared_ptr<const Registry> registry_;
  const protocol::v2::CapabilityInventory inventory_;
};

// One object per actual Native runtime incarnation. There is deliberately no
// reset, replacement, generation-update, or lazy GET_CAPABILITIES capture API.
class FunctionCatalogStartup final {
 private:
  enum class Phase { kNew, kPreparing, kPublished, kFailed, kClosed };
  struct State {
    std::mutex mutex;
    Phase phase{Phase::kNew};
    bool registering{false};
    std::shared_ptr<const FunctionCatalogPublication> publication;
  };

 public:
  class Registration final {
   public:
    Registration(Registration&& other) noexcept;
    Registration(const Registration&) = delete;
    Registration& operator=(const Registration&) = delete;
    Registration& operator=(Registration&&) = delete;
    ~Registration();

    // Called only after the entire actual registration and callback publication
    // have succeeded. Destruction, not completion, releases registration scope.
    void complete();

   private:
    friend class FunctionCatalogStartup;
    explicit Registration(std::shared_ptr<State> state);
    std::shared_ptr<State> state_;
    bool completed_{false};
  };

  using Prepare =
      std::function<std::shared_ptr<const FunctionCatalogPublication>()>;

  FunctionCatalogStartup();
  FunctionCatalogStartup(const FunctionCatalogStartup&) = delete;
  FunctionCatalogStartup& operator=(const FunctionCatalogStartup&) = delete;

  // Reserves the real entire JNI registration transaction without keeping a
  // mutex across JNI, allocation, an eager registration factory, or SDK locks.
  Registration beginRegistration();

  // Prepare captures/project/validates only actual registrations, outside the
  // publication lock. It must not instantiate execution factories or values.
  std::shared_ptr<const FunctionCatalogPublication> finalize(Prepare prepare);

  // Returns the already published immutable owner; no live registry read.
  std::shared_ptr<const FunctionCatalogPublication> requirePublished() const;

  // The surrounding runtime must first drain its real creation/executor owners.
  // An in-flight registration/preparation makes close fail, not pretend drained.
  std::shared_ptr<const FunctionCatalogPublication> close();

 private:
  const std::shared_ptr<State> state_;
};

}  // namespace pangrova::capability
