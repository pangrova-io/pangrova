#include "capabilities/capability/FunctionCatalogPublication.h"

#include <stdexcept>
#include <utility>

namespace pangrova::capability {

FunctionCatalogPublication::FunctionCatalogPublication(
    std::shared_ptr<const Registry> registry,
    protocol::v2::CapabilityInventory inventory, ConstructionAuthority)
    : registry_(std::move(registry)), inventory_(std::move(inventory)) {
  if (!registry_ || !inventory_.has_function_catalog()) {
    throw std::invalid_argument("Actual function publication requires its SDK owner and validated typed catalogue");
  }
}

FunctionCatalogStartup::Registration::Registration(std::shared_ptr<State> state)
    : state_(std::move(state)) {}

FunctionCatalogStartup::Registration::Registration(Registration&& other) noexcept
    : state_(std::move(other.state_)), completed_(other.completed_) {}

FunctionCatalogStartup::Registration::~Registration() {
  if (state_) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!completed_) state_->phase = Phase::kFailed;
    state_->registering = false;
  }
}

void FunctionCatalogStartup::Registration::complete() {
  if (!state_ || completed_) {
    throw std::logic_error("Actual registration completion requires its active lease");
  }
  completed_ = true;
}

FunctionCatalogStartup::FunctionCatalogStartup()
    : state_(std::make_shared<State>()) {}

FunctionCatalogStartup::Registration FunctionCatalogStartup::beginRegistration() {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->phase != Phase::kNew || state_->registering) {
    throw std::logic_error("Native function registration is not open for this runtime incarnation");
  }
  state_->registering = true;
  return Registration(state_);
}

std::shared_ptr<const FunctionCatalogPublication>
FunctionCatalogStartup::finalize(Prepare prepare) {
  if (!prepare) throw std::invalid_argument("Actual catalogue preparation is required");
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->phase != Phase::kNew || state_->registering) {
      throw std::logic_error("Native catalogue can only finalize after startup registration and before READY");
    }
    state_->phase = Phase::kPreparing;
  }
  // This local owner outlives the inner lock guards, including error unwinding.
  // A last callback/factory owner is therefore never retired under this mutex.
  std::shared_ptr<const FunctionCatalogPublication> prepared;
  try {
    prepared = prepare();
    if (!prepared) throw std::runtime_error("Actual catalogue preparation produced no owner");
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->phase != Phase::kPreparing) {
        throw std::logic_error("Native catalogue lost its exact startup preparation authority");
      }
      state_->publication = prepared;
      state_->phase = Phase::kPublished;
    }
    return prepared;
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->phase = Phase::kFailed;
    }
    throw;
  }
}

std::shared_ptr<const FunctionCatalogPublication>
FunctionCatalogStartup::requirePublished() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (state_->phase != Phase::kPublished || !state_->publication) {
    throw std::logic_error("Native function catalogue has not been published by the real startup boundary");
  }
  return state_->publication;
}

std::shared_ptr<const FunctionCatalogPublication> FunctionCatalogStartup::close() {
  std::shared_ptr<const FunctionCatalogPublication> retired;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->registering || state_->phase == Phase::kPreparing) {
      throw std::logic_error("Native startup function operations have not exited");
    }
    if (state_->phase == Phase::kClosed) return nullptr;
    retired = std::move(state_->publication);
    state_->phase = Phase::kClosed;
  }
  // The caller owns final retirement outside its surrounding lifecycle locks.
  return retired;
}

}  // namespace pangrova::capability
