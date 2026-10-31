#pragma once

#include "capabilities/capability/FunctionCatalogPublication.h"

namespace pangrova::capability {

// Builds an immutable catalogue from the actual owned SDK registry and the
// sole compiled binding/type declarations. No execution factory is probed.
class FunctionCatalogAssembler final {
 public:
  static std::shared_ptr<const FunctionCatalogPublication> assemble(
      std::shared_ptr<const FunctionCatalogPublication::Registry> registry,
      protocol::v2::CapabilityInventory inventory);
};

}  // namespace pangrova::capability
