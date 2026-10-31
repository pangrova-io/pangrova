#pragma once

#include "pangrova/protocol/v2/capability.pb.h"
#include "velox/expression/FunctionSignature.h"

namespace pangrova::capability {

// Actual signature AST -> descriptor-checked canonical pattern via the sole
// generated Native type bridge. Unprojected custom AST remains diagnostic.
class RegisteredTypeTemplate final {
 public:
  RegisteredTypeTemplate();
  protocol::v2::FunctionTypeTemplate convert(
      const facebook::velox::exec::TypeSignature& type,
      const facebook::velox::exec::FunctionSignature& signature) const;

 private:
  protocol::v2::FunctionCatalogManifest projections_;
};

}  // namespace pangrova::capability
