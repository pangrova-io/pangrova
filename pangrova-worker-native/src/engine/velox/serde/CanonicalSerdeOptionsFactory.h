#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "substrait/type.pb.h"
#include "velox/vector/VectorStream.h"

namespace pangrova::velox {

// Immutable canonical schemas bound to actual SDK serialization node identities.
class CanonicalSerdeOptionsFactory final
    : public facebook::velox::VectorSerdeOptionsFactory {
 public:
  using SchemaBindings = std::unordered_map<std::string, substrait::NamedStruct>;

  CanonicalSerdeOptionsFactory(SchemaBindings schemas, int64_t maximumBytes);

  std::unique_ptr<facebook::velox::VectorSerde::Options> create(
      const std::string& kind, const std::string& planNodeId) const override;

 private:
  using OwnedSchemas = std::unordered_map<
      std::string, std::shared_ptr<const substrait::NamedStruct>>;

  static OwnedSchemas admitSchemas(SchemaBindings schemas, int64_t maximumBytes);

  const OwnedSchemas schemas_;
  const int64_t maximum_bytes_;
};

}  // namespace pangrova::velox
