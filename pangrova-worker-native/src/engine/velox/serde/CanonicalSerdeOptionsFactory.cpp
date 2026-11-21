#include "engine/velox/serde/CanonicalSerdeOptionsFactory.h"

#include <stdexcept>
#include <utility>

#include "engine/columnar/CanonicalBatchStream.h"
#include "engine/velox/serde/ArrowBatchVectorSerde.h"
#include "engine/velox/serde/CanonicalResultNullability.h"

namespace pangrova::velox {

CanonicalSerdeOptionsFactory::CanonicalSerdeOptionsFactory(
    SchemaBindings schemas, int64_t maximumBytes)
    : schemas_(admitSchemas(std::move(schemas), maximumBytes)),
      maximum_bytes_(maximumBytes) {}

CanonicalSerdeOptionsFactory::OwnedSchemas
CanonicalSerdeOptionsFactory::admitSchemas(SchemaBindings schemas, int64_t maximumBytes) {
  if (maximumBytes <= 0 || maximumBytes > columnar::kMaximumBatchStreamBytes) {
    throw std::invalid_argument("Canonical serde requires a bounded positive wire size");
  }
  OwnedSchemas admitted;
  admitted.reserve(schemas.size());
  for (auto& [nodeId, declared] : schemas) {
    if (nodeId.empty()) throw std::invalid_argument("Canonical serde requires an actual node identity");
    admittedCanonicalResultType(declared);
    admitted.emplace(nodeId, std::make_shared<const substrait::NamedStruct>(std::move(declared)));
  }
  return admitted;
}

std::unique_ptr<facebook::velox::VectorSerde::Options>
CanonicalSerdeOptionsFactory::create(
    const std::string& kind, const std::string& planNodeId) const {
  if (kind != ArrowBatchVectorSerde::kSerdeKind) {
    throw std::invalid_argument("Canonical serde binding requires ArrowBatch");
  }
  const auto found = schemas_.find(planNodeId);
  if (found == schemas_.end()) {
    throw std::invalid_argument("Serialization node has no canonical schema binding: " + planNodeId);
  }
  auto options = std::make_unique<ArrowBatchVectorSerde::StreamOptions>();
  options->maximum_bytes = maximum_bytes_;
  options->canonical_schema = found->second;
  return options;
}

}  // namespace pangrova::velox
