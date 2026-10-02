#pragma once

#include <stdexcept>
#include <string>

#include "capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/type/Type.h"

namespace pangrova::velox {

inline void validateContractSchema(
    const pangrova::protocol::v2::CandidateContract& contract,
    const facebook::velox::RowTypePtr& input_type,
    const facebook::velox::RowTypePtr& declared_type,
    const std::string& context) {
  if (contract.schema_version() !=
          pangrova::capability::kRetrievalCandidateSchemaVersion ||
      contract.score_direction() ==
          pangrova::protocol::v2::SCORE_DIRECTION_UNSPECIFIED ||
      contract.stable_key_fields_size() == 0) {
    throw std::runtime_error(
        "VeloxPlanConverter: invalid " + context + " candidate contract");
  }
  if (declared_type->size() != input_type->size()) {
    throw std::runtime_error(
        "VeloxPlanConverter: " + context + " candidate column count mismatch");
  }
  for (size_t i = 0; i < input_type->size(); ++i) {
    if (declared_type->nameOf(i) != input_type->nameOf(i) ||
        !declared_type->childAt(i)->equivalent(*input_type->childAt(i))) {
      throw std::runtime_error(
          "VeloxPlanConverter: " + context +
          " candidate schema mismatch at column " + std::to_string(i));
    }
  }
}

}  // namespace pangrova::velox

