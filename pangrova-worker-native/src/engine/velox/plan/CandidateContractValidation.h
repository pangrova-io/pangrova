#pragma once

#include <array>
#include <stdexcept>
#include <string>

#include "capabilities/capability/NativeCapabilityCatalog.h"
#include "pangrova/protocol/v2/plan_extension.pb.h"
#include "velox/type/Type.h"
#include "engine/velox/connector/PangrovaTableHandle.h"

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
      (contract.identity_kind() != pangrova::protocol::v2::CANDIDATE_IDENTITY_KIND_VALUE_FIELDS &&
       contract.identity_kind() != pangrova::protocol::v2::CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW) ||
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
  if (contract.identity_kind() == pangrova::protocol::v2::CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW) {
    if (contract.stable_key_fields_size() != 4) {
      throw std::runtime_error("Physical retrieval identity requires exactly four fields");
    }
    const std::array<std::string_view, 4> required_names{
        kSourceIdColumnName, kSourceSnapshotColumnName,
        kSourceObjectIdColumnName, kPhysicalRowPositionColumnName};
    for (size_t key = 0; key < 4; ++key) {
      if (contract.stable_key_fields(static_cast<int>(key)) != required_names[key]) {
        throw std::runtime_error("Physical retrieval identity fields differ from signed source channels");
      }
      const auto channel = declared_type->getChildIdxIfExists(contract.stable_key_fields(key));
      if (!channel || declared_type->childAt(*channel)->kind() !=
          (key == 3 ? facebook::velox::TypeKind::BIGINT : facebook::velox::TypeKind::VARCHAR)) {
        throw std::runtime_error("Physical retrieval identity has invalid canonical types");
      }
      const auto& type = contract.schema().struct_().types(static_cast<int>(*channel));
      if ((key == 3 ? type.i64().nullability() : type.string().nullability()) !=
          substrait::Type::NULLABILITY_REQUIRED) {
        throw std::runtime_error("Physical retrieval identity fields must be required");
      }
    }
  }
}

}  // namespace pangrova::velox
