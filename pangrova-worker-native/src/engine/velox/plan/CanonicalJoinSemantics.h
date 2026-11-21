#pragma once

#include <google/protobuf/generated_enum_reflection.h>

#include <stdexcept>
#include <string>
#include <unordered_map>

#include "velox/core/PlanNode.h"
#include "engine/velox/plan/CanonicalOrdinalIdentity.h"

namespace pangrova::velox {

// Relation enums have different numbers. Their canonical names, not integers,
// select one set of actual Velox join semantics.
template <typename Enum>
std::string canonicalJoinKind(Enum value) {
  const auto descriptor = google::protobuf::GetEnumDescriptor<Enum>();
  const auto entry = descriptor->FindValueByNumber(static_cast<int>(value));
  if (!entry || entry->name() == "JOIN_TYPE_UNSPECIFIED") {
    throw std::runtime_error("Join requires a canonical join type");
  }
  return entry->name();
}

inline facebook::velox::core::JoinType canonicalJoinType(const std::string& kind) {
  using JoinType = facebook::velox::core::JoinType;
  static const std::unordered_map<std::string, JoinType> types = {
      {"JOIN_TYPE_INNER", JoinType::kInner},
      {"JOIN_TYPE_OUTER", JoinType::kFull},
      {"JOIN_TYPE_LEFT", JoinType::kLeft},
      {"JOIN_TYPE_RIGHT", JoinType::kRight},
      {"JOIN_TYPE_LEFT_SINGLE", JoinType::kLeft},
      {"JOIN_TYPE_RIGHT_SINGLE", JoinType::kRight},
      {"JOIN_TYPE_LEFT_SEMI", JoinType::kLeftSemiFilter},
      {"JOIN_TYPE_RIGHT_SEMI", JoinType::kRightSemiFilter},
      {"JOIN_TYPE_LEFT_ANTI", JoinType::kAnti},
      {"JOIN_TYPE_RIGHT_ANTI", JoinType::kRightSemiProject},
      {"JOIN_TYPE_LEFT_MARK", JoinType::kLeftSemiProject},
      {"JOIN_TYPE_RIGHT_MARK", JoinType::kRightSemiProject}};
  const auto entry = types.find(kind);
  if (entry == types.end()) throw std::runtime_error("Unknown canonical join type");
  return entry->second;
}

inline facebook::velox::RowTypePtr combinedJoinType(
    const facebook::velox::RowTypePtr& left,
    const facebook::velox::RowTypePtr& right) {
  auto names = left->names();
  auto types = left->children();
  names.insert(names.end(), right->names().begin(), right->names().end());
  types.insert(types.end(), right->children().begin(), right->children().end());
  return facebook::velox::ROW(std::move(names), std::move(types));
}

inline facebook::velox::RowTypePtr canonicalJoinOutput(
    const std::string& kind, const facebook::velox::RowTypePtr& left,
    const facebook::velox::RowTypePtr& right) {
  const bool subset = kind.ends_with("_SEMI") || kind.ends_with("_ANTI") || kind.ends_with("_MARK");
  auto output = subset ? (kind.starts_with("JOIN_TYPE_LEFT") ? left : right)
                       : combinedJoinType(left, right);
  if (!kind.ends_with("_MARK")) return output;
  auto names = output->names();
  auto types = output->children();
  names.push_back(physicalFieldAlias(left->size() + right->size()));
  types.push_back(facebook::velox::BOOLEAN());
  return facebook::velox::ROW(std::move(names), std::move(types));
}

inline facebook::velox::core::JoinType reverseJoinType(facebook::velox::core::JoinType type) {
  using JoinType = facebook::velox::core::JoinType;
  switch (type) {
    case JoinType::kLeft: return JoinType::kRight;
    case JoinType::kRight: return JoinType::kLeft;
    case JoinType::kLeftSemiFilter: return JoinType::kRightSemiFilter;
    case JoinType::kRightSemiFilter: return JoinType::kLeftSemiFilter;
    case JoinType::kLeftSemiProject: return JoinType::kRightSemiProject;
    case JoinType::kRightSemiProject: return JoinType::kLeftSemiProject;
    case JoinType::kAnti: return JoinType::kRightSemiProject;
    default: return type;
  }
}

}  // namespace pangrova::velox
