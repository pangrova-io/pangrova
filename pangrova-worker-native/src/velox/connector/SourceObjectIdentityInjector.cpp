// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "velox/connector/SourceObjectIdentityInjector.h"

#include <algorithm>
#include <stdexcept>
#include <vector>

#include "velox/connector/PangrovaTableHandle.h"
#include "velox/vector/BaseVector.h"

namespace pangrova::velox {

facebook::velox::RowVectorPtr injectSourceObjectIdentity(
    const facebook::velox::RowVectorPtr& input,
    const std::string& data_object_id,
    const facebook::velox::RowTypePtr& requested_output_type,
    facebook::velox::memory::MemoryPool* pool) {
  return injectPhysicalSourceIdentity(input, {"", "", data_object_id}, requested_output_type, pool);
}

facebook::velox::RowVectorPtr injectPhysicalSourceIdentity(
    const facebook::velox::RowVectorPtr& input,
    const std::array<std::string, 3>& source_values,
    const facebook::velox::RowTypePtr& requested_output_type,
    facebook::velox::memory::MemoryPool* pool) {
  if (!input || !requested_output_type || !pool) {
    throw std::invalid_argument("Physical source identity injection requires input, type and pool");
  }
  const auto& input_type = input->type()->asRow();
  const std::array<std::string_view, 3> identity_names{
      kSourceIdColumnName, kSourceSnapshotColumnName, kSourceObjectIdColumnName};
  auto names = requested_output_type->names();
  auto types = requested_output_type->children();
  std::vector<facebook::velox::VectorPtr> children;
  children.reserve(requested_output_type->size());
  size_t reader_channel = 0;
  size_t injected = 0;
  for (size_t channel = 0; channel < requested_output_type->size(); ++channel) {
    const auto& name = requested_output_type->nameOf(channel);
    auto identity = std::find(identity_names.begin(), identity_names.end(), name);
    if (identity != identity_names.end()) {
      const auto ordinal = static_cast<size_t>(identity - identity_names.begin());
      if (input_type.containsChild(name) || source_values[ordinal].empty() ||
          requested_output_type->childAt(channel)->kind() != facebook::velox::TypeKind::VARCHAR ||
          std::count(requested_output_type->names().begin(), requested_output_type->names().end(), name) != 1) {
        throw std::invalid_argument("Physical source identity field is invalid or reader supplied");
      }
      children.push_back(facebook::velox::BaseVector::createConstant(
          facebook::velox::VARCHAR(), facebook::velox::variant(source_values[ordinal]), input->size(), pool));
      ++injected;
    } else {
      if (reader_channel >= input_type.size() || isSourceIdentityConstant(input_type.nameOf(reader_channel))) {
        throw std::invalid_argument("Physical reader schema differs from the requested source identity schema");
      }
      // Physical source names/types may precede schema-evolution mapping.
      // Preserve them; virtual constants must not rename ordinary file columns.
      names[channel] = input_type.nameOf(reader_channel);
      types[channel] = input_type.childAt(reader_channel);
      children.push_back(input->childAt(reader_channel++));
    }
  }
  if (injected == 0 || reader_channel != input_type.size()) {
    throw std::invalid_argument("Physical reader schema must omit exactly the requested identity constants");
  }
  return std::make_shared<facebook::velox::RowVector>(pool, facebook::velox::ROW(names, types), nullptr,
      input->size(), std::move(children));
}

}  // namespace pangrova::velox
