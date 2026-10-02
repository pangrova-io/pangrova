// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "velox/connector/SourceObjectIdentityInjector.h"

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
  if (!input || !requested_output_type || !pool) {
    throw std::invalid_argument(
        "Source-object identity injection requires input, output type, and pool");
  }
  if (data_object_id.empty()) {
    throw std::invalid_argument(
        "Source-object identity injection requires a non-empty data_object_id");
  }

  size_t identity_channel = requested_output_type->size();
  for (size_t channel = 0; channel < requested_output_type->size(); ++channel) {
    if (requested_output_type->nameOf(channel) != kSourceObjectIdColumnName) {
      continue;
    }
    if (identity_channel != requested_output_type->size() ||
        requested_output_type->childAt(channel)->kind() !=
            facebook::velox::TypeKind::VARCHAR) {
      throw std::invalid_argument(
          "Source-object identity column must occur once and be VARCHAR");
    }
    identity_channel = channel;
  }
  if (identity_channel == requested_output_type->size()) {
    throw std::invalid_argument(
        "Requested output type has no source-object identity column");
  }

  const auto& input_type = input->type()->asRow();
  if (input_type.containsChild(kSourceObjectIdColumnName) ||
      input_type.size() + 1 != requested_output_type->size()) {
    throw std::invalid_argument(
        "Physical reader schema must omit exactly the source-object identity column");
  }

  std::vector<std::string> names = input_type.names();
  std::vector<facebook::velox::TypePtr> types = input_type.children();
  std::vector<facebook::velox::VectorPtr> children = input->children();
  names.insert(names.begin() + identity_channel,
               std::string(kSourceObjectIdColumnName));
  types.insert(types.begin() + identity_channel, facebook::velox::VARCHAR());
  children.insert(
      children.begin() + identity_channel,
      facebook::velox::BaseVector::createConstant(
          facebook::velox::VARCHAR(), facebook::velox::variant(data_object_id),
          input->size(), pool));

  return std::make_shared<facebook::velox::RowVector>(
      pool, facebook::velox::ROW(std::move(names), std::move(types)), nullptr,
      input->size(), std::move(children));
}

}  // namespace pangrova::velox
