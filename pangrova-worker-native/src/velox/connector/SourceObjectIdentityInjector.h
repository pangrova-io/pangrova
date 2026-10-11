// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <string>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

/** Inserts requested virtual constants from the canonical signed split only. */
facebook::velox::RowVectorPtr injectPhysicalSourceIdentity(
    const facebook::velox::RowVectorPtr& input,
    const std::array<std::string, 3>& source_values,
    const facebook::velox::RowTypePtr& requested_output_type,
    facebook::velox::memory::MemoryPool* pool);

/**
 * Inserts the canonical split data-object identity as a Native constant
 * column. The input is the physical reader schema with exactly that reserved
 * column removed; the result preserves all other reader names and types.
 */
facebook::velox::RowVectorPtr injectSourceObjectIdentity(
    const facebook::velox::RowVectorPtr& input,
    const std::string& data_object_id,
    const facebook::velox::RowTypePtr& requested_output_type,
    facebook::velox::memory::MemoryPool* pool);

}  // namespace pangrova::velox
