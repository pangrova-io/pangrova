#pragma once

#include <gtest/gtest.h>

#include "velox/plan/CanonicalOrdinalIdentity.h"
#include "velox/plan/VeloxPlanConverter.h"

namespace pangrova::velox::test {
// Logical labels are the declared Root contract. Physical names identify
// ordinals inside one execution graph; both must be asserted independently.
inline void expectCanonicalOutputField(const VeloxPlanConverter& converter,
    const facebook::velox::RowTypePtr& physicalType, size_t ordinal,
    const std::string& logicalLabel) {
  ASSERT_LT(ordinal, physicalType->size());
  ASSERT_LT(ordinal, converter.resultColumnLabels().size());
  EXPECT_EQ(converter.resultColumnLabels()[ordinal], logicalLabel);
  EXPECT_EQ(physicalType->nameOf(ordinal), physicalFieldAlias(ordinal));
}
}  // namespace pangrova::velox::test
