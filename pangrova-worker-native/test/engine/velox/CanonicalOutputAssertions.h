#pragma once

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <string>

#include "engine/velox/plan/CanonicalOrdinalIdentity.h"
#include "engine/velox/plan/VeloxPlanConverter.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

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

inline void expectOrdinalIdentityProjection(
    const std::shared_ptr<const facebook::velox::core::ProjectNode>& project) {
  ASSERT_NE(project, nullptr);
  ASSERT_EQ(project->sources().size(), 1);
  const auto& source = project->sources().front()->outputType();
  ASSERT_EQ(project->projections().size(), source->size());
  for (size_t ordinal = 0; ordinal < source->size(); ++ordinal) {
    const auto field = std::dynamic_pointer_cast<const facebook::velox::core::FieldAccessTypedExpr>(
        project->projections().at(ordinal));
    ASSERT_NE(field, nullptr);
    EXPECT_TRUE(field->inputs().empty());
    EXPECT_EQ(field->name(), source->nameOf(ordinal));
    EXPECT_TRUE(field->type()->equivalent(*source->childAt(ordinal)));
  }
}
}  // namespace pangrova::velox::test
