// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "capabilities/capability/LogicalTypeContract.h"

#include <array>

#include <gtest/gtest.h>

namespace pangrova::capability {
namespace {

protocol::v2::LogicalType vectorType(uint32_t dimension) {
  protocol::v2::LogicalType result;
  result.set_canonical_sql("VECTOR(" + std::to_string(dimension) + ")");
  auto* list = result.mutable_substrait_type()->mutable_list();
  list->set_nullability(
      substrait::Type_Nullability_NULLABILITY_NULLABLE);
  list->mutable_type()->mutable_fp32()->set_nullability(
      substrait::Type_Nullability_NULLABILITY_REQUIRED);
  auto* dimension_constraint = result.add_constraints();
  dimension_constraint->set_name("dimension");
  dimension_constraint->set_integer_value(dimension);
  auto* element_constraint = result.add_constraints();
  element_constraint->set_name("element_type");
  element_constraint->set_string_value("FLOAT32");
  return result;
}

TEST(LogicalTypeContractTest, AcceptsCanonicalFloatVector) {
  EXPECT_EQ(requireFloatVectorDimension(vectorType(1536)), 1536);
}

TEST(LogicalTypeContractTest, CanonicalTextUsesTheSameDimensionBoundsAsTheTypedContract) {
  for (const uint32_t dimension : {1U, 384U, 65'535U}) {
    const auto type = vectorType(dimension);
    EXPECT_EQ(requireCanonicalFloatVectorDimension(type.canonical_sql()), dimension);
    EXPECT_EQ(requireFloatVectorDimension(type), dimension);
  }
}

TEST(LogicalTypeContractTest, RejectsNonCanonicalTextWithoutLegacyMetricOrPrecisionAliases) {
  const std::array invalid{
      "", "VECTOR()", "VECTOR(0)", "VECTOR(-1)", "VECTOR(65536)",
      "VECTOR(01)", "VECTOR(2, COSINE)", "VECTOR(2,DOUBLE)",
      "VECTOR( 2)", "VECTOR(2 )", "vector(2)", "VECTOR(2)extra", "ARRAY(REAL)"};
  for (const auto* declaration : invalid) {
    EXPECT_THROW(requireCanonicalFloatVectorDimension(declaration), std::invalid_argument)
        << declaration;
  }
}

TEST(LogicalTypeContractTest, RejectsMissingDimensionConstraint) {
  auto type = vectorType(384);
  type.mutable_constraints(0)->set_integer_value(768);
  EXPECT_THROW(requireFloatVectorDimension(type), std::invalid_argument);
}

TEST(LogicalTypeContractTest, RejectsUnboundedArrayAlias) {
  auto type = vectorType(384);
  type.set_canonical_sql("ARRAY(REAL)");
  EXPECT_THROW(requireFloatVectorDimension(type), std::invalid_argument);
}

}  // namespace
}  // namespace pangrova::capability
