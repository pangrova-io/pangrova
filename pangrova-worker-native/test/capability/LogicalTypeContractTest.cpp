// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "capability/LogicalTypeContract.h"

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
