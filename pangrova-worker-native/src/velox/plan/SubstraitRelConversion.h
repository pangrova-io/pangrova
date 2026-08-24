// SubstraitRelConversion.h — Declares the operator conversion methods
// implemented in SubstraitRelConversion.cpp.
//
// These are member functions of VeloxPlanConverter and are declared in
// VeloxPlanConverter.h. This header serves as a module map documenting
// the methods split into this translation unit for maintainability.
//
// Conversion methods:
//   convertReadRel, convertFilterRel, convertProjectRel,
//   convertAggregateRel, convertHashJoinRel, convertJoinRel,
//   convertExchangeRel, convertSortRel, convertFetchRel,
//   convertWindowRel, convertSetRel, convertCrossRel, convertSortOrder.
//
// This file is intentionally NOT included by other headers — it exists
// solely for IDE navigation and documentation purposes.

#pragma once

// All declarations live in VeloxPlanConverter.h.
#include "velox/plan/VeloxPlanConverter.h"
