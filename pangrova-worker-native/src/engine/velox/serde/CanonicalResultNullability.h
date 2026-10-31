#pragma once

#include "substrait/type.pb.h"
#include "velox/vector/ComplexVector.h"

namespace pangrova::velox {

// Validates explicit field nullability and full DFS names before any data exists.
facebook::velox::RowTypePtr admittedCanonicalResultType(const substrait::NamedStruct& schema);

// Checks actual result values against the admitted logical output contract.
// Physical projection erases SQL nullability; encoding must not erase it too.
void validateCanonicalResultNullability(const substrait::NamedStruct& schema,
                                      const facebook::velox::RowVectorPtr& rows);

}  // namespace pangrova::velox
