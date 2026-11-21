// Explicit instantiations of veloxCheckFail templates.
//
// These are normally provided by velox/common/base/Exceptions.cpp, but the
// strip-velox-optional-deps.sh script uses `ar x` which extracts .o files
// into a flat directory. When libvelox.a contains multiple Exceptions.cpp.o
// (from common/base/ and dwio/common/exception/), the second overwrites the
// first, losing these critical instantiations.
//
// This file provides them so both the dylib and test binary can resolve
// the check-fail symbols without depending on the stripped .o ordering.

#include <velox/common/base/Exceptions.h>

namespace facebook::velox::detail {

DEFINE_CHECK_FAIL_TEMPLATES(::facebook::velox::VeloxRuntimeError);
DEFINE_CHECK_FAIL_TEMPLATES(::facebook::velox::VeloxUserError);

} // namespace facebook::velox::detail
