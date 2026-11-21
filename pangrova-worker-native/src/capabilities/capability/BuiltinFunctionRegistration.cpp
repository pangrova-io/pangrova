#include "capabilities/capability/BuiltinFunctionRegistration.h"

#include "indexing/text/bm25/TextSearchFunctions.h"
#include "engine/variant/VariantPathExpression.h"
#include "spatial/SpatialFunctions.h"
#include "vector/VectorDistanceFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "engine/velox/window/DistinctAggregateWindow.h"
#include "engine/velox/aggregate/SumZeroAggregate.h"

namespace pangrova::capability {

void registerBuiltinFunctions() {
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions(
      /*prefix=*/"", /*withCompanionFunctions=*/true);
  pangrova::velox::registerSumZeroAggregate();
  facebook::velox::window::prestosql::registerAllWindowFunctions();
  pangrova::velox::registerDistinctAggregateWindows();
  pangrova::vector::registerVectorDistanceFunctions();
  pangrova::spatial::registerSpatialFunctions();
  pangrova::text::bm25::registerTextSearchFunctions();
  // A missing registered implementation must prevent readiness, not be logged
  // and silently omitted from an otherwise advertised executable contract.
  pangrova::variant::registerVariantPathFunctions();
}

}  // namespace pangrova::capability
