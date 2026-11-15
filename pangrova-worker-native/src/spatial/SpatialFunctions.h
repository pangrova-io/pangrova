#pragma once

namespace pangrova::spatial {

// Registers st_geomfromtext/st_geographyfromtext/st_within/st_intersects/
// st_contains with Velox. Only WKT POINT and POLYGON inputs are supported;
// other geometry kinds fail with an explicit user error instead of an empty
// or fabricated result. GEOGRAPHY uses the same planar predicates until a
// distinct geodesic operator exists.
void registerSpatialFunctions();

}  // namespace pangrova::spatial
