// Native spatial functions: WKT -> WKB construction and point/polygon
// predicates. Only POINT and POLYGON WKT/WKB inputs are implemented; every
// other geometry kind fails explicitly instead of returning a fabricated
// answer.
#include "spatial/SpatialFunctions.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/StringVectorBuffer.h"

namespace pangrova::spatial {

namespace {

using namespace facebook::velox;

struct Point {
  double x{0};
  double y{0};
};

struct Ring {
  std::vector<Point> points;
};

struct Geometry {
  enum class Kind { POINT, POLYGON } kind{Kind::POINT};
  Point point;
  std::vector<Ring> rings;
};

enum class Position { OUTSIDE, INSIDE, BOUNDARY };

enum class SpatialOp { WITHIN, INTERSECTS, CONTAINS };

std::string_view trim(std::string_view text) {
  while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front()))) {
    text.remove_prefix(1);
  }
  while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
    text.remove_suffix(1);
  }
  return text;
}

bool startsWithIgnoreCase(std::string_view text, std::string_view prefix) {
  if (text.size() < prefix.size()) {
    return false;
  }
  for (std::size_t i = 0; i < prefix.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(text[i])) !=
        std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

std::vector<double> parseNumbers(std::string_view text) {
  std::string buffer(text);
  std::vector<double> values;
  const char* cursor = buffer.c_str();
  while (*cursor != '\0') {
    while (*cursor != '\0' && !std::isdigit(static_cast<unsigned char>(*cursor)) &&
           *cursor != '-' && *cursor != '+' && *cursor != '.') {
      ++cursor;
    }
    if (*cursor == '\0') {
      break;
    }
    char* end = nullptr;
    const double value = std::strtod(cursor, &end);
    VELOX_USER_CHECK(end != cursor, "Invalid numeric coordinate in WKT geometry");
    VELOX_USER_CHECK(std::isfinite(value), "WKT geometry coordinate must be finite");
    values.push_back(value);
    cursor = end;
  }
  return values;
}

std::vector<Ring> parsePolygonRings(std::string_view body) {
  VELOX_USER_CHECK(
      body.size() >= 2 && body.front() == '(' && body.back() == ')',
      "POLYGON WKT requires an outer coordinate list");
  const auto inner = body.substr(1, body.size() - 2);
  std::vector<Ring> rings;
  std::size_t index = 0;
  while (index < inner.size()) {
    while (index < inner.size() &&
           (std::isspace(static_cast<unsigned char>(inner[index])) ||
            inner[index] == ',')) {
      ++index;
    }
    if (index >= inner.size()) {
      break;
    }
    VELOX_USER_CHECK(
        inner[index] == '(', "POLYGON WKT ring must start with '('");
    const auto close = inner.find(')', index + 1);
    VELOX_USER_CHECK(
        close != std::string_view::npos, "POLYGON WKT ring is not closed");
    const auto numbers = parseNumbers(inner.substr(index + 1, close - index - 1));
    VELOX_USER_CHECK(
        numbers.size() >= 8 && numbers.size() % 2 == 0,
        "POLYGON WKT ring requires at least four coordinate pairs");
    Ring ring;
    for (std::size_t i = 0; i < numbers.size(); i += 2) {
      ring.points.push_back({numbers[i], numbers[i + 1]});
    }
    VELOX_USER_CHECK(
        ring.points.front().x == ring.points.back().x &&
            ring.points.front().y == ring.points.back().y,
        "POLYGON WKT ring must be closed");
    rings.push_back(std::move(ring));
    index = close + 1;
  }
  VELOX_USER_CHECK(!rings.empty(), "POLYGON WKT requires at least one ring");
  return rings;
}

Geometry parseWkt(std::string_view input) {
  const auto text = trim(input);
  VELOX_USER_CHECK(!text.empty(), "WKT geometry text is empty");
  if (startsWithIgnoreCase(text, "point")) {
    const auto numbers = parseNumbers(text.substr(5));
    VELOX_USER_CHECK_EQ(numbers.size(), 2, "POINT WKT requires exactly two coordinates");
    Geometry geometry;
    geometry.kind = Geometry::Kind::POINT;
    geometry.point = {numbers[0], numbers[1]};
    return geometry;
  }
  if (startsWithIgnoreCase(text, "polygon")) {
    Geometry geometry;
    geometry.kind = Geometry::Kind::POLYGON;
    geometry.rings = parsePolygonRings(trim(text.substr(7)));
    return geometry;
  }
  VELOX_USER_FAIL(
      "Unsupported WKT geometry kind: only POINT and POLYGON are implemented");
}

void appendUint32(std::string& out, std::uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xff));
  }
}

void appendDouble(std::string& out, double value) {
  std::uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>((bits >> (8 * i)) & 0xff));
  }
}

std::string encodeWkb(const Geometry& geometry) {
  std::string out;
  out.push_back(1);  // little-endian byte order
  if (geometry.kind == Geometry::Kind::POINT) {
    appendUint32(out, 1);
    appendDouble(out, geometry.point.x);
    appendDouble(out, geometry.point.y);
    return out;
  }
  appendUint32(out, 3);
  appendUint32(out, static_cast<std::uint32_t>(geometry.rings.size()));
  for (const auto& ring : geometry.rings) {
    appendUint32(out, static_cast<std::uint32_t>(ring.points.size()));
    for (const auto& point : ring.points) {
      appendDouble(out, point.x);
      appendDouble(out, point.y);
    }
  }
  return out;
}

class WkbReader {
 public:
  explicit WkbReader(std::string_view input) : input_(input) {}

  std::uint32_t uint32(std::size_t offset) const {
    VELOX_USER_CHECK_LE(offset + 4, input_.size(), "WKB geometry is truncated");
    std::uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
      const auto byte = static_cast<unsigned char>(input_[offset + (little_ ? i : 3 - i)]);
      value |= static_cast<std::uint32_t>(byte) << (8 * i);
    }
    return value;
  }

  double number(std::size_t offset) const {
    VELOX_USER_CHECK_LE(offset + 8, input_.size(), "WKB geometry is truncated");
    std::uint64_t bits = 0;
    for (int i = 0; i < 8; ++i) {
      const auto byte = static_cast<unsigned char>(input_[offset + (little_ ? i : 7 - i)]);
      bits |= static_cast<std::uint64_t>(byte) << (8 * i);
    }
    double value = 0;
    std::memcpy(&value, &bits, sizeof(value));
    VELOX_USER_CHECK(std::isfinite(value), "WKB geometry coordinate must be finite");
    return value;
  }

  void readByteOrder() {
    VELOX_USER_CHECK_GE(input_.size(), 1, "WKB geometry is empty");
    const auto order = static_cast<unsigned char>(input_.front());
    VELOX_USER_CHECK(order == 0 || order == 1, "Invalid WKB byte order");
    little_ = order == 1;
  }

  std::size_t size() const {
    return input_.size();
  }

 private:
  std::string_view input_;
  bool little_{true};
};

Geometry parseWkb(std::string_view input) {
  WkbReader reader(input);
  reader.readByteOrder();
  const auto type = reader.uint32(1);
  Geometry geometry;
  if (type == 1) {
    VELOX_USER_CHECK_GE(reader.size(), 1 + 4 + 16, "WKB POINT is truncated");
    geometry.kind = Geometry::Kind::POINT;
    geometry.point = {reader.number(5), reader.number(13)};
    return geometry;
  }
  if (type == 3) {
    geometry.kind = Geometry::Kind::POLYGON;
    const auto ringCount = reader.uint32(5);
    std::size_t offset = 9;
    for (std::uint32_t ringIndex = 0; ringIndex < ringCount; ++ringIndex) {
      const auto pointCount = reader.uint32(offset);
      offset += 4;
      VELOX_USER_CHECK_GE(pointCount, 4, "WKB POLYGON ring requires at least four points");
      Ring ring;
      ring.points.reserve(pointCount);
      for (std::uint32_t pointIndex = 0; pointIndex < pointCount; ++pointIndex) {
        ring.points.push_back({reader.number(offset), reader.number(offset + 8)});
        offset += 16;
      }
      geometry.rings.push_back(std::move(ring));
    }
    return geometry;
  }
  VELOX_USER_FAIL(
      "Unsupported WKB geometry type {}: only POINT and POLYGON are implemented",
      type);
}

bool samePoint(const Point& left, const Point& right) {
  return left.x == right.x && left.y == right.y;
}

bool pointOnSegment(const Point& point, const Point& start, const Point& end) {
  const double cross = (end.x - start.x) * (point.y - start.y) -
      (end.y - start.y) * (point.x - start.x);
  if (std::abs(cross) > 1e-9) {
    return false;
  }
  return point.x >= std::min(start.x, end.x) - 1e-12 &&
      point.x <= std::max(start.x, end.x) + 1e-12 &&
      point.y >= std::min(start.y, end.y) - 1e-12 &&
      point.y <= std::max(start.y, end.y) + 1e-12;
}

Position locateInRing(const Point& point, const Ring& ring) {
  bool inside = false;
  for (std::size_t i = 0; i + 1 < ring.points.size(); ++i) {
    const auto& start = ring.points[i];
    const auto& end = ring.points[i + 1];
    if (pointOnSegment(point, start, end)) {
      return Position::BOUNDARY;
    }
    const bool crosses = (start.y > point.y) != (end.y > point.y);
    if (crosses) {
      const double intersectionX =
          (end.x - start.x) * (point.y - start.y) / (end.y - start.y) + start.x;
      if (point.x < intersectionX) {
        inside = !inside;
      }
    }
  }
  return inside ? Position::INSIDE : Position::OUTSIDE;
}

Position locateInPolygon(const Point& point, const Geometry& polygon) {
  VELOX_USER_CHECK(!polygon.rings.empty(), "POLYGON geometry has no rings");
  const auto outer = locateInRing(point, polygon.rings.front());
  if (outer != Position::INSIDE) {
    return outer;
  }
  for (std::size_t i = 1; i < polygon.rings.size(); ++i) {
    const auto hole = locateInRing(point, polygon.rings[i]);
    if (hole == Position::INSIDE) {
      return Position::OUTSIDE;
    }
    if (hole == Position::BOUNDARY) {
      return Position::BOUNDARY;
    }
  }
  return Position::INSIDE;
}

Position locate(const Point& point, const Geometry& geometry) {
  if (geometry.kind == Geometry::Kind::POINT) {
    return samePoint(point, geometry.point) ? Position::BOUNDARY : Position::OUTSIDE;
  }
  return locateInPolygon(point, geometry);
}

bool pointWithin(const Point& point, const Geometry& geometry) {
  return locate(point, geometry) == Position::INSIDE;
}

bool pointIntersects(const Point& point, const Geometry& geometry) {
  return locate(point, geometry) != Position::OUTSIDE;
}

bool evaluate(SpatialOp op, const Geometry& left, const Geometry& right) {
  const bool leftPoint = left.kind == Geometry::Kind::POINT;
  const bool rightPoint = right.kind == Geometry::Kind::POINT;
  VELOX_USER_CHECK(
      leftPoint != rightPoint,
      "Spatial predicates require one POINT and one POLYGON operand");
  if (op == SpatialOp::WITHIN) {
    return leftPoint && pointWithin(left.point, right);
  }
  if (op == SpatialOp::CONTAINS) {
    return rightPoint && pointWithin(right.point, left);
  }
  return leftPoint ? pointIntersects(left.point, right)
                   : pointIntersects(right.point, left);
}

class GeomFromTextFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decoded(rows, args, context);
    const auto& input = *decoded.at(0);
    auto local = BaseVector::create(VARBINARY(), rows.end(), context.pool());
    auto flat = local->asFlatVector<StringView>();
    StringVectorBuffer buffer(flat, 256, 1U << 24);
    rows.applyToSelected([&](vector_size_t row) {
      if (input.isNullAt(row)) {
        local->setNull(row, true);
        return;
      }
      const StringView text = input.valueAt<StringView>(row);
      const auto wkb = encodeWkb(parseWkt(static_cast<std::string_view>(text)));
      for (const char byte : wkb) {
        buffer.appendByte(static_cast<int8_t>(byte));
      }
      buffer.flushRow(row);
    });
    context.moveOrCopyResult(local, rows, result);
  }
};

class SpatialPredicateFunction final : public exec::VectorFunction {
 public:
  explicit SpatialPredicateFunction(SpatialOp op) : op_(op) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decoded(rows, args, context);
    const auto& left = *decoded.at(0);
    const auto& right = *decoded.at(1);
    auto local = BaseVector::create(BOOLEAN(), rows.end(), context.pool());
    auto flat = local->asFlatVector<bool>();
    rows.applyToSelected([&](vector_size_t row) {
      if (left.isNullAt(row) || right.isNullAt(row)) {
        local->setNull(row, true);
        return;
      }
      const StringView leftValue = left.valueAt<StringView>(row);
      const StringView rightValue = right.valueAt<StringView>(row);
      flat->set(
          row,
          evaluate(
              op_,
              parseWkb(static_cast<std::string_view>(leftValue)),
              parseWkb(static_cast<std::string_view>(rightValue))));
    });
    context.moveOrCopyResult(local, rows, result);
  }

 private:
  SpatialOp op_;
};

std::vector<exec::FunctionSignaturePtr> binaryPredicateSignature() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varbinary")
              .argumentType("varbinary")
              .build()};
}

}  // namespace

void registerSpatialFunctions() {
  const std::vector<exec::FunctionSignaturePtr> constructorSignature{
      exec::FunctionSignatureBuilder()
          .returnType("varbinary")
          .argumentType("varchar")
          .build()};
  exec::registerVectorFunction(
      "st_geomfromtext",
      constructorSignature,
      std::make_unique<GeomFromTextFunction>());
  // GEOGRAPHY shares the WKB representation; predicates evaluate planar
  // coordinates until a distinct geodesic operator exists.
  exec::registerVectorFunction(
      "st_geographyfromtext",
      constructorSignature,
      std::make_unique<GeomFromTextFunction>());
  exec::registerVectorFunction(
      "st_within",
      binaryPredicateSignature(),
      std::make_unique<SpatialPredicateFunction>(SpatialOp::WITHIN));
  exec::registerVectorFunction(
      "st_intersects",
      binaryPredicateSignature(),
      std::make_unique<SpatialPredicateFunction>(SpatialOp::INTERSECTS));
  exec::registerVectorFunction(
      "st_contains",
      binaryPredicateSignature(),
      std::make_unique<SpatialPredicateFunction>(SpatialOp::CONTAINS));
}

}  // namespace pangrova::spatial
