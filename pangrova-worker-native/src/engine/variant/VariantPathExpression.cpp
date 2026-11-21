// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// VariantPathExpression: variant_get(variant, path) Velox VectorFunction.
//
// Uses simdjson for high-performance JSON parsing. The path expression is
// parsed once per batch (constant folding) or per row (dynamic paths).
// simdjson's on-demand API provides zero-copy access to JSON values,
// avoiding full DOM construction for simple path lookups.

#include "engine/variant/VariantPathExpression.h"

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "engine/observability/PangrovaLog.h"

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

// simdjson is fetched by CMake (FetchContent) and available at build time.
// Velox itself depends on simdjson, so it is always present.
#include "simdjson.h"

namespace pangrova::variant {

namespace {

using namespace facebook::velox;

/// A single segment in a parsed path expression.
/// Segments are either object key lookups or array index lookups.
struct PathSegment {
  enum class Kind { kKey, kIndex };
  Kind kind;
  std::string key;     // For kKey segments.
  int64_t index = 0;   // For kIndex segments.
};

/// Parse a path expression string into a vector of PathSegments.
///
/// Supported syntax:
///   "name"           → [Key("name")]
///   "a.b.c"          → [Key("a"), Key("b"), Key("c")]
///   "items[0]"       → [Key("items"), Index(0)]
///   "items[0].name"  → [Key("items"), Index(0), Key("name")]
///   "['key']"        → [Key("key")]
///   "a['b'].c[1]"    → [Key("a"), Key("b"), Key("c"), Index(1)]
///
/// Returns an empty vector on parse error.
std::vector<PathSegment> parsePath(const char* path, size_t pathLen) {
  std::vector<PathSegment> segments;
  size_t pos = 0;

  while (pos < pathLen) {
    char c = path[pos];

    if (c == '.') {
      // Skip leading or consecutive dots.
      ++pos;
      continue;
    }

    if (c == '[') {
      ++pos;
      if (pos >= pathLen) {
        return {};  // Malformed: unclosed bracket.
      }

      if (path[pos] == '\'' || path[pos] == '"') {
        // Bracket notation with quoted key: ['key'] or ["key"]
        char quote = path[pos];
        ++pos;
        size_t keyStart = pos;
        while (pos < pathLen && path[pos] != quote) {
          ++pos;
        }
        if (pos >= pathLen) {
          return {};  // Malformed: unclosed quote.
        }
        PathSegment seg;
        seg.kind = PathSegment::Kind::kKey;
        seg.key = std::string(path + keyStart, pos - keyStart);
        segments.push_back(std::move(seg));
        ++pos;  // Skip closing quote.
        if (pos < pathLen && path[pos] == ']') {
          ++pos;  // Skip closing bracket.
        }
      } else {
        // Array index: [0], [123]
        size_t numStart = pos;
        while (pos < pathLen && path[pos] >= '0' && path[pos] <= '9') {
          ++pos;
        }
        if (pos == numStart || pos >= pathLen || path[pos] != ']') {
          return {};  // Malformed: non-numeric index or unclosed bracket.
        }
        PathSegment seg;
        seg.kind = PathSegment::Kind::kIndex;
        seg.index = std::strtol(path + numStart, nullptr, 10);
        segments.push_back(std::move(seg));
        ++pos;  // Skip closing bracket.
      }
    } else {
      // Dot notation key: read until '.', '[', or end.
      size_t keyStart = pos;
      while (pos < pathLen && path[pos] != '.' && path[pos] != '[') {
        ++pos;
      }
      if (pos > keyStart) {
        PathSegment seg;
        seg.kind = PathSegment::Kind::kKey;
        seg.key = std::string(path + keyStart, pos - keyStart);
        segments.push_back(std::move(seg));
      }
    }
  }

  return segments;
}

/// Navigate a simdjson on-demand value through a sequence of path segments.
/// Returns the JSON string representation of the final value, or empty
/// string if the path does not exist.
std::string navigatePath(
    simdjson::ondemand::document& doc,
    const std::vector<PathSegment>& segments) {
  simdjson::ondemand::value current;

  // Get the root value from the document.
  auto rootResult = doc.get_value();
  if (rootResult.error()) {
    return {};
  }
  current = rootResult.value();

  for (size_t i = 0; i < segments.size(); ++i) {
    const auto& seg = segments[i];

    if (seg.kind == PathSegment::Kind::kKey) {
      // Object key lookup.
      auto objResult = current.get_object();
      if (objResult.error()) {
        return {};
      }
      auto fieldResult = objResult.value().find_field_unordered(seg.key);
      if (fieldResult.error()) {
        return {};
      }
      current = fieldResult.value();
    } else {
      // Array index lookup.
      auto arrResult = current.get_array();
      if (arrResult.error()) {
        return {};
      }
      int64_t idx = 0;
      bool found = false;
      for (auto element : arrResult.value()) {
        if (element.error()) {
          return {};
        }
        if (idx == seg.index) {
          current = element.value();
          found = true;
          break;
        }
        ++idx;
      }
      if (!found) {
        return {};
      }
    }
  }

  // Convert the final value to a JSON string.
  auto type = current.type();
  if (type.error()) {
    return {};
  }

  switch (type.value()) {
    case simdjson::ondemand::json_type::null:
      return "null";
    case simdjson::ondemand::json_type::boolean: {
      auto b = current.get_bool();
      return b.error() ? "" : (b.value() ? "true" : "false");
    }
    case simdjson::ondemand::json_type::number: {
      // simdjson 4.x: raw_json_token() returns std::string_view directly (no error).
      auto raw = current.raw_json_token();
      return std::string(raw);
    }
    case simdjson::ondemand::json_type::string: {
      auto s = current.get_string();
      if (s.error()) {
        return {};
      }
      return std::string(s.value());
    }
    case simdjson::ondemand::json_type::object:
    case simdjson::ondemand::json_type::array: {
      auto raw = current.raw_json();
      if (raw.error()) {
        return {};
      }
      return std::string(raw.value());
    }
    case simdjson::ondemand::json_type::unknown:
      return {};
  }
  return {};
}

/// Velox VectorFunction implementation for variant_get(variant, path).
class VariantGetFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto localResult = BaseVector::create(VARCHAR(), rows.end(), context.pool());
    auto* flatResult = localResult->asFlatVector<StringView>();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto& decodedVariant = *decodedArgs.at(0);
    auto& decodedPath = *decodedArgs.at(1);

    // Per-thread simdjson parser (not thread-safe, but Velox VectorFunction
    // apply() is called from a single thread per operator instance).
    simdjson::ondemand::parser parser;

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedVariant.isNullAt(row) || decodedPath.isNullAt(row)) {
        flatResult->setNull(row, true);
        return;
      }

      auto variantSv = decodedVariant.valueAt<StringView>(row);
      auto pathSv = decodedPath.valueAt<StringView>(row);

      // Parse the path expression.
      auto segments = parsePath(pathSv.data(), pathSv.size());
      if (segments.empty() && pathSv.size() > 0) {
        // Malformed path expression — return NULL.
        PANGROVA_LOG_WARN << "variant_path_parse_error"
                        << " path=" << std::string(pathSv.data(), pathSv.size())
                        << " error=malformed_path_expression";
        flatResult->setNull(row, true);
        return;
      }

      // Empty path — return the entire variant value.
      if (segments.empty()) {
        flatResult->set(row, variantSv);
        return;
      }

      // Parse JSON and navigate path.
      // simdjson requires the input buffer to have SIMDJSON_PADDING bytes
      // of padding after the content. Copy to a padded buffer.
      std::string padded(variantSv.data(), variantSv.size());
      padded.resize(padded.size() + simdjson::SIMDJSON_PADDING, '\0');

      auto docResult = parser.iterate(
          padded.data(), variantSv.size(), padded.size());
      if (docResult.error()) {
        flatResult->setNull(row, true);
        return;
      }

      auto& doc = docResult.value();
      std::string resultStr = navigatePath(doc, segments);

      if (resultStr.empty()) {
        flatResult->setNull(row, true);
      } else {
        flatResult->set(row, StringView(resultStr));
      }
    });

    context.moveOrCopyResult(localResult, rows, result);
  }
};

}  // namespace

void registerVariantPathFunctions() {
  auto signatures = std::vector<exec::FunctionSignaturePtr>{
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build()};

  exec::registerVectorFunction(
      "variant_get",
      std::move(signatures),
      std::make_unique<VariantGetFunction>());
}

}  // namespace pangrova::variant
