// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// VariantPathExpression: Velox scalar UDF for Variant path expression evaluation.
//
// Registers variant_get(variant VARCHAR, path VARCHAR) -> VARCHAR as a Velox
// VectorFunction. The planner rewrites payload.name and payload['key'] into
// variant_get(payload, 'name') and variant_get(payload, 'key') calls.
//
// Path syntax:
//   - Dot notation:    "name"         → top-level key "name"
//   - Bracket notation: "['key']"     → top-level key "key"
//   - Nested dot:      "a.b.c"       → nested path a → b → c
//   - Array index:     "items[0]"    → array element at index 0
//   - Mixed:           "items[0].name" → array element 0, then key "name"
//
// The variant argument is expected to be a JSON string (VARCHAR). The function
// parses the JSON, navigates the path, and returns the result as VARCHAR.
// Returns NULL if the path does not exist or the variant is not valid JSON.
//
// Error handling: NATIVE-0012 (Variant path expression parse error) is logged
// for malformed path expressions. Invalid JSON input returns NULL silently
// (common case for sparse data).
//
// Placement: variant/ — Variant type encoding/decoding components.
#pragma once

namespace pangrova::variant {

/// Register variant_get(variant, path) as a Velox scalar VectorFunction.
/// Must be called during runtime initialization (after Velox Presto
/// functions are registered, before query execution begins).
void registerVariantPathFunctions();

}  // namespace pangrova::variant
