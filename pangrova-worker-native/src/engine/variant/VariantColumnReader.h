// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// VariantColumnReader: Reads Variant columns from Parquet Shredded encoding.
//
// Iceberg v3 Variant type uses Parquet's Shredded encoding where a Variant
// column is stored as a group with two sub-columns:
//   - `metadata` (BINARY): binary-encoded type metadata
//   - `value`    (BINARY): binary-encoded value payload (JSON-compatible)
//
// This reader reconstructs the Variant value from the shredded sub-columns
// and produces a VARCHAR vector containing the JSON string representation.
// The JSON output is suitable for downstream path expression evaluation
// via the variant_get() scalar UDF.
//
// Integration: Registered in the connector's reader factory chain via
// PangrovaVeloxConnector. When the output schema contains a VARIANT column,
// the reader maps it to VARCHAR and applies shredded decoding.
//
// Placement: variant/ — Variant type encoding/decoding components.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "engine/compat/NativeFeatureDetection.h"

#if PANGROVA_HAS_VELOX_TYPE
#include "velox/type/Type.h"
#endif

#if PANGROVA_HAS_VELOX_DWIO
#include "velox/vector/BaseVector.h"
#include "velox/vector/FlatVector.h"
#endif

namespace pangrova::variant {

/// Decodes a single Variant value from Parquet Shredded encoding sub-columns.
///
/// The metadata bytes encode the type information (header byte + optional
/// type-specific metadata). The value bytes encode the actual payload.
/// Returns the JSON string representation of the Variant value.
///
/// Supported types (Iceberg Variant spec):
///   - Primitive: null, boolean, int8-int64, float, double, decimal, date,
///     timestamptz, timestampntz, binary, string
///   - Complex: object (shredded key-value pairs), array
///
/// For unrecognized metadata headers, returns the raw value bytes as a
/// hex-escaped string to avoid data loss.
std::string decodeVariantValue(
    const char* metadataBytes,
    size_t metadataLen,
    const char* valueBytes,
    size_t valueLen);

/// Decodes a batch of Variant shredded sub-column pairs into JSON strings.
///
/// This is the SIMD-friendly batch interface. For each row index in
/// [0, count), reads metadata and value from the corresponding positions
/// in the input arrays and produces a JSON string.
///
/// Null handling: if either metadata or value is null for a given row,
/// the output for that row is empty and the caller should set the
/// output vector's null bit.
///
/// @param metadataBuffers  Array of (pointer, length) pairs for metadata
/// @param valueBuffers     Array of (pointer, length) pairs for values
/// @param count            Number of rows to decode
/// @param output           Pre-allocated output vector for JSON strings
void decodeVariantBatch(
    const std::vector<std::pair<const char*, size_t>>& metadataBuffers,
    const std::vector<std::pair<const char*, size_t>>& valueBuffers,
    size_t count,
    std::vector<std::string>& output);

#if PANGROVA_HAS_VELOX_DWIO
/// Converts a Velox RowVector containing shredded Variant sub-columns
/// (metadata + value) into a FlatVector<StringView> containing JSON strings.
///
/// This function is called from PangrovaVeloxDataSource::applyPostProcessing()
/// when the output schema contains VARIANT columns. The Parquet reader
/// produces a RowVector with the shredded group structure; this function
/// flattens it into a single VARCHAR column.
///
/// @param shreddedColumn  RowVector with children [metadata, value]
/// @param pool            Memory pool for output allocation
/// @return FlatVector<StringView> containing JSON representations
facebook::velox::VectorPtr reconstructVariantColumn(
    const facebook::velox::VectorPtr& shreddedColumn,
    facebook::velox::memory::MemoryPool* pool);
#endif

}  // namespace pangrova::variant
