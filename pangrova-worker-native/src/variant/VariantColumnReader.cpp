// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// VariantColumnReader: Decodes Variant Shredded encoding from Parquet.
//
// Iceberg Variant binary format (spec v1):
//   metadata[0] = basic_type (bits 0-1) | physical_type (bits 2-7)
//   basic_type: 0=Primitive, 1=ShortString, 2=Object, 3=Array
//
// Primitive physical_type values:
//   0=null, 1=true, 2=false, 3=int8, 4=int16, 5=int32, 6=int64,
//   7=double, 8=decimal4, 9=decimal8, 10=decimal16, 11=date,
//   12=timestamptz, 13=timestampntz, 14=float, 15=binary, 16=string
//
// ShortString: metadata[0] bits 2-7 encode string length (0-63).
//   value bytes contain the raw UTF-8 string.
//
// For the initial implementation, Variant columns stored as JSON strings
// in Parquet BINARY columns are passed through directly. The shredded
// sub-column reconstruction handles the metadata+value pair format.

#include "variant/VariantColumnReader.h"

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "observability/PangrovaLog.h"

#if PANGROVA_HAS_VELOX_DWIO
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#endif

namespace pangrova::variant {

namespace {

// Variant basic type (bits 0-1 of header byte).
enum class BasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
  kObject = 2,
  kArray = 3,
};

// Variant primitive physical type (bits 2-7 of header byte when basic=0).
enum class PrimitiveType : uint8_t {
  kNull = 0,
  kTrue = 1,
  kFalse = 2,
  kInt8 = 3,
  kInt16 = 4,
  kInt32 = 5,
  kInt64 = 6,
  kDouble = 7,
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  kDate = 11,
  kTimestampTz = 12,
  kTimestampNtz = 13,
  kFloat = 14,
  kBinary = 15,
  kString = 16,
};

// Escape a string for JSON output. Handles control characters and
// double-quote / backslash escaping per RFC 8259.
std::string jsonEscapeString(const char* data, size_t len) {
  std::string result;
  result.reserve(len + 2);
  result.push_back('"');
  for (size_t i = 0; i < len; ++i) {
    char c = data[i];
    switch (c) {
      case '"':
        result.append("\\\"");
        break;
      case '\\':
        result.append("\\\\");
        break;
      case '\b':
        result.append("\\b");
        break;
      case '\f':
        result.append("\\f");
        break;
      case '\n':
        result.append("\\n");
        break;
      case '\r':
        result.append("\\r");
        break;
      case '\t':
        result.append("\\t");
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(c));
          result.append(buf);
        } else {
          result.push_back(c);
        }
        break;
    }
  }
  result.push_back('"');
  return result;
}

// Encode binary data as a hex string for JSON output.
std::string hexEncode(const char* data, size_t len) {
  static const char kHexDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(len * 2 + 2);
  result.push_back('"');
  for (size_t i = 0; i < len; ++i) {
    auto byte = static_cast<uint8_t>(data[i]);
    result.push_back(kHexDigits[byte >> 4]);
    result.push_back(kHexDigits[byte & 0x0F]);
  }
  result.push_back('"');
  return result;
}

// Read a little-endian integer from a byte buffer.
template <typename T>
T readLittleEndian(const char* data) {
  T value;
  std::memcpy(&value, data, sizeof(T));
  return value;
}

// Format a 16-byte little-endian two's-complement integer exactly.
// Variant decimal primitives carry only the unscaled value, so the scale
// must be applied by the reader using the column's declared type.
std::string decimal128ToString(const char* data) {
  const bool negative = (static_cast<uint8_t>(data[15]) & 0x80U) != 0;
  std::array<uint8_t, 16> magnitude{};
  if (negative) {
    unsigned carry = 1;
    for (size_t i = 0; i < magnitude.size(); ++i) {
      unsigned value =
          static_cast<uint8_t>(~static_cast<uint8_t>(data[i])) + carry;
      magnitude[i] = static_cast<uint8_t>(value & 0xFFU);
      carry = value >> 8;
    }
  } else {
    for (size_t i = 0; i < magnitude.size(); ++i) {
      magnitude[i] = static_cast<uint8_t>(data[i]);
    }
  }

  // Base-256 to base-10 conversion into a little-endian digit array.
  std::vector<uint8_t> digits{0};
  for (auto it = magnitude.rbegin(); it != magnitude.rend(); ++it) {
    unsigned carry = *it;
    for (auto& digit : digits) {
      unsigned value = static_cast<unsigned>(digit) * 256U + carry;
      digit = static_cast<uint8_t>(value % 10U);
      carry = value / 10U;
    }
    while (carry != 0) {
      digits.push_back(static_cast<uint8_t>(carry % 10U));
      carry /= 10U;
    }
  }

  std::string result;
  if (negative) {
    result.push_back('-');
  }
  for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
    result.push_back(static_cast<char>('0' + *it));
  }
  return result;
}

// Decode a primitive Variant value to its JSON string representation.
std::string decodePrimitive(
    PrimitiveType physicalType,
    const char* valueBytes,
    size_t valueLen) {
  switch (physicalType) {
    case PrimitiveType::kNull:
      return "null";
    case PrimitiveType::kTrue:
      return "true";
    case PrimitiveType::kFalse:
      return "false";
    case PrimitiveType::kInt8:
      if (valueLen < 1) {
        return "null";
      }
      return std::to_string(static_cast<int8_t>(valueBytes[0]));
    case PrimitiveType::kInt16:
      if (valueLen < 2) {
        return "null";
      }
      return std::to_string(readLittleEndian<int16_t>(valueBytes));
    case PrimitiveType::kInt32:
      if (valueLen < 4) {
        return "null";
      }
      return std::to_string(readLittleEndian<int32_t>(valueBytes));
    case PrimitiveType::kInt64:
      if (valueLen < 8) {
        return "null";
      }
      return std::to_string(readLittleEndian<int64_t>(valueBytes));
    case PrimitiveType::kFloat: {
      if (valueLen < 4) {
        return "null";
      }
      float f = readLittleEndian<float>(valueBytes);
      if (std::isnan(f) || std::isinf(f)) {
        return "null";
      }
      std::ostringstream oss;
      oss << f;
      return oss.str();
    }
    case PrimitiveType::kDouble: {
      if (valueLen < 8) {
        return "null";
      }
      double d = readLittleEndian<double>(valueBytes);
      if (std::isnan(d) || std::isinf(d)) {
        return "null";
      }
      std::ostringstream oss;
      oss << d;
      return oss.str();
    }
    case PrimitiveType::kDate:
      if (valueLen < 4) {
        return "null";
      }
      return std::to_string(readLittleEndian<int32_t>(valueBytes));
    case PrimitiveType::kTimestampTz:
    case PrimitiveType::kTimestampNtz:
      if (valueLen < 8) {
        return "null";
      }
      return std::to_string(readLittleEndian<int64_t>(valueBytes));
    case PrimitiveType::kString:
      return jsonEscapeString(valueBytes, valueLen);
    case PrimitiveType::kBinary:
      return hexEncode(valueBytes, valueLen);
    case PrimitiveType::kDecimal4:
      if (valueLen < 4) {
        return "null";
      }
      return std::to_string(readLittleEndian<int32_t>(valueBytes));
    case PrimitiveType::kDecimal8:
      if (valueLen < 8) {
        return "null";
      }
      return std::to_string(readLittleEndian<int64_t>(valueBytes));
    case PrimitiveType::kDecimal16:
      if (valueLen < 16) {
        return "null";
      }
      return decimal128ToString(valueBytes);
  }
  return "null";
}

}  // namespace

std::string decodeVariantValue(
    const char* metadataBytes,
    size_t metadataLen,
    const char* valueBytes,
    size_t valueLen) {
  if (metadataLen == 0) {
    // No metadata — treat the value bytes as a raw JSON string.
    // This handles the common case where Variant is stored as plain
    // JSON in a single BINARY column (non-shredded encoding).
    if (valueLen == 0) {
      return "null";
    }
    return std::string(valueBytes, valueLen);
  }

  uint8_t header = static_cast<uint8_t>(metadataBytes[0]);
  auto basicType = static_cast<BasicType>(header & 0x03);
  uint8_t typeInfo = header >> 2;

  switch (basicType) {
    case BasicType::kPrimitive:
      return decodePrimitive(
          static_cast<PrimitiveType>(typeInfo), valueBytes, valueLen);

    case BasicType::kShortString: {
      // Short string: typeInfo encodes the string length (0-63).
      size_t strLen = typeInfo;
      if (strLen > valueLen) {
        strLen = valueLen;
      }
      return jsonEscapeString(valueBytes, strLen);
    }

    case BasicType::kObject:
    case BasicType::kArray:
      // Complex types: the value bytes contain the serialized JSON
      // representation of the object/array. Pass through directly.
      if (valueLen == 0) {
        return basicType == BasicType::kObject ? "{}" : "[]";
      }
      return std::string(valueBytes, valueLen);
  }

  // Unrecognized header — return raw value as hex to avoid data loss.
  if (valueLen > 0) {
    return hexEncode(valueBytes, valueLen);
  }
  return "null";
}

void decodeVariantBatch(
    const std::vector<std::pair<const char*, size_t>>& metadataBuffers,
    const std::vector<std::pair<const char*, size_t>>& valueBuffers,
    size_t count,
    std::vector<std::string>& output) {
  output.resize(count);
  for (size_t i = 0; i < count; ++i) {
    const auto& [metaPtr, metaLen] = metadataBuffers[i];
    const auto& [valPtr, valLen] = valueBuffers[i];
    if (metaPtr == nullptr && valPtr == nullptr) {
      output[i].clear();
      continue;
    }
    output[i] = decodeVariantValue(
        metaPtr, metaLen,
        valPtr, valLen);
  }
}

#if PANGROVA_HAS_VELOX_DWIO
facebook::velox::VectorPtr reconstructVariantColumn(
    const facebook::velox::VectorPtr& shreddedColumn,
    facebook::velox::memory::MemoryPool* pool) {
  using namespace facebook::velox;

  // Case 1: Already a flat VARCHAR/VARBINARY — Variant stored as plain JSON.
  // This is the most common case for current Iceberg/Paimon implementations
  // where Variant is serialized as a JSON string in a single BINARY column.
  if (shreddedColumn->typeKind() == TypeKind::VARCHAR ||
      shreddedColumn->typeKind() == TypeKind::VARBINARY) {
    return shreddedColumn;
  }

  // Case 2: RowVector with shredded sub-columns [metadata, value].
  auto* rowVector = shreddedColumn->as<RowVector>();
  if (!rowVector || rowVector->childrenSize() < 2) {
    PANGROVA_LOG_WARN << "variant_reconstruct_unexpected_structure"
                    << " type_kind=" << static_cast<int>(shreddedColumn->typeKind())
                    << " children=" << (rowVector ? rowVector->childrenSize() : 0);
    return shreddedColumn;
  }

  auto metadataVec = rowVector->childAt(0);
  auto valueVec = rowVector->childAt(1);
  auto numRows = static_cast<vector_size_t>(rowVector->size());

  auto result = BaseVector::create(VARCHAR(), numRows, pool);
  auto* flatResult = result->asFlatVector<StringView>();

  auto* metaFlat = metadataVec->asFlatVector<StringView>();
  auto* valFlat = valueVec->asFlatVector<StringView>();

  for (vector_size_t row = 0; row < numRows; ++row) {
    bool metaNull = metadataVec->isNullAt(row);
    bool valNull = valueVec->isNullAt(row);

    if (metaNull && valNull) {
      flatResult->setNull(row, true);
      continue;
    }

    const char* metaPtr = nullptr;
    size_t metaLen = 0;
    const char* valPtr = nullptr;
    size_t valLen = 0;

    if (!metaNull && metaFlat) {
      auto sv = metaFlat->valueAt(row);
      metaPtr = sv.data();
      metaLen = sv.size();
    }
    if (!valNull && valFlat) {
      auto sv = valFlat->valueAt(row);
      valPtr = sv.data();
      valLen = sv.size();
    }

    std::string json = decodeVariantValue(metaPtr, metaLen, valPtr, valLen);
    flatResult->set(row, StringView(json));
  }

  return result;
}
#endif

}  // namespace pangrova::variant
