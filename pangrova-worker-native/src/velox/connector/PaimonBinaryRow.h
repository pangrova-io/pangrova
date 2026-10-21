#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace pangrova::velox {

struct PaimonScalarType {
  enum class Kind { BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
    STRING, BINARY, DECIMAL, DATE, TIME, TIMESTAMP, COMPLEX };
  Kind kind;
  int precision = 0;
  int scale = 0;
  static PaimonScalarType parse(std::string_view declaration);
};

// SDK SerializationUtils framing: big-endian arity followed by BinaryRow's
// little-endian fixed/variable regions. Every offset is checked before access.
class PaimonBinaryRowView final {
 public:
  explicit PaimonBinaryRowView(std::string_view serialized);
  int arity() const { return arity_; }
  std::optional<std::string> text(int index, const PaimonScalarType& source,
      const PaimonScalarType& target, bool partitionCast = false) const;

 private:
  std::string decimalValueText(int index, const PaimonScalarType& source,
      const PaimonScalarType& target) const;
  std::string timestampValueText(int index, const PaimonScalarType& source, bool partitionCast) const;
  uint64_t slot(int index) const;
  uint64_t little(size_t offset, size_t width) const;
  std::string_view variable(int index, bool inlineAllowed = true) const;
  std::string_view data_;
  int arity_;
  size_t null_bytes_;
};

}  // namespace pangrova::velox
