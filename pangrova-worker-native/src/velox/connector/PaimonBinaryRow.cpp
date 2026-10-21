#include "velox/connector/PaimonBinaryRow.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "velox/type/DecimalUtil.h"
#include "velox/connector/PaimonFloatingText.h"
#include "velox/type/Timestamp.h"

namespace pangrova::velox {
namespace {
using Kind = PaimonScalarType::Kind;
using facebook::velox::int128_t;
using facebook::velox::uint128_t;

std::string trim(std::string_view text) {
  while (!text.empty() && text.front() == ' ') text.remove_prefix(1);
  while (!text.empty() && text.back() == ' ') text.remove_suffix(1);
  return std::string(text);
}

int parameter(std::string_view value) {
  auto text = trim(value);
  int result = 0;
  auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
  if (error != std::errc{} || end != text.data() + text.size() || result < 0 || result > 38)
    throw std::runtime_error("Invalid Paimon scalar type parameter");
  return result;
}

std::string decimalText(int128_t value, int scale) {
  std::string digits = std::to_string(value);
  bool negative = digits.front() == '-';
  if (negative) digits.erase(0, 1);
  // Paimon Decimal.toString delegates to BigDecimal.toPlainString, including
  // small magnitudes and the complete declared scale.
  if (scale > 0) {
    if (digits.size() <= static_cast<size_t>(scale)) digits.insert(0, scale + 1 - digits.size(), '0');
    digits.insert(digits.size() - scale, 1, '.');
  }
  return (negative ? "-" : "") + digits;
}

std::string timestampText(int64_t millis, uint32_t nanos, bool partition = false, int precision = 0) {
  auto value = facebook::velox::Timestamp::fromMillis(millis);
  value = facebook::velox::Timestamp(value.getSeconds(), value.getNanos() + nanos);
  facebook::velox::TimestampToStringOptions options;
  options.zeroPaddingYear = true;
  options.leadingPositiveSign = true;
  options.dateTimeSeparator = partition ? ' ' : 'T';
  auto text = value.toString(options);
  const auto fraction = text.rfind('.');
  if (fraction == std::string::npos) throw std::runtime_error("Cannot render Paimon timestamp fraction");
  if (partition) {
    while (text.size() - fraction - 1 > static_cast<size_t>(precision) && text.ends_with('0')) text.pop_back();
    if (text.ends_with('.')) text.pop_back();
  } else if (value.getNanos() == 0) {
    text.erase(fraction);
    if (text.ends_with(":00")) text.resize(text.size() - 3);
  } else {
    const int digits = value.getNanos() % 1'000'000 == 0 ? 3 : value.getNanos() % 1'000 == 0 ? 6 : 9;
    text.resize(fraction + 1 + digits);
  }
  return text;
}

std::string timeText(int32_t millis, int precision) {
  int64_t time = millis;
  while (time < 0) time += 86'400'000;
  auto two = [](int64_t value) { return (value < 10 ? "0" : "") + std::to_string(value); };
  std::string text = two(time / 3'600'000) + ":" + two(time / 60'000 % 60) + ":" + two(time / 1'000 % 60);
  int fraction = time % 1'000;
  if (precision > 0) {
    text += '.';
    while (precision-- > 0) {
      text += static_cast<char>('0' + fraction / 100);
      fraction = fraction % 100 * 10;
      if (fraction == 0) break;
    }
  }
  return text;
}

}  // namespace

PaimonScalarType PaimonScalarType::parse(std::string_view declaration) {
  auto text = trim(declaration);
  auto suffix = text.find(" NOT NULL");
  if (suffix != std::string::npos) text.erase(suffix);
  auto open = text.find('(');
  auto name = text.substr(0, open);
  PaimonScalarType type{Kind::COMPLEX};
  if (name == "BOOLEAN") type.kind = Kind::BOOLEAN;
  else if (name == "TINYINT") type.kind = Kind::TINYINT;
  else if (name == "SMALLINT") type.kind = Kind::SMALLINT;
  else if (name == "INT" || name == "INTEGER") type.kind = Kind::INTEGER;
  else if (name == "BIGINT") type.kind = Kind::BIGINT;
  else if (name == "FLOAT") type.kind = Kind::FLOAT;
  else if (name == "DOUBLE") type.kind = Kind::DOUBLE;
  else if (name == "STRING" || name == "VARCHAR" || name == "CHAR") type.kind = Kind::STRING;
  else if (name == "BYTES" || name == "VARBINARY" || name == "BINARY") type.kind = Kind::BINARY;
  else if (name == "DATE") type.kind = Kind::DATE;
  else if (name == "TIME") type.kind = Kind::TIME;
  else if (name == "DECIMAL") type.kind = Kind::DECIMAL;
  else if (name == "TIMESTAMP" || name == "TIMESTAMP_LTZ") type.kind = Kind::TIMESTAMP;
  if (type.kind == Kind::DECIMAL || type.kind == Kind::TIMESTAMP || type.kind == Kind::TIME) {
    type.precision = type.kind == Kind::TIMESTAMP ? 6 : 0;
    if (open != std::string::npos) {
      auto close = text.find(')', open);
      if (close == std::string::npos) throw std::runtime_error("Invalid Paimon type declaration");
      auto parameters = std::string_view(text).substr(open + 1, close - open - 1);
      auto comma = parameters.find(',');
      type.precision = parameter(parameters.substr(0, comma));
      if (comma != std::string::npos) type.scale = parameter(parameters.substr(comma + 1));
    }
    if ((type.kind == Kind::DECIMAL && (type.precision == 0 || type.scale > type.precision)) ||
        ((type.kind == Kind::TIMESTAMP || type.kind == Kind::TIME) && type.precision > 9))
      throw std::runtime_error("Invalid Paimon scalar precision");
  }
  return type;
}

PaimonBinaryRowView::PaimonBinaryRowView(std::string_view serialized) {
  if (serialized.size() < 4) throw std::runtime_error("Truncated Paimon BinaryRow arity");
  uint32_t arity = 0;
  for (size_t index = 0; index < 4; ++index) arity = (arity << 8) | static_cast<uint8_t>(serialized[index]);
  if (arity > 1'000'000) throw std::runtime_error("Paimon BinaryRow exceeds field budget");
  arity_ = arity;
  data_ = serialized.substr(4);
  null_bytes_ = ((arity + 71) / 64) * 8;
  if (arity != 0 && data_.size() < null_bytes_ + arity * uint64_t{8})
    throw std::runtime_error("Truncated Paimon BinaryRow fixed region");
}

uint64_t PaimonBinaryRowView::little(size_t offset, size_t width) const {
  if (offset > data_.size() || width > data_.size() - offset || width > 8)
    throw std::runtime_error("Paimon BinaryRow offset exceeds payload");
  uint64_t value = 0;
  for (size_t index = 0; index < width; ++index)
    value |= static_cast<uint64_t>(static_cast<uint8_t>(data_[offset + index])) << (index * 8);
  return value;
}

uint64_t PaimonBinaryRowView::slot(int index) const {
  if (index < 0 || index >= arity_) throw std::runtime_error("Paimon BinaryRow field exceeds arity");
  return little(null_bytes_ + static_cast<size_t>(index) * 8, 8);
}

std::string_view PaimonBinaryRowView::variable(int index, bool inlineAllowed) const {
  auto encoded = slot(index);
  size_t offset, size;
  if (inlineAllowed && (encoded & (uint64_t{1} << 63))) {
    size = (encoded >> 56) & 0x7f;
    if (size > 7) throw std::runtime_error("Invalid inline Paimon binary length");
    offset = null_bytes_ + static_cast<size_t>(index) * 8;
  } else {
    offset = encoded >> 32;
    size = encoded & 0xffffffff;
    if (offset < null_bytes_ + static_cast<size_t>(arity_) * 8)
      throw std::runtime_error("Paimon variable value overlaps fixed region");
  }
  if (offset > data_.size() || size > data_.size() - offset)
    throw std::runtime_error("Paimon variable value exceeds payload");
  return data_.substr(offset, size);
}

std::optional<std::string> PaimonBinaryRowView::text(int index, const PaimonScalarType& source,
    const PaimonScalarType& target, bool partitionCast) const {
  if (index < 0) return std::nullopt;
  auto raw = slot(index);
  const auto nullBit = static_cast<size_t>(index) + 8;
  if ((little(nullBit / 8, 1) & (uint8_t{1} << (nullBit % 8))) != 0) return std::nullopt;
  auto integer = [&](size_t width) -> int64_t {
    const auto bits = little(null_bytes_ + static_cast<size_t>(index) * 8, width);
    if (width == 1) return static_cast<int8_t>(bits);
    if (width == 2) return static_cast<int16_t>(bits);
    if (width == 4) return static_cast<int32_t>(bits);
    return std::bit_cast<int64_t>(bits);
  };
  auto integerText = [&](int64_t value) {
    if (target.kind == Kind::FLOAT) return paimonFloatingText(static_cast<float>(value));
    if (target.kind == Kind::DOUBLE) return paimonFloatingText(static_cast<double>(value));
    if (target.kind == Kind::DECIMAL) {
      int128_t scaled;
      if (__builtin_mul_overflow(static_cast<int128_t>(value),
          facebook::velox::DecimalUtil::kPowersOfTen[target.scale], &scaled))
        throw std::runtime_error("Paimon metadata integer to decimal evolution overflows");
      return decimalText(scaled, target.scale);
    }
    return std::to_string(value);
  };
  switch (source.kind) {
    case Kind::BOOLEAN:
      if ((raw & 0xff) > 1) throw std::runtime_error("Invalid Paimon boolean");
      return (raw & 0xff) ? "true" : "false";
    case Kind::TINYINT: return integerText(integer(1));
    case Kind::SMALLINT: return integerText(integer(2));
    case Kind::INTEGER: return integerText(integer(4));
    case Kind::BIGINT: return integerText(integer(8));
    case Kind::DATE: {
      auto days = integer(4);
      if (!partitionCast) return std::to_string(days);
      auto value = timestampText(days * int64_t{86'400'000}, 0);
      return value.substr(0, value.find_first_of("T "));
    }
    case Kind::TIME: return partitionCast ? timeText(integer(4), source.precision) : std::to_string(integer(4));
    case Kind::FLOAT: {
      auto value = std::bit_cast<float>(static_cast<uint32_t>(raw));
      return target.kind == Kind::DOUBLE ? paimonFloatingText(static_cast<double>(value)) : paimonFloatingText(value);
    }
    case Kind::DOUBLE: return paimonFloatingText(std::bit_cast<double>(raw));
    case Kind::STRING: return std::string(variable(index));
    case Kind::BINARY: {
      constexpr char hex[] = "0123456789abcdef";
      std::string value = "0x";
      for (uint8_t byte : variable(index)) { value += hex[byte >> 4]; value += hex[byte & 0xf]; }
      return value;
    }
    case Kind::DECIMAL: return decimalValueText(index, source, target);
    case Kind::TIMESTAMP: return timestampValueText(index, source, partitionCast);
    default: throw std::runtime_error("Unsupported non-null Paimon metadata scalar");
  }
}

std::string PaimonBinaryRowView::decimalValueText(int index, const PaimonScalarType& source,
    const PaimonScalarType& target) const {
  int128_t value;
  if (source.precision <= 18) value = std::bit_cast<int64_t>(slot(index));
  else {
    auto bytes = variable(index, false);
    if (bytes.empty() || bytes.size() > 16) throw std::runtime_error("Invalid Paimon decimal payload");
    uint128_t bits = 0;
    for (uint8_t byte : bytes) bits = (bits << 8) | byte;
    if (static_cast<int8_t>(bytes.front()) < 0 && bytes.size() < 16)
      bits |= ~uint128_t{0} << (bytes.size() * 8);
    value = static_cast<int128_t>(bits);
  }
  const auto sourceLimit = facebook::velox::DecimalUtil::kPowersOfTen[source.precision];
  if (value <= -sourceLimit || value >= sourceLimit)
    throw std::runtime_error("Paimon decimal payload exceeds its declared precision");
  if (target.kind == Kind::FLOAT) return paimonFloatingText(static_cast<float>(std::stod(decimalText(value, source.scale))));
  if (target.kind == Kind::DOUBLE) return paimonFloatingText(std::stod(decimalText(value, source.scale)));
  if (target.scale < source.scale) throw std::runtime_error("Paimon metadata decimal evolution narrows scale");
  if (target.scale != source.scale) {
    int128_t scaled;
    if (__builtin_mul_overflow(value, facebook::velox::DecimalUtil::kPowersOfTen[target.scale - source.scale], &scaled))
      throw std::runtime_error("Paimon metadata decimal evolution overflows");
    value = scaled;
  }
  return decimalText(value, target.scale);
}

std::string PaimonBinaryRowView::timestampValueText(int index, const PaimonScalarType& source,
    bool partitionCast) const {
  const auto raw = slot(index);
  int64_t millis;
  uint32_t nanos = 0;
  if (source.precision <= 3) millis = std::bit_cast<int64_t>(raw);
  else {
    if ((raw >> 32) < null_bytes_ + static_cast<size_t>(arity_) * 8)
      throw std::runtime_error("Paimon timestamp overlaps fixed region");
    millis = std::bit_cast<int64_t>(little(raw >> 32, 8));
    nanos = raw & 0xffffffff;
    if (nanos > 999'999) throw std::runtime_error("Invalid Paimon timestamp fractional value");
  }
  return timestampText(millis, nanos, partitionCast, source.precision);
}

}  // namespace pangrova::velox
