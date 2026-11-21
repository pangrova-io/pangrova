#include "engine/velox/connector/PaimonFloatingText.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <optional>
#include <stdexcept>

#include "boost/multiprecision/cpp_int.hpp"

namespace pangrova::velox {
namespace {
using Integer = boost::multiprecision::cpp_int;

// IEEE binary64 needs at most 325 decimal scaling places. These process-wide
// immutable tables bound allocation and avoid rebuilding powers for each field.
const Integer& powerOfTen(unsigned exponent) {
  static const auto powers = [] {
    std::array<Integer, 350> values;
    values[0] = 1;
    for (size_t index = 1; index < values.size(); ++index) values[index] = values[index - 1] * 10;
    return values;
  }();
  return powers.at(exponent);
}

int powerOfFiveBits(unsigned exponent) {
  static const auto bits = [] {
    std::array<int, 350> values{};
    Integer power = 1;
    for (size_t index = 1; index < values.size(); ++index) {
      power *= 5;
      values[index] = static_cast<int>(boost::multiprecision::msb(power)) + 1;
    }
    return values;
  }();
  return bits.at(exponent);
}

struct Ratio {
  Integer numerator;
  Integer denominator;
};

struct BinaryValue {
  Ratio exact;
  uint64_t significand;
  int exponent;
  int precision;
};

template <typename T>
BinaryValue binaryValue(T value) {
  constexpr int fractionBits = sizeof(T) == 4 ? 23 : 52;
  constexpr int bias = sizeof(T) == 4 ? 127 : 1023;
  uint64_t bits;
  if constexpr (sizeof(T) == 4) bits = std::bit_cast<uint32_t>(value);
  else bits = std::bit_cast<uint64_t>(value);
  const auto encodedExponent = bits >> fractionBits;
  auto significand = bits & ((uint64_t{1} << fractionBits) - 1);
  if (encodedExponent) significand |= uint64_t{1} << fractionBits;
  const int precision = encodedExponent ? fractionBits + 1 : static_cast<int>(std::bit_width(significand));
  const int power = (encodedExponent ? static_cast<int>(encodedExponent) - bias : 1 - bias) - fractionBits;
  Ratio exact = power < 0 ? Ratio{significand, Integer(1) << -power}
                          : Ratio{Integer(significand) << power, 1};
  return {std::move(exact), significand, std::ilogb(value), precision};
}

int decimalExponent(double magnitude, const Ratio& exact) {
  int exponent = static_cast<int>(std::floor(std::log10(magnitude)));
  auto below = [&](int candidate) {
    return candidate >= 0 ? exact.numerator < exact.denominator * powerOfTen(candidate)
                          : exact.numerator * powerOfTen(-candidate) < exact.denominator;
  };
  while (below(exponent)) --exponent;
  while (!below(exponent + 1)) ++exponent;
  return exponent;
}

int initialScale(double magnitude, int binaryExponent) {
  const auto normalized = std::ldexp(magnitude, -binaryExponent);
  // JDK 17's initial decimal interval affects legacy stopping decisions. Keep
  // each operation rounded to binary64, including on compilers that fuse FMA.
  volatile double slope = (normalized - 1.5) * 0.289529654;
  volatile double offset = slope + 0.176091259;
  volatile double shift = static_cast<double>(binaryExponent) * 0.301029995663981;
  return static_cast<int>(std::floor(offset + shift));
}

struct DecimalDigits {
  Integer coefficient;
  int exponent;
};

std::string decimalText(DecimalDigits value) {
  while (value.coefficient % 10 == 0) {
    value.coefficient /= 10;
    ++value.exponent;
  }
  const auto digits = value.coefficient.convert_to<std::string>();
  const int exponent = value.exponent + static_cast<int>(digits.size()) - 1;
  if (exponent < -3 || exponent >= 7) {
    return digits.substr(0, 1) + "." + (digits.size() == 1 ? "0" : digits.substr(1)) +
        "E" + std::to_string(exponent);
  }
  const int point = static_cast<int>(digits.size()) + value.exponent;
  if (point <= 0) return "0." + std::string(-point, '0') + digits;
  if (point >= static_cast<int>(digits.size())) return digits + std::string(point - digits.size(), '0') + ".0";
  return digits.substr(0, point) + "." + digits.substr(point);
}

template <typename T>
std::optional<DecimalDigits> integralValue(T magnitude) {
  const int exponent = std::ilogb(magnitude);
  if (exponent < -21 || exponent > 62 || std::floor(magnitude) != magnitude) return std::nullopt;
  // Legacy integer formatting discards decimal places below a quarter ULP,
  // rather than selecting the shortest round-tripping representation.
  constexpr int precision = sizeof(T) == 4 ? 24 : 53;
  const int gap = exponent - precision - 1;
  const int places = gap > 1 ? static_cast<int>(std::to_string(uint64_t{1} << gap).size()) - 1 : 0;
  const auto& quantum = powerOfTen(places);
  Integer integer = static_cast<uint64_t>(magnitude);
  return DecimalDigits{(integer + quantum / 2) / quantum, places};
}

struct DigitRanges {
  Integer numerator;
  Integer denominator;
  Integer margin;
  int wordBits;
  int exponent;
  int extraDigit;
};

DigitRanges digitRanges(double magnitude, const BinaryValue& binary) {
  const int exponent = decimalExponent(magnitude, binary.exact);
  const int scale = initialScale(magnitude, binary.exponent);
  if (scale < exponent || scale > exponent + 1) throw std::runtime_error("Invalid Paimon floating scale");
  int marginPower = binary.exponent - binary.precision;
  if (std::has_single_bit(binary.significand)) --marginPower;
  const Ratio delta = marginPower < 0 ? Ratio{1, Integer(1) << -marginPower}
                                     : Ratio{Integer(1) << marginPower, 1};
  const Integer common = std::max(binary.exact.denominator, delta.denominator);
  DigitRanges ranges{binary.exact.numerator * (common / binary.exact.denominator), common,
      delta.numerator * (common / delta.denominator), 0, exponent, scale - exponent};
  if (scale >= 0) ranges.denominator *= powerOfTen(scale);
  else {
    ranges.numerator *= powerOfTen(-scale);
    ranges.margin *= powerOfTen(-scale);
  }
  while ((ranges.numerator & 1) == 0 && (ranges.denominator & 1) == 0 && (ranges.margin & 1) == 0) {
    ranges.numerator >>= 1;
    ranges.denominator >>= 1;
    ranges.margin >>= 1;
  }
  const int significantBits = static_cast<int>(std::bit_width(binary.significand)) -
      std::countr_zero(binary.significand);
  const int numeratorBits = significantBits + boost::multiprecision::lsb(ranges.numerator) +
      powerOfFiveBits(std::max(-scale, 0));
  const int limitBits = boost::multiprecision::lsb(ranges.denominator) + 1 +
      powerOfFiveBits(std::max(scale, 0) + 1);
  ranges.wordBits = numeratorBits < 32 && limitBits < 32 ? 32 : numeratorBits < 64 && limitBits < 64 ? 64 : 0;
  return ranges;
}

int64_t signedWord(const Integer& value, int width) {
  // Reproduce the observable fixed-width JDK 17 comparison using defined
  // unsigned arithmetic. Never perform signed overflow in the Native engine.
  const uint64_t word = (value & ((Integer(1) << width) - 1)).convert_to<uint64_t>();
  return width == 32 ? static_cast<int64_t>(std::bit_cast<int32_t>(static_cast<uint32_t>(word)))
                     : std::bit_cast<int64_t>(word);
}

struct StoppingBounds { bool lower; bool upper; };

StoppingBounds stoppingBounds(const DigitRanges& ranges, const Integer& remainder, const Integer& margin) {
  const Integer limit = ranges.denominator * 10;
  if (ranges.wordBits) {
    const auto wordMargin = signedWord(margin, ranges.wordBits);
    if (wordMargin <= 0) return {true, true};
    return {remainder < wordMargin, signedWord(remainder + margin, ranges.wordBits) > limit};
  }
  return {remainder < margin, remainder + margin >= limit};
}

int firstDigitCount(const DigitRanges& ranges) {
  bool acceptedLeadingZero = false;
  if (ranges.extraDigit && ranges.numerator < ranges.denominator) {
    acceptedLeadingZero = stoppingBounds(ranges, ranges.numerator * 10, ranges.margin * 10).upper;
  }
  const bool scientific = ranges.exponent < -3 || ranges.exponent >= 7;
  return scientific && !acceptedLeadingZero ? 2 : 1;
}

std::optional<DecimalDigits> roundedDigits(const DigitRanges& ranges, int count) {
  const Integer scaled = ranges.numerator * powerOfTen(count - 1 + ranges.extraDigit);
  Integer coefficient = scaled / ranges.denominator;
  const Integer remainder = (scaled % ranges.denominator) * 10;
  const Integer margin = ranges.margin * powerOfTen(count + ranges.extraDigit);
  const auto bounds = stoppingBounds(ranges, remainder, margin);
  if (!bounds.lower && !bounds.upper) return std::nullopt;
  const Integer limit = ranges.denominator * 10;
  if (bounds.upper && (!bounds.lower || remainder * 2 > limit ||
      (remainder * 2 == limit && (coefficient & 1) != 0))) ++coefficient;
  return DecimalDigits{std::move(coefficient), ranges.exponent - count + 1};
}

template <typename T>
std::string floatingText(T value) {
  if (std::isnan(value)) return "NaN";
  if (std::isinf(value)) return value < 0 ? "-Infinity" : "Infinity";
  if (value == 0) return std::signbit(value) ? "-0.0" : "0.0";
  const auto magnitude = std::abs(value);
  const std::string sign = std::signbit(value) ? "-" : "";
  if (auto integer = integralValue(magnitude)) return sign + decimalText(std::move(*integer));
  const auto ranges = digitRanges(static_cast<double>(magnitude), binaryValue(magnitude));
  constexpr int maxDigits = sizeof(T) == 4 ? 9 : 17;
  for (int count = firstDigitCount(ranges); count <= maxDigits; ++count) {
    if (auto digits = roundedDigits(ranges, count)) return sign + decimalText(std::move(*digits));
  }
  throw std::runtime_error("Cannot render Paimon floating value");
}

}  // namespace

std::string paimonFloatingText(float value) { return floatingText(value); }
std::string paimonFloatingText(double value) { return floatingText(value); }

}  // namespace pangrova::velox
