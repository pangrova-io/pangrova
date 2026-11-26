#include "indexing/index/BTreeIndexReader.h"

#include <bit>
#include <cmath>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "pangrova/protocol/v2/connector.pb.h"

namespace pangrova::index {
namespace {

namespace proto = pangrova::protocol::v2;

constexpr uint8_t kNullTag = 0;
constexpr uint8_t kStringTag = 1;
constexpr uint8_t kInt32Tag = 2;
constexpr uint8_t kInt64Tag = 3;
constexpr uint8_t kDoubleTag = 4;
constexpr uint8_t kFloatTag = 5;
constexpr uint8_t kShortTag = 6;
constexpr uint8_t kByteTag = 7;
constexpr uint8_t kBigDecimalTag = 8;
constexpr uint8_t kBigIntegerTag = 9;
constexpr uint8_t kUuidTag = 10;
constexpr uint8_t kCompositeTag = 11;
constexpr uint32_t kMaxCompositeComponents = 1024;

using Key = std::variant<std::u16string, int32_t, int64_t, double, float, bool,
                         std::monostate>;

[[noreturn]] void malformed() {
  throw std::runtime_error("Malformed B-tree index payload");
}

class Input {
 public:
  explicit Input(const std::string& bytes) : bytes_(bytes) {}

  size_t remaining() const { return bytes_.size() - position_; }

  uint8_t readByte() {
    if (remaining() < 1) malformed();
    return static_cast<uint8_t>(bytes_[position_++]);
  }

  uint16_t readU16() {
    if (remaining() < 2) malformed();
    const auto* raw = reinterpret_cast<const unsigned char*>(bytes_.data() + position_);
    position_ += 2;
    return static_cast<uint16_t>((raw[0] << 8) | raw[1]);
  }

  uint32_t readU32() {
    if (remaining() < 4) malformed();
    const auto* raw = reinterpret_cast<const unsigned char*>(bytes_.data() + position_);
    position_ += 4;
    return (static_cast<uint32_t>(raw[0]) << 24) |
           (static_cast<uint32_t>(raw[1]) << 16) |
           (static_cast<uint32_t>(raw[2]) << 8) | raw[3];
  }

  int32_t readI32() { return std::bit_cast<int32_t>(readU32()); }

  int64_t readI64() {
    const auto high = readU32();
    const auto low = readU32();
    return std::bit_cast<int64_t>((static_cast<uint64_t>(high) << 32) | low);
  }

  double readDouble() { return std::bit_cast<double>(static_cast<uint64_t>(readI64())); }

  float readFloat() { return std::bit_cast<float>(readU32()); }

  std::string_view readBytes(size_t length) {
    if (length > remaining()) malformed();
    auto result = std::string_view(bytes_).substr(position_, length);
    position_ += length;
    return result;
  }

  void skip(size_t length) {
    if (length > remaining()) malformed();
    position_ += length;
  }

  std::u16string readModifiedUtf8() {
    const auto length = readU16();
    const auto bytes = readBytes(length);
    std::u16string result;
    result.reserve(length);
    size_t offset = 0;
    while (offset < bytes.size()) {
      const auto first = static_cast<uint8_t>(bytes[offset++]);
      if (first >= 0x01 && first <= 0x7f) {
        result.push_back(static_cast<char16_t>(first));
        continue;
      }
      if (first == 0xc0 && offset < bytes.size()
          && static_cast<uint8_t>(bytes[offset]) == 0x80) {
        ++offset;
        result.push_back(u'\0');
        continue;
      }
      if ((first & 0xe0) == 0xc0) {
        if (first < 0xc2 || offset >= bytes.size()) malformed();
        const auto second = static_cast<uint8_t>(bytes[offset++]);
        if ((second & 0xc0) != 0x80) malformed();
        const auto code = static_cast<uint32_t>(((first & 0x1f) << 6) | (second & 0x3f));
        result.push_back(static_cast<char16_t>(code));
        continue;
      }
      if ((first & 0xf0) == 0xe0) {
        if (offset + 1 >= bytes.size()) malformed();
        const auto second = static_cast<uint8_t>(bytes[offset++]);
        const auto third = static_cast<uint8_t>(bytes[offset++]);
        if ((second & 0xc0) != 0x80 || (third & 0xc0) != 0x80
            || (first == 0xe0 && second < 0xa0)) {
          malformed();
        }
        const auto code = static_cast<uint32_t>(((first & 0x0f) << 12)
            | ((second & 0x3f) << 6) | (third & 0x3f));
        result.push_back(static_cast<char16_t>(code));
        continue;
      }
      malformed();
    }
    return result;
  }

 private:
  const std::string& bytes_;
  size_t position_ = 0;
};

std::u16string readUtf8(std::string_view bytes) {
  std::u16string result;
  size_t offset = 0;
  while (offset < bytes.size()) {
    const auto first = static_cast<uint8_t>(bytes[offset++]);
    if (first <= 0x7f) {
      result.push_back(static_cast<char16_t>(first));
      continue;
    }
    if ((first & 0xe0) == 0xc0) {
      if (first < 0xc2 || offset >= bytes.size()) malformed();
      const auto second = static_cast<uint8_t>(bytes[offset++]);
      if ((second & 0xc0) != 0x80) malformed();
      const auto code = static_cast<uint32_t>(((first & 0x1f) << 6) | (second & 0x3f));
      result.push_back(static_cast<char16_t>(code));
      continue;
    }
    if ((first & 0xf0) == 0xe0) {
      if (offset + 1 >= bytes.size()) malformed();
      const auto second = static_cast<uint8_t>(bytes[offset++]);
      const auto third = static_cast<uint8_t>(bytes[offset++]);
      const auto code = static_cast<uint32_t>(((first & 0x0f) << 12)
          | ((second & 0x3f) << 6) | (third & 0x3f));
      if ((second & 0xc0) != 0x80 || (third & 0xc0) != 0x80
          || code < 0x800 || (code >= 0xd800 && code <= 0xdfff)) {
        malformed();
      }
      result.push_back(static_cast<char16_t>(code));
      continue;
    }
    if ((first & 0xf8) == 0xf0) {
      if (offset + 2 >= bytes.size()) malformed();
      const auto second = static_cast<uint8_t>(bytes[offset++]);
      const auto third = static_cast<uint8_t>(bytes[offset++]);
      const auto fourth = static_cast<uint8_t>(bytes[offset++]);
      const auto code = static_cast<uint32_t>(((first & 0x07) << 18)
          | ((second & 0x3f) << 12) | ((third & 0x3f) << 6) | (fourth & 0x3f));
      if ((second & 0xc0) != 0x80 || (third & 0xc0) != 0x80
          || (fourth & 0xc0) != 0x80 || code < 0x10000 || code > 0x10ffff
          || (first == 0xf0 && second < 0x90) || (first == 0xf4 && second > 0x8f)) {
        malformed();
      }
      result.push_back(static_cast<char16_t>(0xd800 + ((code - 0x10000) >> 10)));
      result.push_back(static_cast<char16_t>(0xdc00 + ((code - 0x10000) & 0x3ff)));
      continue;
    }
    malformed();
  }
  return result;
}

std::optional<Key> parseKey(Input& input, bool allowComposite) {
  switch (input.readByte()) {
    case kNullTag:
      return std::nullopt;
    case kStringTag:
      return Key(input.readModifiedUtf8());
    case kInt32Tag:
      return Key(input.readI32());
    case kInt64Tag:
      return Key(input.readI64());
    case kDoubleTag:
      return Key(input.readDouble());
    case kFloatTag:
      return Key(input.readFloat());
    case kShortTag:
      input.skip(2);
      return Key(std::monostate{});
    case kByteTag:
      input.skip(1);
      return Key(std::monostate{});
    case kBigDecimalTag:
      input.skip(input.readU32());
      input.skip(4);
      return Key(std::monostate{});
    case kBigIntegerTag:
      input.skip(input.readU32());
      return Key(std::monostate{});
    case kUuidTag:
      input.skip(16);
      return Key(std::monostate{});
    case kCompositeTag: {
      if (!allowComposite) malformed();
      const auto count = input.readU32();
      if (count < 2 || count > kMaxCompositeComponents) malformed();
      for (uint32_t index = 0; index < count; ++index) {
        if (!parseKey(input, false).has_value()) malformed();
      }
      return Key(std::monostate{});
    }
    default:
      malformed();
  }
}

std::optional<Key> parseProtoKey(const proto::ComparableValue& value) {
  switch (value.value_case()) {
    case proto::ComparableValue::kStringValue:
      return Key(readUtf8(value.string_value()));
    case proto::ComparableValue::kInt32Value:
      return Key(value.int32_value());
    case proto::ComparableValue::kInt64Value:
      return Key(value.int64_value());
    case proto::ComparableValue::kDoubleValue:
      return Key(value.double_value());
    case proto::ComparableValue::kFloatValue:
      return Key(value.float_value());
    case proto::ComparableValue::kBoolValue:
      return Key(value.bool_value());
    default:
      return std::nullopt;
  }
}

template <typename T>
int compareFloating(T left, T right) {
  if (std::isnan(left)) return std::isnan(right) ? 0 : 1;
  if (std::isnan(right)) return -1;
  if (left == right) {
    if (left == T{0} && std::signbit(left) != std::signbit(right)) {
      return std::signbit(left) ? -1 : 1;
    }
    return 0;
  }
  return left < right ? -1 : 1;
}

std::optional<int> compareKeys(const Key& left, const Key& right) {
  if (left.index() != right.index()) return std::nullopt;
  if (const auto* a = std::get_if<std::u16string>(&left)) {
    const auto comparison = a->compare(*std::get_if<std::u16string>(&right));
    return comparison < 0 ? -1 : comparison > 0 ? 1 : 0;
  }
  if (const auto* a = std::get_if<int32_t>(&left)) {
    const auto b = *std::get_if<int32_t>(&right);
    return *a < b ? -1 : *a > b ? 1 : 0;
  }
  if (const auto* a = std::get_if<int64_t>(&left)) {
    const auto b = *std::get_if<int64_t>(&right);
    return *a < b ? -1 : *a > b ? 1 : 0;
  }
  if (const auto* a = std::get_if<double>(&left)) {
    return compareFloating(*a, *std::get_if<double>(&right));
  }
  if (const auto* a = std::get_if<float>(&left)) {
    return compareFloating(*a, *std::get_if<float>(&right));
  }
  if (const auto* a = std::get_if<bool>(&left)) {
    const auto b = *std::get_if<bool>(&right);
    return *a == b ? 0 : *a ? 1 : -1;
  }
  return std::nullopt;
}

enum class PredicateKind {
  kEqual,
  kRange,
  kInList,
  kIsNull,
  kIsNotNull,
};

struct Predicate {
  PredicateKind kind;
  std::vector<Key> values;
  std::optional<Key> lower;
  std::optional<Key> upper;
  bool lowerInclusive = false;
  bool upperInclusive = false;
};

bool sameKeyType(const Key& left, const Key& right) {
  return left.index() == right.index()
      && !std::holds_alternative<std::monostate>(left);
}

std::optional<Predicate> parsePredicate(const proto::IndexPredicate& predicate) {
  if (predicate.column().empty()) return std::nullopt;
  switch (predicate.predicate_case()) {
    case proto::IndexPredicate::kEqualTo: {
      auto value = parseProtoKey(predicate.equal_to().value());
      if (!value || std::holds_alternative<std::monostate>(*value)) return std::nullopt;
      return Predicate{PredicateKind::kEqual, {*value}, std::nullopt, std::nullopt, false, false};
    }
    case proto::IndexPredicate::kRangeQuery: {
      const auto& range = predicate.range_query();
      if (!range.has_lower() && !range.has_upper()) return std::nullopt;
      Predicate result{PredicateKind::kRange, {}, std::nullopt, std::nullopt,
                       range.lower_inclusive(), range.upper_inclusive()};
      if (range.has_lower()) {
        auto lower = parseProtoKey(range.lower());
        if (!lower || std::holds_alternative<std::monostate>(*lower)) return std::nullopt;
        result.lower = *lower;
      }
      if (range.has_upper()) {
        auto upper = parseProtoKey(range.upper());
        if (!upper || std::holds_alternative<std::monostate>(*upper)
            || (result.lower && !sameKeyType(*result.lower, *upper))) {
          return std::nullopt;
        }
        result.upper = *upper;
      }
      return result;
    }
    case proto::IndexPredicate::kInList: {
      if (predicate.in_list().values_size() == 0) return std::nullopt;
      Predicate result{PredicateKind::kInList, {}, std::nullopt, std::nullopt, false, false};
      for (const auto& value : predicate.in_list().values()) {
        auto key = parseProtoKey(value);
        if (!key || std::holds_alternative<std::monostate>(*key)
            || (!result.values.empty() && !sameKeyType(result.values.front(), *key))) {
          return std::nullopt;
        }
        result.values.push_back(*key);
      }
      return result;
    }
    case proto::IndexPredicate::kIsNull:
      return Predicate{PredicateKind::kIsNull, {}, std::nullopt, std::nullopt, false, false};
    case proto::IndexPredicate::kIsNotNull:
      return Predicate{PredicateKind::kIsNotNull, {}, std::nullopt, std::nullopt, false, false};
    default:
      return std::nullopt;
  }
}

bool matches(const Key& key, const Predicate& predicate) {
  switch (predicate.kind) {
    case PredicateKind::kEqual:
      return compareKeys(key, predicate.values.front()) == 0;
    case PredicateKind::kRange: {
      if (predicate.lower) {
        const auto comparison = compareKeys(key, *predicate.lower);
        if (!comparison || (predicate.lowerInclusive ? *comparison < 0 : *comparison <= 0)) {
          return false;
        }
      }
      if (predicate.upper) {
        const auto comparison = compareKeys(key, *predicate.upper);
        return comparison && (predicate.upperInclusive ? *comparison <= 0 : *comparison < 0);
      }
      return true;
    }
    case PredicateKind::kInList:
      for (const auto& value : predicate.values) {
        if (compareKeys(key, value) == 0) return true;
      }
      return false;
    case PredicateKind::kIsNull:
      return false;
    case PredicateKind::kIsNotNull:
      return true;
  }
  return false;
}

Key requireKey(const std::optional<Key>& key) {
  if (!key || std::holds_alternative<std::monostate>(*key)) malformed();
  return *key;
}

}  // namespace

BTreeIndexReader::Result BTreeIndexReader::evaluate(
    const std::string& payload, const proto::IndexPredicate& predicate) {
  try {
    const auto parsedPredicate = parsePredicate(predicate);
    if (!parsedPredicate || parsedPredicate->kind == PredicateKind::kIsNull) {
      return Result::kUnknown;
    }
    const auto& query = *parsedPredicate;
    const Key* queryKey = query.lower ? &*query.lower : query.upper ? &*query.upper
        : query.values.empty() ? nullptr : &query.values.front();
    Input input(payload);
    const auto storePositions = input.readByte();
    if (storePositions > 1) malformed();
    const auto pageCount = input.readU32();
    if (pageCount > input.remaining() / 7) malformed();

    bool sawKey = false;
    bool mayMatch = false;
    std::optional<Key> previousPageMaximum;
    for (uint32_t page = 0; page < pageCount; ++page) {
      const auto keyCount = input.readU32();
      if (keyCount == 0 || keyCount > input.remaining()) malformed();
      std::optional<Key> first;
      std::optional<Key> previous;
      for (uint32_t index = 0; index < keyCount; ++index) {
        auto key = requireKey(parseKey(input, true));
        if (queryKey && !sameKeyType(key, *queryKey)) {
          return Result::kUnknown;
        }
        if (!first.has_value()) first = key;
        if (previous.has_value()) {
          const auto comparison = compareKeys(*previous, key);
          if (!comparison || *comparison > 0) malformed();
        }
        if (queryKey && matches(key, query)) mayMatch = true;
        previous = std::move(key);
        sawKey = true;
      }
      const auto minimum = requireKey(parseKey(input, true));
      const auto maximum = requireKey(parseKey(input, true));
      const auto minimumComparison = first ? compareKeys(minimum, *first) : std::nullopt;
      const auto maximumComparison = previous ? compareKeys(maximum, *previous) : std::nullopt;
      if (!minimumComparison || *minimumComparison != 0
          || !maximumComparison || *maximumComparison != 0) {
        malformed();
      }
      if (previousPageMaximum) {
        const auto pageComparison = compareKeys(*previousPageMaximum, minimum);
        if (!pageComparison || *pageComparison > 0) malformed();
      }
      previousPageMaximum = maximum;
      if (storePositions != 0) {
        for (uint32_t index = 0; index < keyCount; ++index) {
          const auto positions = input.readU32();
          if (positions > input.remaining() / 4) malformed();
          input.skip(static_cast<size_t>(positions) * 4);
        }
      }
    }
    if (input.remaining() != 0 || !sawKey) return Result::kUnknown;
    if (query.kind == PredicateKind::kIsNotNull) return Result::kMayMatch;
    return mayMatch ? Result::kMayMatch : Result::kProvenAbsent;
  } catch (const std::exception& failure) {
    return Result::kUnknown;
  }
}

}  // namespace pangrova::index
