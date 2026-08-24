// IndexHintFilterTest: Unit tests for predicate JSON parsing.
//
// F-NATIVE-WRITE-10: Validates the simdjson-based predicate parser
// correctly handles whitespace, escaped strings, additional fields,
// nested objects, and malformed input — all cases where the legacy
// hand-rolled regex parser produced incorrect results.

#include <gtest/gtest.h>

#include "index/IndexHintFilter.h"

namespace pangrova::index {
namespace {

// --- Happy path: documented predicate shapes -------------------------------

TEST(IndexHintFilterTest, ParseEqPredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"user_id","value":"abc123"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "user_id");
  EXPECT_EQ(p.value, "abc123");
}

TEST(IndexHintFilterTest, ParseInPredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"region","values":["us","eu","apac"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  EXPECT_EQ(p.column_name, "region");
  ASSERT_EQ(p.values.size(), 3u);
  EXPECT_EQ(p.values[0], "us");
  EXPECT_EQ(p.values[1], "eu");
  EXPECT_EQ(p.values[2], "apac");
}

TEST(IndexHintFilterTest, ParseRangePredicate) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"amount","min":100,"max":999})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.column_name, "amount");
  EXPECT_EQ(p.range_min, 100);
  EXPECT_EQ(p.range_max, 999);
}

TEST(IndexHintFilterTest, ParseRangePredicateNegativeBounds) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"delta","min":-50,"max":-10})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, -50);
  EXPECT_EQ(p.range_max, -10);
}

// --- Robustness: cases that broke the legacy regex parser ------------------

TEST(IndexHintFilterTest, ParseEqWithWhitespace) {
  // Legacy parser already handled this, but verify still works.
  auto p = IndexHintFilter::parsePredicate(
      "{ \"op\" : \"eq\" , \"column\" : \"k\" , \"value\" : \"v\" }");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "k");
  EXPECT_EQ(p.value, "v");
}

TEST(IndexHintFilterTest, ParseEqWithEscapedQuoteInValue) {
  // Legacy regex would terminate at the first inner `"` — break.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"name","value":"O\"Brien"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "name");
  EXPECT_EQ(p.value, "O\"Brien");
}

TEST(IndexHintFilterTest, ParseEqWithBackslashInValue) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"path","value":"C:\\Users\\joe"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.value, "C:\\Users\\joe");
}

TEST(IndexHintFilterTest, ParseEqWithNestedObjectBeforeOp) {
  // Legacy regex: `find("op")` matches inside the nested "operator" string.
  auto p = IndexHintFilter::parsePredicate(
      R"({"context":{"operator":"scan","stage":"leaf"},"op":"eq","column":"k","value":"v"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.column_name, "k");
  EXPECT_EQ(p.value, "v");
}

TEST(IndexHintFilterTest, ParseRangeWithExtraFields) {
  // Realistic: predicate emitter adds metadata fields the parser must skip.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"ts","min":1000,"max":2000,"hint_id":"h7","priority":3})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, 1000);
  EXPECT_EQ(p.range_max, 2000);
}

TEST(IndexHintFilterTest, ParseInWithKeyNamedValueElsewhere) {
  // Legacy: `find("values")` could collide with a sibling field whose key
  // contains "values" — simdjson key matching is exact.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"c","cached_values_hash":"x","values":["a","b"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  ASSERT_EQ(p.values.size(), 2u);
  EXPECT_EQ(p.values[0], "a");
  EXPECT_EQ(p.values[1], "b");
}

// --- Malformed input: parser must return UNSUPPORTED, not crash ------------

TEST(IndexHintFilterTest, ParseMalformedJsonReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate("{not even json");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseEmptyStringReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate("");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseMissingColumnReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","value":"v"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseMissingValueReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"k"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseEmptyValuesArrayReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"k","values":[]})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseUnknownOpReturnsUnsupported) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"like","column":"k","value":"%foo%"})");
  EXPECT_EQ(p.op, PredicateOp::UNSUPPORTED);
}

TEST(IndexHintFilterTest, ParseRangeWithMissingBoundsDefaultsToZero) {
  // Legacy contract: extractJsonInt() returned 0 on missing key.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"range","column":"k"})");
  EXPECT_EQ(p.op, PredicateOp::RANGE);
  EXPECT_EQ(p.range_min, 0);
  EXPECT_EQ(p.range_max, 0);
}

TEST(IndexHintFilterTest, ParseInWithNonStringElementsFiltersThem) {
  // Mixed types: only string elements are extracted; rest skipped.
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"in","column":"k","values":["a",123,"b",null,"c"]})");
  EXPECT_EQ(p.op, PredicateOp::IN);
  ASSERT_EQ(p.values.size(), 3u);
  EXPECT_EQ(p.values[0], "a");
  EXPECT_EQ(p.values[1], "b");
  EXPECT_EQ(p.values[2], "c");
}

TEST(IndexHintFilterTest, ParseValueWithUnicode) {
  auto p = IndexHintFilter::parsePredicate(
      R"({"op":"eq","column":"name","value":"日本語"})");
  EXPECT_EQ(p.op, PredicateOp::EQ);
  EXPECT_EQ(p.value, "日本語");
}

}  // namespace
}  // namespace pangrova::index
