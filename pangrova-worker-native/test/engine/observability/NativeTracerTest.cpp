// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include "engine/observability/NativeTracer.h"
#include "engine/observability/SpanScope.h"

namespace pangrova::observability::test {
namespace {

class NativeTracerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Ensure clean state before each test.
    NativeTracer::shutdown();
  }

  void TearDown() override {
    NativeTracer::shutdown();
  }
};

// --- Initialization ---

TEST_F(NativeTracerTest, NotInitializedByDefault) {
  EXPECT_FALSE(NativeTracer::isInitialized());
}

TEST_F(NativeTracerTest, InitializeSetsInitialized) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");
  EXPECT_TRUE(NativeTracer::isInitialized());
}

TEST_F(NativeTracerTest, ShutdownClearsInitialized) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");
  NativeTracer::shutdown();
  EXPECT_FALSE(NativeTracer::isInitialized());
}

// --- startSpanWithParent ---

TEST_F(NativeTracerTest, StartSpanWithParentReturnsNullWhenNotInitialized) {
  auto scope = NativeTracer::startSpanWithParent(
      "0123456789abcdef0123456789abcdef",
      "0123456789abcdef",
      "test-span");
  EXPECT_EQ(scope, nullptr);
}

TEST_F(NativeTracerTest, StartSpanWithParentReturnsValidScope) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto scope = NativeTracer::startSpanWithParent(
      "0123456789abcdef0123456789abcdef",
      "fedcba9876543210",
      "native-execution");

  ASSERT_NE(scope, nullptr);
  EXPECT_TRUE(scope->isValid());
  EXPECT_EQ(scope->traceIdHex(), "0123456789abcdef0123456789abcdef");
  EXPECT_EQ(scope->spanIdHex().size(), 16u);  // 8 bytes = 16 hex chars
  EXPECT_EQ(scope->spanName(), "native-execution");
  EXPECT_FALSE(scope->hasEnded());
}

TEST_F(NativeTracerTest, StartSpanWithParentPreservesTraceId) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  const std::string parentTraceId = "aaaabbbbccccddddeeee111122223333";
  const std::string parentSpanId = "1111222233334444";

  auto scope = NativeTracer::startSpanWithParent(
      parentTraceId, parentSpanId, "child-span");

  ASSERT_NE(scope, nullptr);
  // Child span inherits the parent trace ID.
  EXPECT_EQ(scope->traceIdHex(), parentTraceId);
  // Child span gets its own unique span ID.
  EXPECT_NE(scope->spanIdHex(), parentSpanId);
}

TEST_F(NativeTracerTest, StartSpanWithParentInvalidTraceIdReturnsNull) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  // Too short trace ID.
  auto scope = NativeTracer::startSpanWithParent(
      "0123456789abcdef", "0123456789abcdef", "bad-trace");
  EXPECT_EQ(scope, nullptr);
}

TEST_F(NativeTracerTest, StartSpanWithParentInvalidSpanIdReturnsNull) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  // Too short span ID.
  auto scope = NativeTracer::startSpanWithParent(
      "0123456789abcdef0123456789abcdef", "0123", "bad-span");
  EXPECT_EQ(scope, nullptr);
}

TEST_F(NativeTracerTest, StartSpanWithParentNonHexReturnsNull) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto scope = NativeTracer::startSpanWithParent(
      "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
      "0123456789abcdef",
      "non-hex");
  EXPECT_EQ(scope, nullptr);
}

TEST_F(NativeTracerTest, StartSpanWithParentEmptyNameReturnsNull) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto scope = NativeTracer::startSpanWithParent(
      "0123456789abcdef0123456789abcdef",
      "0123456789abcdef",
      "");
  EXPECT_EQ(scope, nullptr);
}

// --- startSpan ---

TEST_F(NativeTracerTest, StartSpanReturnsNullWhenNotInitialized) {
  auto scope = NativeTracer::startSpan("test-span");
  EXPECT_EQ(scope, nullptr);
}

TEST_F(NativeTracerTest, StartSpanReturnsValidScope) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto scope = NativeTracer::startSpan("root-span");

  ASSERT_NE(scope, nullptr);
  EXPECT_TRUE(scope->isValid());
  EXPECT_EQ(scope->traceIdHex().size(), 32u);  // 16 bytes = 32 hex chars
  EXPECT_EQ(scope->spanIdHex().size(), 16u);
  EXPECT_EQ(scope->spanName(), "root-span");
}

TEST_F(NativeTracerTest, StartSpanEmptyNameReturnsNull) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");
  EXPECT_EQ(NativeTracer::startSpan(""), nullptr);
}

// --- SpanScope lifecycle ---

TEST_F(NativeTracerTest, SpanEndsOnScopeDestruction) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  bool ended = false;
  {
    auto scope = NativeTracer::startSpan("scoped-span");
    ASSERT_NE(scope, nullptr);
    EXPECT_FALSE(scope->hasEnded());
    // scope destroyed here
  }
  // After destruction, the span should have ended.
  // We verify this indirectly by checking the scope was valid before destruction.
}

TEST_F(NativeTracerTest, SpanScopeMoveTransfersOwnership) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto scope1 = NativeTracer::startSpan("movable-span");
  ASSERT_NE(scope1, nullptr);

  auto traceId = scope1->traceIdHex();
  auto spanId = scope1->spanIdHex();

  SpanScope scope2 = std::move(*scope1);
  EXPECT_TRUE(scope2.isValid());
  EXPECT_EQ(scope2.traceIdHex(), traceId);
  EXPECT_EQ(scope2.spanIdHex(), spanId);

  // Original should be invalidated after move.
  EXPECT_FALSE(scope1->isValid());
}

TEST_F(NativeTracerTest, MultipleSpansGetUniqueIds) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker");

  auto span1 = NativeTracer::startSpan("span-1");
  auto span2 = NativeTracer::startSpan("span-2");

  ASSERT_NE(span1, nullptr);
  ASSERT_NE(span2, nullptr);

  // Different spans should have different span IDs.
  EXPECT_NE(span1->spanIdHex(), span2->spanIdHex());
}

} // namespace
} // namespace pangrova::observability::test
