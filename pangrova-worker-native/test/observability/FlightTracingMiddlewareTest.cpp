// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Verifies the data-plane Flight tracing middleware (F-CHAIN-OBS-03, task 178
// half B): the inbound W3C traceparent on a Flight RPC must be parsed and the
// native span must parent to the upstream trace, so result/shuffle streaming
// joins the same distributed trace as the coordinator.

#include "observability/FlightTracingMiddleware.h"

#include <gtest/gtest.h>

#include "arrow/flight/types.h"
#include "observability/NativeTracer.h"
#include "observability/SpanScope.h"

namespace pangrova::observability::test {
namespace {

// traceparent the upstream (coordinator/worker) injects.
constexpr const char* kTraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
constexpr const char* kSpanId = "00f067aa0ba902b7";
constexpr const char* kTraceparent =
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

class FlightTracingMiddlewareTest : public ::testing::Test {
 protected:
  void SetUp() override { NativeTracer::shutdown(); }
  void TearDown() override { NativeTracer::shutdown(); }
};

// --- parseTraceparent ---

TEST_F(FlightTracingMiddlewareTest, ParseValidTraceparent) {
  std::string traceId;
  std::string spanId;
  ASSERT_TRUE(parseTraceparent(kTraceparent, traceId, spanId));
  EXPECT_EQ(traceId, kTraceId);
  EXPECT_EQ(spanId, kSpanId);
}

TEST_F(FlightTracingMiddlewareTest, ParseRejectsMalformed) {
  std::string traceId;
  std::string spanId;
  EXPECT_FALSE(parseTraceparent("garbage", traceId, spanId));
  EXPECT_FALSE(parseTraceparent("00-tooshort-00f067aa0ba902b7-01", traceId, spanId));
  EXPECT_FALSE(parseTraceparent(
      "00-4bf92f3577b34da6a3ce929d0e0e4736-tooshort-01", traceId, spanId));
  // All-zero trace/span IDs are invalid per W3C.
  EXPECT_FALSE(parseTraceparent(
      "00-00000000000000000000000000000000-00f067aa0ba902b7-01", traceId, spanId));
  EXPECT_FALSE(parseTraceparent(
      "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", traceId, spanId));
  // Non-hex.
  EXPECT_FALSE(parseTraceparent(
      "00-ZZZZ2f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", traceId, spanId));
  // Wrong field count.
  EXPECT_FALSE(parseTraceparent(
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", traceId, spanId));
}

// --- startSpanFromFlightHeaders (cross-process linking) ---

TEST_F(FlightTracingMiddlewareTest, SpanJoinsUpstreamTraceFromHeader) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker-native-test");

  arrow::flight::CallHeaders headers;
  headers.emplace("traceparent", kTraceparent);

  auto scope = startSpanFromFlightHeaders(headers, "flight.DoGet");
  ASSERT_NE(scope, nullptr);
  EXPECT_TRUE(scope->isValid());
  // The data-plane span must share the upstream trace id (cross-process link).
  EXPECT_EQ(scope->traceIdHex(), kTraceId);
  // …with its own span id (a child of the injected parent span).
  EXPECT_NE(scope->spanIdHex(), kSpanId);
  EXPECT_EQ(scope->spanName(), "flight.DoGet");
}

TEST_F(FlightTracingMiddlewareTest, NoTraceparentStartsFreshRoot) {
  NativeTracer::initialize("http://localhost:4317", "pangrova-worker-native-test");

  arrow::flight::CallHeaders headers;  // no traceparent
  auto scope = startSpanFromFlightHeaders(headers, "flight.DoGet");
  ASSERT_NE(scope, nullptr);
  EXPECT_TRUE(scope->isValid());
  // Fresh root: a full random 32-hex trace id, not the upstream one.
  EXPECT_EQ(scope->traceIdHex().size(), 32u);
  EXPECT_NE(scope->traceIdHex(), kTraceId);
}

TEST_F(FlightTracingMiddlewareTest, ReturnsNullWhenTracerDisabled) {
  // Tracer not initialized → no spans produced (degrades to no-op).
  arrow::flight::CallHeaders headers;
  headers.emplace("traceparent", kTraceparent);
  EXPECT_EQ(startSpanFromFlightHeaders(headers, "flight.DoGet"), nullptr);
}

}  // namespace
}  // namespace pangrova::observability::test
