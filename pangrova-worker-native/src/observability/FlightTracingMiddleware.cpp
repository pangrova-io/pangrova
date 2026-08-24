// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "observability/FlightTracingMiddleware.h"

#include <algorithm>
#include <array>
#include <cctype>

#include "arrow/flight/server.h"
#include "observability/NativeTracer.h"
#include "observability/SpanScope.h"

namespace pangrova::observability {

namespace {

constexpr size_t kTraceIdHexLen = 32;
constexpr size_t kSpanIdHexLen = 16;

bool isLowerHex(std::string_view s) {
  return std::all_of(s.begin(), s.end(), [](unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

bool isAllZero(std::string_view s) {
  return std::all_of(s.begin(), s.end(), [](char c) { return c == '0'; });
}

}  // namespace

bool parseTraceparent(std::string_view traceparent, std::string& traceIdHex,
                      std::string& spanIdHex) {
  // Expected layout: "vv-<32 hex>-<16 hex>-ff" (split on '-' into 4 fields).
  std::array<std::string_view, 4> fields{};
  size_t field = 0;
  size_t start = 0;
  for (size_t i = 0; i <= traceparent.size(); ++i) {
    if (i == traceparent.size() || traceparent[i] == '-') {
      if (field >= fields.size()) {
        return false;  // too many fields
      }
      fields[field++] = traceparent.substr(start, i - start);
      start = i + 1;
    }
  }
  if (field != fields.size()) {
    return false;  // wrong number of fields
  }

  std::string_view version = fields[0];
  std::string_view trace = fields[1];
  std::string_view span = fields[2];

  if (version.size() != 2 || !isLowerHex(version) || version == "ff") {
    return false;
  }
  if (trace.size() != kTraceIdHexLen || !isLowerHex(trace) || isAllZero(trace)) {
    return false;
  }
  if (span.size() != kSpanIdHexLen || !isLowerHex(span) || isAllZero(span)) {
    return false;
  }

  traceIdHex.assign(trace.begin(), trace.end());
  spanIdHex.assign(span.begin(), span.end());
  return true;
}

std::unique_ptr<SpanScope> startSpanFromFlightHeaders(
    const arrow::flight::CallHeaders& headers, const std::string& spanName) {
  auto it = headers.find("traceparent");
  if (it != headers.end()) {
    std::string traceId;
    std::string spanId;
    if (parseTraceparent(it->second, traceId, spanId)) {
      return NativeTracer::startSpanWithParent(traceId, spanId, spanName);
    }
  }
  // No usable inbound context — start a fresh root span (no-op if disabled).
  return NativeTracer::startSpan(spanName);
}

FlightTracingMiddleware::FlightTracingMiddleware(std::unique_ptr<SpanScope> scope)
    : scope_(std::move(scope)) {}

FlightTracingMiddleware::~FlightTracingMiddleware() = default;

void FlightTracingMiddleware::SendingHeaders(
    arrow::flight::AddCallHeaders* /*outgoing_headers*/) {}

void FlightTracingMiddleware::CallCompleted(const arrow::Status& /*status*/) {
  // End the span deterministically at call completion rather than waiting for
  // middleware destruction (which the Flight runtime may defer).
  scope_.reset();
}

arrow::Status FlightTracingMiddlewareFactory::StartCall(
    const arrow::flight::CallInfo& info,
    const arrow::flight::ServerCallContext& context,
    std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) {
  std::string spanName = "flight." + arrow::flight::ToString(info.method);
  auto scope = startSpanFromFlightHeaders(context.incoming_headers(), spanName);
  *middleware = std::make_shared<FlightTracingMiddleware>(std::move(scope));
  return arrow::Status::OK();
}

}  // namespace pangrova::observability
