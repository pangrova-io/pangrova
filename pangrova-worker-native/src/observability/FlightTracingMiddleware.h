// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// FlightTracingMiddleware: server-side Arrow Flight middleware that joins the
// data plane (result/shuffle streaming) to the distributed trace. It reads the
// W3C "traceparent" header from each inbound Flight RPC and starts a native
// span parented to the coordinator-injected trace, closing the cross-process
// gap (F-CHAIN-OBS-03, task 178 half B).

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "arrow/flight/server_middleware.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"

namespace pangrova::observability {

class SpanScope;

// Parses a W3C traceparent header value of the form
// "<version>-<32hex traceid>-<16hex spanid>-<flags>". Returns true and fills
// traceIdHex/spanIdHex on success; false for any malformed or all-zero value.
bool parseTraceparent(std::string_view traceparent, std::string& traceIdHex,
                      std::string& spanIdHex);

// Starts a native span parented to the inbound trace context carried in the
// Flight call headers (W3C "traceparent"). When no/invalid traceparent is
// present, starts a fresh root span. Returns null when the tracer is disabled
// or the span name is empty.
std::unique_ptr<SpanScope> startSpanFromFlightHeaders(
    const arrow::flight::CallHeaders& headers, const std::string& spanName);

// Per-call server middleware that owns the active data-plane span and ends it
// (via SpanScope destruction) when the Flight call completes.
class FlightTracingMiddleware : public arrow::flight::ServerMiddleware {
 public:
  explicit FlightTracingMiddleware(std::unique_ptr<SpanScope> scope);
  ~FlightTracingMiddleware() override;

  std::string name() const override { return "FlightTracingMiddleware"; }
  void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override;
  void CallCompleted(const arrow::Status& status) override;

 private:
  std::unique_ptr<SpanScope> scope_;
};

// Factory: instantiated once per RPC. Reads the inbound traceparent and starts
// a data-plane span so result/shuffle streaming joins the distributed trace.
class FlightTracingMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
 public:
  arrow::Status StartCall(
      const arrow::flight::CallInfo& info,
      const arrow::flight::ServerCallContext& context,
      std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override;
};

}  // namespace pangrova::observability
