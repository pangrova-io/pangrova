// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Dispatches canonical v2 RuntimeCommand proto to the appropriate handler.
// Each command type is handled by a dedicated function for single responsibility.
// Returns serialized RuntimeCommandResponse proto bytes.

#pragma once

#include <cstdint>
#include <vector>

namespace pangrova::jni {

// Dispatches a serialized RuntimeCommand proto to the appropriate handler.
// Returns serialized RuntimeCommandResponse proto bytes.
// Unknown command types return a response with success=false.
std::vector<uint8_t> dispatchCommand(const std::vector<uint8_t>& command_proto);

// Dispatches a RuntimeCommand for a process-local Native executor. The handle
// exists only at the JNI boundary and is never duplicated in the wire message.
std::vector<uint8_t> dispatchCommandWithHandle(
    const std::vector<uint8_t>& command_proto,
    int64_t executor_handle);

// Stops the per-worker FlightResultServer singleton if running.
// Must be called BEFORE Velox runtime shutdown because FlightResultServer
// depends on Velox OutputBufferManager. Idempotent — safe to call if
// the server was never started or already stopped.
void shutdownFlightResultServer();

}  // namespace pangrova::jni
