// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// Dispatches NativeCommand proto to the appropriate handler.
// Each command type is handled by a dedicated function for single responsibility.
// Returns serialized NativeCommandResponse proto bytes.

#pragma once

#include <cstdint>
#include <vector>

namespace pangrova::jni {

// Dispatches a serialized NativeCommand proto to the appropriate handler.
// Returns serialized NativeCommandResponse proto bytes.
// Unknown command types return a response with success=false.
std::vector<uint8_t> dispatchCommand(const std::vector<uint8_t>& command_proto);

// Dispatches a serialized NativeCommand proto with a timeout in milliseconds.
// Returns an error response if the command exceeds the deadline.
// A timeout_ms of 0 means no timeout (equivalent to dispatchCommand).
std::vector<uint8_t> dispatchCommandWithTimeout(
    const std::vector<uint8_t>& command_proto,
    int64_t timeout_ms);

// Dispatches a serialized NativeCommand proto, overriding the executor_handle
// field with the provided JNI-authoritative handle value.
// This ensures the JNI parameter is the single source of truth for routing,
// preventing mismatches between the JNI argument and the proto payload.
std::vector<uint8_t> dispatchCommandWithHandle(
    const std::vector<uint8_t>& command_proto,
    int64_t executor_handle);

// Dispatches with both handle override and timeout.
std::vector<uint8_t> dispatchCommandWithHandleAndTimeout(
    const std::vector<uint8_t>& command_proto,
    int64_t executor_handle,
    int64_t timeout_ms);

// Stops the per-worker FlightResultServer singleton if running.
// Must be called BEFORE Velox runtime shutdown because FlightResultServer
// depends on Velox OutputBufferManager. Idempotent — safe to call if
// the server was never started or already stopped.
void shutdownFlightResultServer();

// Stops and removes the per-executor FlightShuffleServer for the given handle.
// Called from nativeReleaseExecutor to prevent port and memory leaks when
// Java side releases an executor without explicitly stopping its shuffle server.
// Idempotent — safe to call if no server exists for this handle.
void cleanupShuffleServerForHandle(int64_t executor_handle);

}  // namespace pangrova::jni
