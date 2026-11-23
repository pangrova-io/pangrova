#pragma once

#include "engine/result/FlightResultServer.h"

namespace pangrova::result {
// Opens a registration claimed by FlightResultServer after exact grant validation.
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> resultBatchStreamReader(
    FlightResultServer::TaskOutputInfo info,
    int64_t maximumResponseBytes,
    int64_t timeoutSeconds,
    const arrow::flight::ServerCallContext* context = nullptr);
}  // namespace pangrova::result
