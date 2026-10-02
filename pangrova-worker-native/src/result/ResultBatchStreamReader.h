#pragma once

#include "result/FlightResultServer.h"

namespace pangrova::result {
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> resultBatchStreamReader(
    FlightResultServer::TaskOutputInfo info,
    protocol::v2::ResultReadGrant grant,
    int64_t maximumResponseBytes,
    int64_t timeoutSeconds);
}  // namespace pangrova::result
