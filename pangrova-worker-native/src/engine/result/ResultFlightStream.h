#pragma once

#include "engine/result/FlightResultServer.h"

namespace pangrova::result {

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> resultFlightStream(
    FlightResultServer::TaskOutputInfo info, int64_t timeoutSeconds,
    const arrow::flight::ServerCallContext* context);

}  // namespace pangrova::result
