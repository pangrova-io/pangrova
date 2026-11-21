#pragma once

#include <arrow/result.h>
#include <string_view>
#include "pangrova/protocol/v2/result.pb.h"

namespace pangrova::result {
arrow::Result<protocol::v2::ResultReadGrant> authenticateResultRead(std::string_view ticket);
arrow::Status validateResultRead(
    const protocol::v2::ResultReadGrant& grant,
    const protocol::v2::ExecutionCallbackContext& admitted);
}  // namespace pangrova::result
