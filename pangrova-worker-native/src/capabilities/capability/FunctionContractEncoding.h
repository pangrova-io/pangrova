#pragma once

#include <string>

#include "google/protobuf/message.h"

namespace pangrova::capability {

std::string encodeFunctionContract(const google::protobuf::Message& message);
std::string functionContractDigest(const google::protobuf::Message& message);

}  // namespace pangrova::capability
