#pragma once

#include <string>

#include "pangrova/protocol/v2/task.pb.h"

namespace pangrova::resource {

// Reject an object that does not belong to the admitted warehouse and storage authority.
void validateStorageLocation(const std::string& warehouse, const std::string& uri);
void validateStorageLocation(const protocol::v2::ObjectStoreExecutionBinding& binding,
                             const std::string& uri);

// Select exactly one matching catalog and tenant binding before opening any dataset resource.
const protocol::v2::ObjectStoreExecutionBinding& requireWorkerLocalStorage(
    const google::protobuf::RepeatedPtrField<protocol::v2::ObjectStoreExecutionBinding>& bindings,
    const protocol::v2::CatalogExecutionBinding& catalog,
    const std::string& tenant, const std::string& uri);

std::string workerLocalStorage(
    const google::protobuf::RepeatedPtrField<protocol::v2::ObjectStoreExecutionBinding>& bindings,
    const protocol::v2::CatalogExecutionBinding& catalog,
    const std::string& tenant, const std::string& uri);

}  // namespace pangrova::resource
