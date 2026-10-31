package org.pangrova.coordinator.http.endpoint.catalog;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /v1/namespaces/{namespace}/properties — updates namespace properties.
 *
 * @param removals   property keys to remove
 * @param updates    property key-value pairs to add or update
 */
public record UpdateNamespacePropertiesRequest(
        List<String> removals,
        Map<String, String> updates) {

    public UpdateNamespacePropertiesRequest {
        removals = removals == null ? List.of() : List.copyOf(removals);
        updates = updates == null ? Map.of() : Map.copyOf(updates);
    }
}
