package org.pangrova.server.coordinator.http.endpoint.catalog;

import java.util.List;
import java.util.Map;

/**
 * Request body for POST /v1/namespaces — creates a new namespace.
 *
 * @param namespace  multi-level namespace identifier (e.g., ["catalog", "schema"])
 * @param properties optional namespace properties
 */
public record CreateNamespaceRequest(
        List<String> namespace,
        Map<String, String> properties) {

    public CreateNamespaceRequest {
        namespace = namespace == null ? List.of() : List.copyOf(namespace);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }
}
