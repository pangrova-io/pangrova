package org.pangrova.protocol.rest.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

/** Exact unresolved catalog revision fetched by an authenticated Worker. */
public record CatalogDeploymentResponse(
        @JsonProperty("catalog_name") String catalogName,
        @JsonProperty("connector_provider") String connectorProvider,
        @JsonProperty("catalog_incarnation_id") String catalogIncarnationId,
        @JsonProperty("catalog_revision") long catalogRevision,
        @JsonProperty("properties") Map<String, String> properties,
        @JsonProperty("allowed_tenants") Set<String> allowedTenants) {

    public CatalogDeploymentResponse {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        allowedTenants = allowedTenants == null ? Set.of() : Set.copyOf(allowedTenants);
    }
}
