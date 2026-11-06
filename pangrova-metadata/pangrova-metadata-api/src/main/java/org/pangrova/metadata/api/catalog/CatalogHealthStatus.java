package org.pangrova.metadata.api.catalog;

/**
 * Health check result for a catalog handle.
 */
public record CatalogHealthStatus(boolean isHealthy, String message) {

    public static CatalogHealthStatus healthy() {
        return new CatalogHealthStatus(true, "");
    }

    public static CatalogHealthStatus unhealthy(String message) {
        return new CatalogHealthStatus(false, message);
    }
}
