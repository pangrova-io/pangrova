package org.pangrova.metadata.core.control;

/** Catalog authority captured by a controlled table, independent of catalog implementation details. */
public record CatalogDeploymentIdentity(String incarnationId, long revision) {
    public CatalogDeploymentIdentity {
        if (incarnationId == null || incarnationId.isBlank()) {
            throw new IllegalArgumentException("incarnationId must not be blank");
        }
        if (revision <= 0) {
            throw new IllegalArgumentException("revision must be positive");
        }
    }
}
