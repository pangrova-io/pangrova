package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

/** Exact durable catalog deployment; secrets are not part of this identity. */
@Experimental
public record SourceCatalogDeployment(String catalog, String incarnation, long revision) {
    public SourceCatalogDeployment {
        text(catalog);
        text(incarnation);
        if (revision <= 0) throw new IllegalArgumentException("Invalid source catalog revision");
    }
    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }
}
