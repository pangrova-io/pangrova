package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;
import java.util.Objects;

/** Immutable identity of one deployment revision; a reusable catalog name alone is not authority. */
@Experimental
public record CatalogDeploymentIdentity(String catalogName, String incarnationId, long revision) {
    public CatalogDeploymentIdentity {
        if (Objects.requireNonNull(catalogName, "catalogName").isBlank()
                || Objects.requireNonNull(incarnationId, "incarnationId").isBlank() || revision < 1) {
            throw new IllegalArgumentException(
                    "A deployment identity requires a catalog, incarnation and positive revision");
        }
    }
}
