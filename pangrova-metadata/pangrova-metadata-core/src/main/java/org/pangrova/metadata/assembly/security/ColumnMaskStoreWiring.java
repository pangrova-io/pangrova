package org.pangrova.metadata.assembly.security;

import org.pangrova.metadata.adapter.security.TableControlColumnMaskStore;

import java.util.Objects;

import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.service.RegistryTablePolicyAuthority;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;

/**
 * Metadata owner wiring entry for the column-mask projection onto stable table identities.
 * Composition roots obtain the decorated store here rather than constructing it directly.
 */
public final class ColumnMaskStoreWiring {

    private ColumnMaskStoreWiring() {
    }

    /** Wraps the security store with metadata's stable table-identity projection. */
    public static EnrichedColumnMaskStore tableControl(
            EnrichedColumnMaskStore delegate, CatalogRegistry catalogs) {
        return new TableControlColumnMaskStore(
                new RegistryTablePolicyAuthority(Objects.requireNonNull(catalogs, "catalogs")),
                Objects.requireNonNull(delegate, "delegate"));
    }
}
