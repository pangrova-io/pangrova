package org.pangrova.metadata.api;

import org.pangrova.metadata.api.binding.QueryMetadataLifecyclePort;
import org.pangrova.metadata.api.binding.TableResolutionPort;
import org.pangrova.metadata.api.catalog.CatalogLifecyclePort;
import org.pangrova.metadata.api.control.ColumnTagPort;
import org.pangrova.metadata.api.mutation.TableDdlPort;

import java.util.Objects;

/**
 * Aggregates the small metadata use-case ports. Consumers should depend on the
 * specific port they use; this record exists only for composition roots.
 */
public record MetadataGateway(
        CatalogLifecyclePort catalogs,
        TableDdlPort tables,
        TableResolutionPort resolution,
        QueryMetadataLifecyclePort queries,
        ColumnTagPort tags) {

    public MetadataGateway {
        Objects.requireNonNull(catalogs, "catalogs");
        Objects.requireNonNull(tables, "tables");
        Objects.requireNonNull(resolution, "resolution");
        Objects.requireNonNull(queries, "queries");
        Objects.requireNonNull(tags, "tags");
    }
}
