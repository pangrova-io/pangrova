package org.pangrova.metadata.model.asset;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * {@link AssetType#MATERIALIZED_VIEW} asset: a SQL view whose result is
 * physically stored and refreshed.
 *
 * <p>This is the catalog-side projection of an MV — distinct from the
 * existing operational record {@code org.pangrova.metadata.mv.MaterializedViewMetadata}
 * which only carries lifecycle/refresh-state flags. Callers that need both
 * the catalog identity (for listing / governance) and the runtime state
 * should compose the two records.</p>
 *
 * @param catalog          containing catalog
 * @param schema           containing schema
 * @param name             MV name
 * @param definitionSql    canonical SQL the MV materializes
 * @param columns          projected columns
 * @param dependentTables  base tables read by {@code definitionSql}
 * @param refreshStrategy  refresh strategy identifier (e.g. {@code MANUAL},
 *                         {@code ON_SCHEDULE}, {@code INCREMENTAL})
 * @param storageLocation  physical location of the materialized data (may be
 *                         {@code ""} when the connector hides storage)
 * @param lastRefreshedAt  last successful refresh timestamp; {@code null} if
 *                         never refreshed
 * @param properties       free-form properties
 * @param createdAt        creation timestamp
 * @param updatedAt        last-update timestamp
 */
public record MaterializedViewAsset(
        String catalog,
        String schema,
        String name,
        String definitionSql,
        List<ColumnMetadata> columns,
        List<QualifiedTableRef> dependentTables,
        String refreshStrategy,
        String storageLocation,
        Instant lastRefreshedAt,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public MaterializedViewAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(definitionSql, "definitionSql must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        columns = columns == null ? List.of() : List.copyOf(columns);
        dependentTables = dependentTables == null ? List.of() : List.copyOf(dependentTables);
        refreshStrategy = refreshStrategy == null ? "MANUAL" : refreshStrategy;
        storageLocation = storageLocation == null ? "" : storageLocation;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.MATERIALIZED_VIEW;
    }
}
