package org.pangrova.metadata.api.object;

import org.pangrova.metadata.api.enums.AssetType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * {@link AssetType#TABLE} asset: catalog-managed relational table.
 *
 * <p>Kept structurally aligned with {@link TableMetadata} so existing call sites
 * can lift to / from the asset model via {@link #from(TableMetadata)} and
 * {@link #toTableMetadata()} without information loss. New code should prefer
 * {@code TableAsset} for any path that also handles views, functions, models,
 * or volumes uniformly.</p>
 */
public record TableAsset(
        String catalog,
        String schema,
        String name,
        List<ColumnMetadata> columns,
        Map<String, String> partitionSpec,
        List<String> sortOrder,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public TableAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        columns = columns == null ? List.of() : List.copyOf(columns);
        partitionSpec = partitionSpec == null ? Map.of() : Map.copyOf(partitionSpec);
        sortOrder = sortOrder == null ? List.of() : List.copyOf(sortOrder);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.TABLE;
    }

    /** Lifts a {@link TableMetadata} into the asset model without losing fields. */
    public static TableAsset from(TableMetadata metadata) {
        checkNotNull(metadata, "metadata must not be null");
        Instant created = metadata.createdAt() == null ? Instant.EPOCH : metadata.createdAt();
        Instant updated = metadata.updatedAt() == null ? created : metadata.updatedAt();
        return new TableAsset(
                metadata.catalog(),
                metadata.schema(),
                metadata.table(),
                metadata.columns(),
                metadata.partitionSpec(),
                metadata.sortOrder(),
                metadata.properties(),
                created,
                updated);
    }

    /** Drops back to {@link TableMetadata} for code paths that still consume that record. */
    public TableMetadata toTableMetadata() {
        return new TableMetadata(
                catalog, schema, name,
                columns, partitionSpec, sortOrder, properties,
                createdAt, updatedAt);
    }
}
