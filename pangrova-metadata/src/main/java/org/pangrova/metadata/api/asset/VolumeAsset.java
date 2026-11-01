package org.pangrova.metadata.api.asset;

import java.time.Instant;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * {@link AssetType#VOLUME} asset: a named handle to a directory of files
 * (Parquet, CSV, JSON, ML artifacts, configs, etc.) addressable from SQL
 * without a relational schema.
 *
 * <p>Volumes bridge the gap between table-oriented catalogs and raw file
 * storage. A volume is a logical mount-point that exposes a storage location
 * as a first-class catalog object, enabling governance (ACLs, audit, lineage)
 * and discoverability via the same catalog APIs used for tables and views.</p>
 *
 * <p>This models the concepts found in Databricks Unity Catalog volumes and
 * Snowflake directory tables.</p>
 *
 * @param catalog          containing catalog
 * @param schema           containing schema
 * @param name             volume name
 * @param volumeType       {@link VolumeType#MANAGED} or {@code EXTERNAL}
 * @param storageLocation  root storage URI (e.g.
 *                         {@code s3://bucket/path/} or {@code /mnt/data/})
 * @param properties       free-form properties (description, tags, etc.)
 * @param createdAt        creation timestamp
 * @param updatedAt        last-update timestamp
 */
public record VolumeAsset(
        String catalog,
        String schema,
        String name,
        VolumeType volumeType,
        String storageLocation,
        Map<String, String> properties,
        Instant createdAt,
        Instant updatedAt) implements Asset {

    public VolumeAsset {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(name, "name must not be null");
        checkNotNull(createdAt, "createdAt must not be null");
        checkNotNull(updatedAt, "updatedAt must not be null");
        volumeType = volumeType == null ? VolumeType.MANAGED : volumeType;
        storageLocation = storageLocation == null ? "" : storageLocation;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @Override
    public AssetType type() {
        return AssetType.VOLUME;
    }

    /** Volume storage management mode. */
    public enum VolumeType {
        /** Catalog manages the storage lifecycle (create, delete on DROP). */
        MANAGED,
        /** Catalog only tracks the external location; storage is externally managed. */
        EXTERNAL
    }
}
