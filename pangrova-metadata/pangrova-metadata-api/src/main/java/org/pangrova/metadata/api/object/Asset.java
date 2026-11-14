package org.pangrova.metadata.api.object;

import org.pangrova.metadata.api.enums.AssetType;

import org.pangrova.common.api.annotation.Experimental;
import java.time.Instant;
import java.util.Map;

/**
 * Common contract for any catalog-managed asset.
 *
 * <p>Pangrova catalogs historically only modelled tables. This sealed interface
 * widens the metadata layer so views, materialized views, functions, ML models,
 * and volumes are first-class siblings — discoverable through a single registry,
 * usable in lineage / governance / sync pipelines, and addressable by the same
 * three-part identity ({@code catalog.schema.name}).</p>
 *
 * <p>All implementing records are immutable and may be safely cached and
 * serialized.</p>
 *
 * <p>Resolution: the tuple {@code (catalog, schema, name)} together with
 * {@link #type()} uniquely identifies an asset within a deployment. Two assets
 * of different types may share the same {@code (catalog, schema, name)} when
 * the underlying connector permits it (the namespace overlap question is
 * connector-specific — Iceberg/Paimon enforce table-name uniqueness, while
 * function and model namespaces are typically separate).</p>
 *
 * @see AssetType
 */
@Experimental
public sealed interface Asset
        permits TableAsset, ViewAsset, MaterializedViewAsset,
                FunctionAsset, ModelAsset, VolumeAsset {

    /** The asset kind (used for fan-out and serialization tagging). */
    AssetType type();

    /** Catalog the asset lives in (top-level namespace). */
    String catalog();

    /** Schema within the catalog. */
    String schema();

    /** Asset name (table / view / function / model / volume identifier). */
    String name();

    /** Free-form properties attached to the asset (owner, description, tags, etc.). */
    Map<String, String> properties();

    /** Creation timestamp, never null. */
    Instant createdAt();

    /** Last-update timestamp; never null (use {@link #createdAt()} for never-modified). */
    Instant updatedAt();
}
