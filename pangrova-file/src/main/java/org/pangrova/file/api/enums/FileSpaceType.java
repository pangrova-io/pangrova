package org.pangrova.file.api.enums;

/**
 * The two user-visible file namespace surfaces.
 *
 * <p>Volumes follow Databricks semantics; stages follow Snowflake semantics.
 * Both share the same internal registry and file index.</p>
 */
public enum FileSpaceType {
    VOLUME,
    STAGE
}
