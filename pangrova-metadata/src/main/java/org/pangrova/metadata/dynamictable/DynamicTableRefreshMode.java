package org.pangrova.metadata.dynamictable;

/**
 * Refresh mode for a Dynamic Table.
 *
 * <p>This enum mirrors {@code org.pangrova.parser.ast.RefreshMode} to avoid
 * a dependency from pangrova-metadata to pangrova-parser. Conversion between
 * the two is handled at the coordinator boundary.</p>
 */
public enum DynamicTableRefreshMode {

    /** Incremental refresh via snapshot diff (Iceberg append scan, Paimon changelog). */
    SNAPSHOT,

    /** Real-time incremental refresh via CDC (Debezium). */
    CDC,

    /** System auto-selects: prefer CDC, fallback to SNAPSHOT. */
    AUTO
}
