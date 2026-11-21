package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

import java.util.Map;

/**
 * Thrown when metadata resolution fails due to backend unavailability or connector errors.
 * Distinguishes "metadata service unavailable" from "table not found" to prevent semantic drift.
 *
 * <p>Carries catalog and schema context so callers can identify the failing scope.
 * The multi-catalog constructor aggregates failures from a query lifecycle operation
 * (beginQuery/cleanupQuery) across multiple catalogs.</p>
 */
public class MetadataResolutionException extends MetadataException {

    private final String catalogName;
    private final String schemaName;
    private final Map<String, Exception> catalogFailures;

    /** Preserves the backend failure while identifying its catalog and schema. */
    public MetadataResolutionException(String catalogName, String schemaName, Throwable cause) {
        super(PangrovaErrorCode.METADATA_RESOLUTION_FAILED,
                String.format("Metadata resolution failed for catalog=%s, schema=%s: %s",
                        catalogName, schemaName, cause.getMessage()),
                cause);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.catalogFailures = Map.of();
    }

    /** Preserves the backend failure while identifying the requested table. */
    public MetadataResolutionException(String catalogName, String schemaName, String tableName, Throwable cause) {
        super(PangrovaErrorCode.METADATA_RESOLUTION_FAILED,
                String.format("Metadata resolution failed for catalog=%s, schema=%s, table=%s: %s",
                        catalogName, schemaName, tableName, cause.getMessage()),
                cause);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.catalogFailures = Map.of();
    }

    /**
     * Constructs an aggregated failure for a query lifecycle operation across multiple catalogs.
     *
     * @param queryId         the query identifier
     * @param operation       the lifecycle operation name (e.g. "beginQuery", "cleanupQuery")
     * @param catalogFailures per-catalog failure map (catalog name to exception)
     */
    public MetadataResolutionException(String queryId, String operation, Map<String, Exception> catalogFailures) {
        super(PangrovaErrorCode.METADATA_RESOLUTION_FAILED,
                String.format("Query lifecycle %s failed for queryId=%s, failedCatalogs=%s",
                        operation, queryId, catalogFailures.keySet()));
        this.catalogName = null;
        this.schemaName = null;
        this.catalogFailures = Map.copyOf(catalogFailures);
        catalogFailures.values().forEach(this::addSuppressed);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Map<String, Exception> getCatalogFailures() {
        return catalogFailures;
    }
}
