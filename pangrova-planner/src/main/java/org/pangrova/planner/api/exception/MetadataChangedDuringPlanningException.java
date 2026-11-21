package org.pangrova.planner.api.exception;

import org.pangrova.common.api.annotation.Internal;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when metadata for a catalog changes during query planning.
 *
 * <p>This indicates a concurrent DDL or metadata sync modified the schema
 * between the start and end of compilation. The client should retry the query
 * to get a consistent metadata snapshot.</p>
 */
@Internal
public final class MetadataChangedDuringPlanningException extends PlannerException {

    private final String catalogName;

    /**
     * Creates an exception for the catalog whose metadata changed during planning.
     *
     * @param catalogName name of the affected catalog
     */
    public MetadataChangedDuringPlanningException(String catalogName) {
        super(PangrovaErrorCode.METADATA_CHANGED_DURING_PLANNING,
                "Metadata changed during planning for catalog '" + catalogName
                        + "'; retry the query for a consistent snapshot");
        this.catalogName = catalogName;
    }

    public String catalogName() {
        return catalogName;
    }
}
