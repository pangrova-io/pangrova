package org.pangrova.planner.runtimefilter;

import org.pangrova.spi.connector.data.type.PangrovaType;

/**
 * Immutable descriptor for a planned runtime filter produced during join analysis.
 *
 * @param filterId        unique identifier for this filter
 * @param buildColumn     column name on the build side (source of filter values)
 * @param probeColumn     column name on the probe side (target of filtering)
 * @param joinType        the join type (INNER, LEFT, SEMI)
 * @param buildColumnType declared type of the build column for binary encoding
 * @param targetStageId   stage ID of the probe side that this filter targets
 */
public record RuntimeFilterDescriptor(
        String filterId,
        String buildColumn,
        String probeColumn,
        String joinType,
        PangrovaType buildColumnType,
        String targetStageId) {

    public RuntimeFilterDescriptor {
        if (filterId == null || filterId.isBlank()) {
            throw new IllegalArgumentException("filterId must not be null or blank");
        }
        if (buildColumn == null || buildColumn.isBlank()) {
            throw new IllegalArgumentException("buildColumn must not be null or blank");
        }
        if (probeColumn == null || probeColumn.isBlank()) {
            throw new IllegalArgumentException("probeColumn must not be null or blank");
        }
        if (joinType == null || joinType.isBlank()) {
            throw new IllegalArgumentException("joinType must not be null or blank");
        }
        if (buildColumnType == null) {
            throw new IllegalArgumentException("buildColumnType must not be null");
        }
        // targetStageId may be null during planning; resolved later during dispatch
    }

    /** Convenience constructor without targetStageId (resolved later). */
    public RuntimeFilterDescriptor(String filterId, String buildColumn,
                                    String probeColumn, String joinType,
                                    PangrovaType buildColumnType) {
        this(filterId, buildColumn, probeColumn, joinType, buildColumnType, null);
    }
}
