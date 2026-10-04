package org.pangrova.planner.cost;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import org.pangrova.planner.stats.ColumnStatistics;

import java.util.Map;

/**
 * Provides index-derived column statistics for cost estimation.
 *
 * <p>Implemented by the coordinator layer to bridge {@code IndexMetadataStore}
 * file-level statistics into the planner's cost model. The planner module
 * cannot depend on metadata directly, so this interface acts as the
 * anti-corruption boundary.</p>
 */
@FunctionalInterface
public interface IndexStatisticsProvider {

    /**
     * Returns aggregated column statistics derived from index builds.
     *
     * @param tableName fully qualified table name
     * @return column name → aggregated statistics; empty map if unavailable
     */
    Map<String, ColumnStatistics> getIndexDerivedStatistics(QualifiedTableName tableName);
}
