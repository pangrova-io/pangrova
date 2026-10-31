package org.pangrova.connector.spi.metadata;

import java.util.List;
import java.util.Optional;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.TableStatistics;

/**
 * Planning operations that connectors may absorb into table scans.
 *
 * <p>Split out of {@link ConnectorMetadata}, which extends it, so every method here
 * is already part of the public SPI surface. The type is therefore {@code public}
 * too: a connector author must be able to name it to document or override these
 * operations selectively.</p>
 *
 * <p>Marked {@code @Experimental} like the other extracted mix-ins
 * ({@code ConnectorSchemaMetadata}, {@code ConnectorSnapshotMetadata},
 * {@code ConnectorIndexMetadata}, {@code ConnectorQueryLifecycle}): the pushdown
 * surface still gains operations. Every method is a {@code default} returning
 * {@link Optional#empty()}, so adding one does not break existing connectors.</p>
 */
@Experimental
public interface ConnectorPushdownMetadata {

    /**
     * Attempts to push a filter constraint into the connector's table scan.
     *
     * @param session    the current session context
     * @param table      the table handle
     * @param constraint the filter constraint to push down
     * @return a new table handle with the constraint absorbed, or empty if not supported
     */
    default Optional<ConnectorTableHandle> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
        return Optional.empty();
    }

    /**
     * Attempts to push a row limit into the connector's table scan.
     *
     * @param session the current session context
     * @param table   the table handle
     * @param limit   the maximum number of rows to return
     * @return a new table handle with the limit absorbed, or empty if not supported
     */
    default Optional<ConnectorTableHandle> applyLimit(
            ConnectorSession session, ConnectorTableHandle table, long limit) {
        return Optional.empty();
    }

    /**
     * Attempts to push a top-N operation into the connector's table scan.
     *
     * @param session the current session context
     * @param table   the table handle
     * @param topN    the top-N specification
     * @return a new table handle with the top-N absorbed, or empty if not supported
     */
    default Optional<ConnectorTableHandle> applyTopN(
            ConnectorSession session, ConnectorTableHandle table, TopNInfo topN) {
        return Optional.empty();
    }

    /**
     * Attempts to push a table sample into the connector's scan.
     *
     * @param session the current session context
     * @param table   the table handle
     * @param sample  the sample specification
     * @return a new table handle with the sample absorbed, or empty if not supported
     */
    default Optional<ApplicationResult<ConnectorTableHandle>> applySample(
            ConnectorSession session, ConnectorTableHandle table, SampleInfo sample) {
        return Optional.empty();
    }

    /**
     * Collects statistics for the given table.
     *
     * @param session       the current session context
     * @param table         the table handle
     * @param columns       columns to analyze
     * @param samplePercent percentage of data to sample
     * @return computed statistics, or empty if not supported
     */
    default Optional<TableStatistics> analyzeTable(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<String> columns,
            double samplePercent) {
        return Optional.empty();
    }

    /**
     * Attempts to push an aggregation into the connector's scan.
     *
     * @param session     the current session context
     * @param table       the table handle
     * @param aggregation the aggregation to push down
     * @return a new table handle with the aggregation absorbed, or empty if not supported
     */
    default Optional<ApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            AggregationApplication aggregation) {
        return Optional.empty();
    }
}
