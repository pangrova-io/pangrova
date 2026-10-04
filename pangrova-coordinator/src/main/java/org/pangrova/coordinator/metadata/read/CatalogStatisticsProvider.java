package org.pangrova.coordinator.metadata.read;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import org.pangrova.planner.stats.StatisticsCollector.ConnectorStatisticsProvider;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges query-local {@link PlanningReadBindings} to {@link ConnectorStatisticsProvider},
 * enabling the planner's {@link org.pangrova.planner.stats.StatisticsCollector}
 * to read real statistics from Iceberg/Paimon connectors.
 *
 * <p>Resolved catalog, schema and table components are retained separately; display
 * strings are never parsed into a statistics authority.</p>
 */
public final class CatalogStatisticsProvider implements ConnectorStatisticsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogStatisticsProvider.class);

    private final PlanningReadBindings readBindings;

    public CatalogStatisticsProvider(PlanningReadBindings readBindings) {
        this.readBindings = java.util.Objects.requireNonNull(readBindings, "readBindings");
    }

    @Override
    public TableStatistics getTableStatistics(QualifiedTableName tableName) {
        java.util.Objects.requireNonNull(tableName, "tableName");
        var catalogName = tableName.catalog();
        var schemaName = tableName.schema();
        var table = tableName.table();
        var binding = readBindings.resolve(catalogName, schemaName, table);
        if (binding.isEmpty()) {
            LOG.debug("event=stats_lookup_miss tableName={}", tableName);
            return TableStatistics.UNKNOWN;
        }
        TableStatistics stats = binding.orElseThrow().statistics();
        LOG.debug("event=stats_lookup_hit tableName={} rowCount={} dataSize={}",
                tableName, stats.rowCount(), stats.dataSize());
        return stats;
    }
}
