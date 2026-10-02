package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.stats.StatisticsCollector.ConnectorStatisticsProvider;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges {@link CatalogRegistry} to {@link ConnectorStatisticsProvider},
 * enabling the planner's {@link org.pangrova.planner.stats.StatisticsCollector}
 * to read real statistics from Iceberg/Paimon connectors.
 *
 * <p>Table name format: "catalogName.schemaName.tableName" (fully qualified).</p>
 */
public final class CatalogStatisticsProvider implements ConnectorStatisticsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogStatisticsProvider.class);

    private final CatalogRegistry catalogRegistry;
    private final ConnectorSession authenticatedSession;

    public CatalogStatisticsProvider(CatalogRegistry catalogRegistry, ConnectorSession session) {
        this.catalogRegistry = catalogRegistry;
        this.authenticatedSession = PlanningConnectorSession.bind(session,
                session.getCatalog().orElse(null), session.getSchema().orElse(null));
    }

    @Override
    public TableStatistics getTableStatistics(String tableName) {
        String[] parts = tableName.split("\\.", 3);
        if (parts.length < 3) {
            LOG.debug("event=stats_lookup_skip reason=unqualified_name tableName={}", tableName);
            return TableStatistics.UNKNOWN;
        }
        try {
            return fetchStatistics(parts[0], parts[1], parts[2], tableName);
        } catch (Exception e) {
            throw new org.pangrova.metadata.exception.MetadataResolutionException(
                    parts[0], parts[1], parts[2], e);
        }
    }

    private TableStatistics fetchStatistics(
            String catalogName, String schemaName, String table, String fullName) {
        CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
        ConnectorSession session = PlanningConnectorSession.bind(authenticatedSession, catalogName, schemaName);
        ConnectorTableHandle tableHandle = handle.getMetadata(session)
                .getTableHandle(session, new SchemaTableName(schemaName, table));
        if (tableHandle == null) {
            LOG.debug("event=stats_lookup_miss tableName={}", fullName);
            return TableStatistics.UNKNOWN;
        }
        TableStatistics stats = handle.getMetadata(session)
                .getTableStatistics(session, tableHandle);
        LOG.debug("event=stats_lookup_hit tableName={} rowCount={} dataSize={}",
                fullName, stats.rowCount(), stats.dataSize());
        return stats;
    }
}
