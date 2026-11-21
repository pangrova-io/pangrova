package org.pangrova.persistence.table.postgresql.provider;

import org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore;
import org.pangrova.persistence.table.postgresql.statistics.JdbcTableControlStatisticsStore;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.persistence.table.postgresql.maintenance.JdbcTableControlMaintenanceStore;
import org.pangrova.persistence.table.api.provider.TableStoreFactory;
import org.pangrova.persistence.table.api.provider.TableStores;

import javax.sql.DataSource;
import java.util.Optional;

/** Creates postgresql repositories for one table authority. */
public final class PostgresTableStoreFactory implements TableStoreFactory {
    @Override
    public int apiVersion() { return API_VERSION; }

    @Override
    public String identifier() {
        return "postgresql";
    }

    @Override
    public TableStores create(Optional<DataSource> dataSource) {
        DataSource source = dataSource.orElseThrow(() -> new IllegalArgumentException(
                "PostgreSQL requires a data source"));
        return new TableStores(
                new JdbcTableControlStore(source),
                new JdbcTableControlStatisticsStore(source),
                new PostgresTablePolicyRepository(source),
                new JdbcTableControlMaintenanceStore(source));
    }
}
