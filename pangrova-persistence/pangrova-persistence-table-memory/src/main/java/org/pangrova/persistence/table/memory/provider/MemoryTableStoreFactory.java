package org.pangrova.persistence.table.memory.provider;

import org.pangrova.persistence.table.memory.control.InMemoryTableControlStore;
import org.pangrova.persistence.table.memory.statistics.InMemoryTableControlStatisticsStore;
import org.pangrova.persistence.table.memory.governancebinding.InMemoryTableControlGovernanceBindingStore;
import org.pangrova.persistence.table.memory.maintenance.InMemoryTableControlMaintenanceStore;
import org.pangrova.persistence.table.api.provider.TableStoreFactory;
import org.pangrova.persistence.table.api.provider.TableStores;

import javax.sql.DataSource;
import java.util.Optional;

/** Creates memory repositories for one table authority. */
public final class MemoryTableStoreFactory implements TableStoreFactory {
    @Override
    public String identifier() {
        return "memory";
    }

    @Override
    public TableStores create(Optional<DataSource> dataSource) {
        if (dataSource.isPresent()) throw new IllegalArgumentException("Memory table storage cannot use a JDBC data source");
        return new TableStores(
                new InMemoryTableControlStore(),
                new InMemoryTableControlStatisticsStore(),
                new InMemoryTableControlGovernanceBindingStore(),
                new InMemoryTableControlMaintenanceStore());
    }
}
