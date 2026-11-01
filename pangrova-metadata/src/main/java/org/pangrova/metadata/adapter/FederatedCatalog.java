package org.pangrova.metadata.adapter;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.time.Instant;
import java.util.List;

/**
 * SPI for external catalog federation. Adapts an external catalog system
 * into the Pangrova metadata model.
 */
public interface FederatedCatalog extends AutoCloseable {

    List<String> listSchemas();

    List<String> listTables(String schema);

    TableMetadata getTable(String schema, String table);

    TableStatistics getTableStatistics(String schema, String table);

    List<MetadataChange> getChangesSince(Instant since);

    default void subscribeToChanges(ChangeListener listener) {
    }

    @Override
    void close();
}
