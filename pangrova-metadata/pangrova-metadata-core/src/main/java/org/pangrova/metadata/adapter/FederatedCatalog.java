package org.pangrova.metadata.adapter;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;

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

    org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since);

    default void subscribeToChanges(ChangeListener listener) {
    }

    @Override
    void close();
}
