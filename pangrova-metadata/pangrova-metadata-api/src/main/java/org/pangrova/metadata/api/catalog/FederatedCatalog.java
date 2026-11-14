package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.time.Instant;
import java.util.List;

/**
 * SPI for external catalog federation. Adapts an external catalog system
 * into the Pangrova metadata model.
 */
@Experimental
public interface FederatedCatalog extends AutoCloseable {

    List<String> listSchemas();

    List<String> listTables(String schema);

    TableMetadata getTable(String schema, String table);

    TableStatistics getTableStatistics(String schema, String table);

    org.pangrova.metadata.api.catalog.MetadataChangeBatch readChangesSince(Instant since);

    /** Registers push notifications when the provider supports them. Pull-only catalogs reject the request. */
    default void subscribeToChanges(ChangeListener listener) {
        throw new UnsupportedOperationException("Catalog does not support push change notifications");
    }

    @Override
    void close();
}
