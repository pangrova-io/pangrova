package org.pangrova.connector.iceberg;

import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory store for view definitions registered through the connector
 * (views are tracked per catalog and survive until dropped or the connector
 * shuts down).
 */
final class IcebergViewStore {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergViewStore.class);

    private final String catalogName;
    private final ConcurrentHashMap<SchemaTableName, String> viewDefinitions = new ConcurrentHashMap<>();

    IcebergViewStore(String catalogName) {
        this.catalogName = catalogName;
    }

    /** Registers or replaces the SQL definition of a view. */
    void create(SchemaTableName viewName, String sql) {
        viewDefinitions.put(viewName, sql);
        LOG.info("event=view_created catalog={} view={}", catalogName, viewName);
    }

    /** Removes the definition of a view. */
    void drop(SchemaTableName viewName) {
        viewDefinitions.remove(viewName);
        LOG.info("event=view_dropped catalog={} view={}", catalogName, viewName);
    }

    /** Lists views, optionally restricted to a single schema. */
    List<SchemaTableName> list(Optional<String> schemaName) {
        return viewDefinitions.keySet().stream()
                .filter(name -> schemaName.isEmpty()
                        || name.schema().equals(schemaName.get()))
                .toList();
    }

    /** Returns the SQL definition of a view, or empty when unknown. */
    Optional<String> definition(SchemaTableName viewName) {
        return Optional.ofNullable(viewDefinitions.get(viewName));
    }
}
