package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.connector.api.enums.ViewCreateMode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.enums.PangrovaErrorCode;

import java.util.Objects;
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

    /** Makes the conflict decision atomically in this catalog instance. This store is not durable. */
    void create(SchemaTableName viewName, String sql, ViewCreateMode mode) {
        Objects.requireNonNull(viewName, "viewName");
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(mode, "mode");
        switch (mode) {
            case CREATE -> {
                if (viewDefinitions.putIfAbsent(viewName, sql) != null) {
                    throw new PangrovaException(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS,
                            "View already exists: " + viewName);
                }
            }
            case IF_NOT_EXISTS -> viewDefinitions.putIfAbsent(viewName, sql);
            case CREATE_OR_REPLACE -> viewDefinitions.put(viewName, sql);
            default -> throw new IllegalStateException("Unsupported view create mode: " + mode);
        }
        LOG.info("event=view_create_completed catalog={} view={} mode={}", catalogName, viewName, mode);
    }

    /** Removes exactly one current definition; absence is decided by the same atomic operation. */
    void drop(SchemaTableName viewName, boolean ifExists) {
        if (viewDefinitions.remove(viewName) == null && !ifExists) {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "View not found: " + viewName);
        }
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
