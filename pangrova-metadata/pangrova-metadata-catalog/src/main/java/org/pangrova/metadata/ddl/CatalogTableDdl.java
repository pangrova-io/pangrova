package org.pangrova.metadata.ddl;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.exception.MetadataException;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.api.schema.SchemaValidationResult;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.DefaultConnectorSession;
import org.pangrova.metadata.resolution.CatalogTableResolution;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.types.enums.PangrovaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Performs connector DDL with current table authority, then invalidates resolutions and emits events. */
public final class CatalogTableDdl {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogTableDdl.class);

    private final CatalogRegistry catalogRegistry;
    private final CatalogTableResolution resolution;
    private final MetadataEventDispatcher eventDispatcher;
    private final SchemaEvolutionValidator schemaValidator;
    private final Object alterTableLock = new Object();

    public CatalogTableDdl(CatalogRegistry catalogRegistry, CatalogTableResolution resolution,
            MetadataEventDispatcher eventDispatcher, SchemaEvolutionValidator schemaValidator) {
        this.catalogRegistry = catalogRegistry;
        this.resolution = resolution;
        this.eventDispatcher = eventDispatcher;
        this.schemaValidator = schemaValidator;
    }

    public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
        catalogRegistry.tableControl().registrations().requireCreatableName(tableRef.catalog(), tableRef.schema(), tableRef.table());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        List<String> columnNames = metadata.columns().stream().map(ColumnMetadata::name).toList();
        var typedColumns = metadata.columns().stream().map(MetadataColumnDeclarations::connectorColumn).toList();
        ConnectorTableMetadata connTableMeta = new ConnectorTableMetadata(
                schemaTable, columnNames, typedColumns, metadata.properties());
        var descriptor = catalogHandle.getDescriptor();
        connMetadata.createTable(connSession, connTableMeta);
        registerCreatedTable(descriptor, connMetadata, connSession, tableRef, session.user());
        resolution.invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableCreatedEvent(tableRef, metadata));
        LOG.info("event=table_created catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
        var tableControl = catalogRegistry.tableControl();
        tableControl.requireDroppable(
                tableRef.catalog(), tableRef.schema(), tableRef.table(), "DROP TABLE");
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        if (tableControl.enforcing()) {
            tableControl.requireDroppable(
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), "DROP TABLE",
                    connMetadata.getTableIdentity(connSession, tableHandle));
        }
        var record = tableControl.find(
                tableRef.catalog(), tableRef.schema(), tableRef.table());
        if (tableControl.enabled()) {
            record.ifPresent(captured -> tableControl.requireCurrentCatalog(captured, "DROP TABLE"));
        }
        connMetadata.dropTable(connSession, tableHandle);
        record.ifPresent(tableControl.registrations()::markDropped);
        resolution.invalidateTableCaches(tableRef);
        eventDispatcher.dispatch(new MetadataEvent.TableDroppedEvent(tableRef));
        LOG.info("event=table_dropped catalog={} schema={} table={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table());
    }

    public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        synchronized (alterTableLock) {
            catalogRegistry.tableControl().requireManagedForDdl(
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), "ALTER TABLE");
            TableMetadata current = resolution.currentMetadata(session, tableRef);
            validateSchemaChanges(current, changes);
            applySchemaChangesToConnector(session, tableRef, changes);
            // Re-read connector authority, not the unscoped inventory projection.
            resolution.currentMetadata(session, tableRef);
            resolution.invalidateTableCaches(tableRef);
            eventDispatcher.dispatch(new MetadataEvent.TableAlteredEvent(tableRef, changes));
        }
    }

    private void registerCreatedTable(org.pangrova.metadata.api.catalog.CatalogDescriptor descriptor,
            ConnectorMetadata metadata, ConnectorSession connSession,
            QualifiedTableRef tableRef, String owner) {
        var tableControl = catalogRegistry.tableControl();
        try {
            tableControl.registrations().registerCreatedFromConnector(metadata, connSession,
                    tableRef.catalog(), tableRef.schema(), tableRef.table(), owner,
                    new org.pangrova.table.api.identity.CatalogDeploymentIdentity(
                            descriptor.incarnationId(), descriptor.revision()));
        } catch (RuntimeException failure) {
            throw new MetadataException(PangrovaErrorCode.METADATA_METASTORE_ERROR,
                    "Table was created but its control record could not be registered: " + tableRef,
                    failure);
        }
    }

    private void applySchemaChangesToConnector(
            MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(tableRef.catalog());
        ConnectorSession connSession = DefaultConnectorSession.fromSessionContext(
                session, tableRef.catalog(), tableRef.schema());
        ConnectorMetadata connMetadata = catalogHandle.getMetadata(connSession);
        SchemaTableName schemaTable = new SchemaTableName(tableRef.schema(), tableRef.table());
        ConnectorTableHandle tableHandle = connMetadata.getTableHandle(connSession, schemaTable);
        if (tableHandle == null) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                    "Table not found: " + tableRef);
        }
        for (SchemaChange change : changes) {
            if (catalogRegistry.tableControl().enforcing()) {
                catalogRegistry.tableControl().requireManagedForDdl(
                        tableRef.catalog(), tableRef.schema(), tableRef.table(), "ALTER TABLE",
                        connMetadata.getTableIdentity(connSession, tableHandle));
            }
            applySchemaChange(connMetadata, connSession, tableHandle, change);
        }
        LOG.info("event=alter_table_connector_propagated catalog={} schema={} table={} changes={}",
                tableRef.catalog(), tableRef.schema(), tableRef.table(), changes.size());
    }

    private static void applySchemaChange(
            ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle table, SchemaChange change) {
        if (change instanceof SchemaChange.AddColumn add) {
            metadata.addColumn(session, table, add.name(), PangrovaType.fromSqlTypeName(add.type().baseType()));
        } else if (change instanceof SchemaChange.DropColumn drop) {
            metadata.dropColumn(session, table, drop.name());
        } else if (change instanceof SchemaChange.RenameColumn rename) {
            metadata.renameColumn(session, table, rename.oldName(), rename.newName());
        } else if (change instanceof SchemaChange.ChangeType changeType) {
            metadata.changeColumnType(session, table, changeType.name(),
                    PangrovaType.fromSqlTypeName(changeType.newType().baseType()));
        }
    }

    private void validateSchemaChanges(TableMetadata current, List<SchemaChange> changes) {
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.AddColumn add && !add.nullable()) {
                throw new MetadataException(
                        PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                        "Adding a NOT NULL column is not supported by the connector metadata SPI: " + add.name());
            }
            if (change instanceof SchemaChange.ChangeNullability nullability) {
                throw new MetadataException(
                        PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                        "Changing column nullability is not supported by the connector metadata SPI: "
                                + nullability.name());
            }
        }
        SchemaValidationResult result = schemaValidator.validate(current, changes);
        if (!result.compatible()) {
            throw new MetadataException(
                    PangrovaErrorCode.METADATA_SCHEMA_INCOMPATIBLE,
                    "Schema changes incompatible: " + result.errors());
        }
    }
}
