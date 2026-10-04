package org.pangrova.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.write.PaimonNativePhysicalWriteProvider;
import org.pangrova.connector.paimon.write.PaimonPhysicalWriteAllocations;
import org.pangrova.protocol.PhysicalFileWriteReceipts;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Map;

/** Exact output allocation and sole durable SDK publication; no execution of row data. */
final class PaimonWriteLifecycle {
    private final Catalog catalog;
    private final String catalogName;
    private final PaimonCommitNotifier notifier;
    private final PaimonNativePhysicalWriteProvider physical;

    PaimonWriteLifecycle(Catalog catalog, String catalogName, PaimonCommitNotifier notifier) {
        this.catalog = catalog;
        this.catalogName = catalogName;
        this.notifier = notifier;
        this.physical = new PaimonNativePhysicalWriteProvider(catalog, catalogName);
    }

    PaimonNativePhysicalWriteProvider physical() { return physical; }

    PaimonOutputTableHandle create(org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata metadata) {
        PaimonTableHandleFactory.requireWriteDeclarationSupported(metadata.properties());
        var schema = org.pangrova.connector.paimon.schema.PaimonSchemaBuilder.buildPaimonSchema(metadata);
        var id = Identifier.create(metadata.table().schema(), metadata.table().table());
        try {
            catalog.createTable(id, schema, false);
            return PaimonTableHandleFactory.outputFromTable(
                    metadata.table().schema(), metadata.table().table(), catalog.getTable(id));
        } catch (Catalog.TableAlreadyExistException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS,
                    "Table already exists: " + id, failure);
        } catch (Catalog.DatabaseNotExistException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_SCHEMA_NOT_FOUND,
                    "Database not found: " + id.getDatabaseName(), failure);
        } catch (Catalog.TableNotExistException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Failed to load created table: " + id, failure);
        }
    }

    void deleteByFilter(PaimonTableHandle handle, org.pangrova.spi.connector.metadata.DeleteFilter filter) {
        PaimonDeleteByFilterExecutor.execute(load(handle.schemaName(), handle.tableName()),
                filter, handle.schemaName(), handle.tableName());
    }

    PaimonOutputTableHandle insert(PaimonTableHandle handle) {
        Table table = load(handle.schemaName(), handle.tableName());
        PaimonDdlOperations.warnCrossPartitionUpdateIfNeeded(table, handle);
        var output = PaimonTableHandleFactory.outputFromTable(handle.schemaName(), handle.tableName(), table, "INSERT");
        if (table instanceof FileStoreTable fileStore
                && "lance".equalsIgnoreCase(fileStore.options().get("file.format"))) {
            return output.withPhysicalAllocation(
                    PaimonPhysicalWriteAllocations.append(fileStore, output.baseSnapshotId()));
        }
        return output;
    }

    PaimonOutputTableHandle overwrite(PaimonTableHandle handle) {
        return PaimonTableHandleFactory.outputFromTable(handle.schemaName(), handle.tableName(),
                load(handle.schemaName(), handle.tableName()), "OVERWRITE");
    }

    PaimonOutputTableHandle overwrite(PaimonTableHandle handle, Map<String, String> partition) {
        return PaimonTableHandleFactory.outputFromTable(handle.schemaName(), handle.tableName(),
                load(handle.schemaName(), handle.tableName()), partition);
    }

    PaimonOutputTableHandle delete(PaimonTableHandle handle) {
        Table table = load(handle.schemaName(), handle.tableName());
        if (table.primaryKeys().isEmpty()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "DELETE requires a primary key table: " + handle.schemaName() + "." + handle.tableName());
        }
        return PaimonTableHandleFactory.outputFromTable(handle.schemaName(), handle.tableName(), table, "DELETE");
    }

    PaimonOutputTableHandle reconstruct(SchemaTableName tableName, String mode, long baseSnapshotId) {
        if (!List.of("INSERT", "OVERWRITE", "DELETE").contains(mode)) {
            throw new UnsupportedOperationException("Paimon recovery cannot reconstruct write mode: " + mode);
        }
        return PaimonTableHandleFactory.outputFromTable(tableName.schema(), tableName.table(),
                load(tableName.schema(), tableName.table()), mode).withBaseSnapshotId(baseSnapshotId);
    }

    void finish(ConnectorSession session, PaimonOutputTableHandle handle, List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        validateFingerprint(session, handle, fragments, context);
        Table table = load(handle.schemaName(), handle.tableName());
        List<byte[]> sdkFragments = fragments;
        if (handle.nativePhysicalWriteAllocation().isPresent()) {
            var receipts = PhysicalFileWriteReceipts.decode(physicalBinding(context), fragments);
            sdkFragments = physical.prepareCommitMetadata(session, handle, receipts, context);
        }
        String tenant = tenantId(session);
        PaimonDistributedWriteFinisher.finish(table, handle, sdkFragments, context,
                (schema, target, added, removed, snapshot, event, fingerprint) -> notifier.notify(
                        tenant, schema, target, added, removed, snapshot, event, fingerprint));
    }

    private void validateFingerprint(ConnectorSession session, PaimonOutputTableHandle handle,
            List<byte[]> fragments, DistributedWriteCommitContext context) {
        if (handle.baseSnapshotId() != context.baseSnapshotId() || !tenantId(session).equals(context.tenantId())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon commit context does not match the bound write handle");
        }
        String expected = org.pangrova.spi.coordination.WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(context.executionId(),
                        context.tenantId(), catalogName,
                        new SchemaTableName(handle.schemaName(), handle.tableName()).toString(),
                        handle.writeMode(), handle.baseSnapshotId(), context.writeOutputBinding(),
                        context.principal(), context.authorizationReceipt()), fragments);
        if (!expected.equals(context.commitFingerprint())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon write payload does not match durable commit fingerprint");
        }
    }

    private static org.pangrova.protocol.v2.PhysicalFileWriteBinding physicalBinding(
            DistributedWriteCommitContext context) {
        try {
            var binding = WriteOutputBinding.parseFrom(context.writeOutputBinding());
            if (!binding.hasPhysical()) {
                throw new IllegalArgumentException("Durable physical output binding is missing");
            }
            return binding.getPhysical();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed durable physical output binding", failure);
        }
    }

    private static String tenantId(ConnectorSession session) {
        java.util.Objects.requireNonNull(session, "session must not be null");
        String tenant = session.getProperties().get("tenant_id");
        if (tenant == null || tenant.isBlank()) {
            throw new IllegalArgumentException("tenant_id connector session property is required");
        }
        return tenant;
    }

    private Table load(String schema, String table) {
        var id = Identifier.create(schema, table);
        try {
            return catalog.getTable(id);
        } catch (Catalog.TableNotExistException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, failure);
        }
    }
}
