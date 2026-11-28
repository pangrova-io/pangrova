package org.pangrova.connector.paimon;

import org.pangrova.connector.paimon.handle.PaimonTableIdentity;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.write.PaimonNativePhysicalWriteProvider;
import org.pangrova.connector.paimon.write.PaimonPhysicalWriteAllocations;
import org.pangrova.protocol.write.PhysicalFileWriteReceipts;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

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

    PaimonOutputTableHandle create(org.pangrova.connector.api.metadata.table.ConnectorTableMetadata metadata) {
        PaimonTableHandleFactory.requireWriteDeclarationSupported(metadata.properties());
        var schema = org.pangrova.connector.paimon.schema.PaimonSchemaBuilder.buildPaimonSchema(metadata);
        var id = Identifier.create(metadata.table().schema(), metadata.table().table());
        try {
            catalog.createTable(id, schema, false);
            return PaimonTableHandleFactory.outputFromTable(
                    metadata.table().schema(), metadata.table().table(), PaimonTableIdentity.loadFresh(catalog, id));
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

    void deleteByFilter(PaimonTableHandle handle, org.pangrova.connector.api.metadata.DeleteFilter filter) {
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

    PaimonOutputTableHandle merge(PaimonTableHandle handle) {
        Table table = load(handle.schemaName(), handle.tableName());
        if (table.primaryKeys().isEmpty()) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "CDC MERGE requires a primary key table: "
                            + handle.schemaName() + "." + handle.tableName());
        }
        return PaimonTableHandleFactory.outputFromTable(
                handle.schemaName(), handle.tableName(), table, "MERGE");
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

    void finish(ConnectorSession session, PaimonOutputTableHandle handle, List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        byte[] bindingBytes = context.writeOutputBinding();
        WriteOutputBinding binding = parseWriteBinding(bindingBytes);
        PaimonOutputTableHandle journaled = boundHandle(binding);
        requireSameTarget(handle, journaled);
        String tenant = tenantId(session);
        validateFingerprint(tenant, journaled, fragments, context, bindingBytes);
        var identifier = Identifier.create(journaled.schemaName(), journaled.tableName());
        Table table;
        try {
            table = PaimonTableIdentity.loadFresh(catalog, identifier);
        } catch (Catalog.TableNotExistException failure) {
            throw new org.pangrova.connector.api.exception.SourceTableUnavailableException(
                    "Paimon publication target is absent: " + identifier, failure);
        }
        PaimonTableIdentity.requireSame(
                table, journaled.tableUuid(), journaled.tableLocation(), journaled.schemaFingerprint(),
                "Paimon distributed write");
        List<byte[]> sdkFragments = fragments;
        if (journaled.nativePhysicalWriteAllocation().isPresent()) {
            if (!binding.hasPhysical()) {
                throw new IllegalArgumentException("Durable physical output binding is missing");
            }
            var receipts = PhysicalFileWriteReceipts.decode(binding.getPhysical(), fragments);
            sdkFragments = physical.prepareCommitMetadata(session, journaled, receipts, context);
        }
        PaimonDistributedWriteFinisher.finish(table, journaled, sdkFragments, context,
                (schema, target, added, removed, snapshot, event, fingerprint) -> notifier.notify(
                        tenant, schema, target, added, removed, snapshot, event, fingerprint));
    }

    /**
     * The journaled output binding is the authoritative write identity for a first attempt and
     * crash recovery. Reconstructed handles may only restate the same target.
     */
    private static PaimonOutputTableHandle boundHandle(WriteOutputBinding binding) {
        var serialized = binding.hasPhysical()
                ? binding.getPhysical().getOutputHandle()
                : binding.hasConnector() ? binding.getConnector().getOutputHandle() : null;
        if (serialized == null) {
            throw new IllegalArgumentException("Durable Paimon write binding has no output handle");
        }
        var codec = new org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec();
        if (!codec.typeUrl().equals(serialized.getTypeUrl())
                || codec.codecVersion() != serialized.getCodecVersion()) {
            throw new IllegalArgumentException("Durable Paimon write binding uses an unsupported output handle");
        }
        return (PaimonOutputTableHandle) codec.deserialize(
                serialized.getCodecVersion(), serialized.getPayload().toByteArray());
    }

    private static void requireSameTarget(PaimonOutputTableHandle handle, PaimonOutputTableHandle journaled) {
        if (!handle.schemaName().equals(journaled.schemaName())
                || !handle.tableName().equals(journaled.tableName())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Recovered Paimon output handle differs from the journaled target");
        }
    }

    private void validateFingerprint(String tenant, PaimonOutputTableHandle handle,
            List<byte[]> fragments, DistributedWriteCommitContext context, byte[] bindingBytes) {
        if (context.copyFile() != null) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon cannot publish an Iceberg COPY file claim marker");
        }
        if (handle.baseSnapshotId() != context.baseSnapshotId() || !tenant.equals(context.tenantId())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon commit context does not match the bound write handle");
        }
        var cdcSource = context.cdcSource() == null ? null
                : new org.pangrova.persistence.query.api.commit.CdcSourceIdentity(
                        context.cdcSource().streamId(),
                        context.cdcSource().sourceEpoch(),
                        context.cdcSource().ranges().stream()
                                .map(range ->
                                        new org.pangrova.persistence.query.api.commit
                                                .CdcSourceIdentity.OffsetRange(
                                                range.topic(), range.partition(),
                                                range.startOffset(), range.endOffset()))
                                .toList());
        String expected = org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(context.executionId(),
                        context.tenantId(), catalogName,
                        new SchemaTableName(handle.schemaName(), handle.tableName()).toString(),
                        handle.writeMode(), handle.baseSnapshotId(), bindingBytes,
                        context.principal(), context.authorizationReceipt(), null, cdcSource), fragments);
        if (!expected.equals(context.commitFingerprint())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon write payload does not match durable commit fingerprint");
        }
    }

    private static WriteOutputBinding parseWriteBinding(byte[] binding) {
        try {
            return WriteOutputBinding.parseFrom(binding);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed durable Paimon output binding", failure);
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
            return PaimonTableIdentity.loadFresh(catalog, id);
        } catch (Catalog.TableNotExistException failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, failure);
        }
    }
}
