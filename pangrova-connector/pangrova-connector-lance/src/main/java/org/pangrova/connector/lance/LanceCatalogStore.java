package org.pangrova.connector.lance;

import org.pangrova.connector.api.metadata.SourceTableState;

import org.pangrova.connector.api.metadata.SourceTableKey;

import org.pangrova.connector.api.metadata.AcceptedSourceSnapshot;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.ManagedSourceCatalog;
import org.pangrova.connector.api.metadata.PreparedSourcePublication;
import org.pangrova.connector.api.metadata.SourcePublicationReservation;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Format codec over the sole metadata source authority; this connector never opens a JDBC connection. */
final class LanceCatalogStore {
    private final ManagedSourceCatalog authority;

    LanceCatalogStore(ManagedSourceCatalog authority) {
        this.authority = java.util.Objects.requireNonNull(authority);
    }

    void createSchema(String catalog, String tenant, String schema) {
        requireCatalog(catalog);
        requireSchema(schema);
        authority.createSchema(tenant, schema);
    }

    List<String> listSchemas(String catalog, String tenant) {
        requireCatalog(catalog);
        return authority.listSchemas(tenant);
    }

    List<SchemaTableName> list(String catalog, String tenant) {
        requireCatalog(catalog);
        return authority.listTables(tenant);
    }

    void create(LanceTableHandle table) {
        requireCatalog(table.catalog());
        authority.create(state(table));
    }

    LanceTableHandle load(String catalog, String tenant, SchemaTableName table) {
        requireCatalog(catalog);
        return authority.load(new SourceTableKey(tenant, table))
                .map(LanceCatalogStore::decode).orElse(null);
    }

    void drop(LanceTableHandle table) {
        requireCatalog(table.catalog());
        authority.drop(state(table));
    }

    Snapshot loadSnapshot(LanceTableHandle selector) {
        requireCatalog(selector.catalog());
        var snapshot = authority.snapshot(state(selector));
        return new Snapshot(decode(snapshot.table()), snapshot.reference());
    }
    Optional<Snapshot> accepted(
            LanceOutputHandle output, List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        requireCatalog(output.table().catalog());
        return authority.accepted(state(output.table()), fragments, context)
                .map(snapshot -> new Snapshot(decode(snapshot.table()), snapshot.reference()));
    }
    SourcePublicationReservation reserve(LanceOutputHandle output, List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        requireCatalog(output.table().catalog());
        return authority.reserve(state(output.table()), fragments, context);
    }
    Snapshot accept(LanceOutputHandle output, SourcePublicationReservation reservation,
            PreparedSourcePublication publication, long writtenRows) {
        var base = output.table();
        long expectedRows = output.allocation().operation()
                == org.pangrova.connector.api.metadata.NativeWriteAllocation.Operation.APPEND
                ? Math.addExact(base.rowCount(), writtenRows) : writtenRows;
        if (publication.rows() != expectedRows) {
            throw new IllegalArgumentException("SDK manifest rows differ from winners");
        }
        var next = new LanceTableHandle(
                base.catalog(), base.tenantId(), base.table(), base.tableId(), base.datasetUri(),
                base.columns(), base.properties(), base.schemaFence(), Math.addExact(base.version(), 1),
                Math.addExact(base.revision(), 1), expectedRows);
        var accepted = authority.accept(reservation, publication, state(next));
        return new Snapshot(decode(accepted.table()), accepted.reference());
    }
    org.pangrova.connector.api.metadata.DistributedWriteAbortReceipt abort(
            LanceOutputHandle output, DistributedWriteCommitContext context) {
        requireCatalog(output.table().catalog());
        return authority.abort(reservation(output, context));
    }

    SourcePublicationReservation reservation(LanceOutputHandle output, DistributedWriteCommitContext context) {
        requireCatalog(output.table().catalog());
        return new SourcePublicationReservation(
                state(output.table()), context.executionId(), context.commitFingerprint(),
                "_pangrova/prepared/" + context.commitFingerprint() + ".manifest",
                output.acceptedWriteBaseSnapshot());
    }

    record Snapshot(LanceTableHandle table, AcceptedSourceSnapshot reference) {
        Snapshot {
            java.util.Objects.requireNonNull(table);
            java.util.Objects.requireNonNull(reference);
        }

        byte[] manifestDigest() { return reference.manifestSha256(); }
    }

    private void requireCatalog(String catalog) {
        if (!authority.deployment().catalog().equals(catalog)) throw new SecurityException("Source catalog differs");
    }
    static void requireSchema(String schema) {
        if (schema == null || schema.isBlank()) throw new IllegalArgumentException("Schema name must not be blank");
    }
    static SourceTableState state(LanceTableHandle table) {
        try {
            return new SourceTableState(
                    new SourceTableKey(table.tenantId(), table.table()),
                    table.tableId(), table.datasetUri(), 1, table.revision(), table.version(), table.rowCount(),
                    table.schemaFence(), LanceOutputHandleCodec.MAPPER.writeValueAsBytes(table));
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot encode source table", failure);
        }
    }
    private static LanceTableHandle decode(SourceTableState state) {
        try {
            var table = LanceOutputHandleCodec.MAPPER.readValue(state.payload(), LanceTableHandle.class);
            if (!table.tenantId().equals(state.key().tenant()) || !table.table().equals(state.key().table())
                    || !table.tableId().equals(state.tableId()) || !table.datasetUri().equals(state.datasetUri())
                    || state.generation() != 1 || table.version() != state.version()
                    || table.revision() != state.revision()
                    || table.rowCount() != state.rows()
                    || !java.util.Arrays.equals(table.schemaFence(), state.schemaSha256())
                    || !java.util.Arrays.equals(state(table).payload(), state.payload())) {
                throw new IllegalStateException("Format metadata differs from authoritative source scalars");
            }
            return table;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Malformed source table", failure);
        }
    }
}
