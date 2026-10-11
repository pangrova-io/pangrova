package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Optional;

/** Catalog-owner authority for immutable source manifests, sharing the execution and write journal transaction. */
@Experimental
public interface ManagedSourceCatalog {
    /** Exact durable catalog deployment; secrets are not part of this identity. */
    @Experimental
    record Deployment(String catalog, String incarnation, long revision) {
        public Deployment {
            text(catalog);
            text(incarnation);
            if (revision <= 0) throw new IllegalArgumentException("Invalid source catalog revision");
        }
    }

    /** Tenant-scoped SQL identity, independent of a storage format. */
    @Experimental
    record TableKey(String tenant, SchemaTableName table) {
        public TableKey {
            text(tenant);
            java.util.Objects.requireNonNull(table);
            text(table.schema());
            text(table.table());
        }
    }

    /** Format-owned metadata bytes and the scalars checked by the authoritative catalog CAS. */
    @Experimental
    record TableState(TableKey key, String tableId, String datasetUri, long generation, long revision,
            long version, long rows, byte[] schemaSha256, byte[] payload) {
        public TableState {
            java.util.Objects.requireNonNull(key);
            text(tableId);
            text(datasetUri);
            if (generation <= 0 || revision <= 0 || version < 0 || rows < 0
                    || schemaSha256.length != 32 || payload.length == 0 || payload.length > 4 * 1024 * 1024) {
                throw new IllegalArgumentException("Invalid source table state");
            }
            schemaSha256 = schemaSha256.clone();
            payload = payload.clone();
        }
        @Override public byte[] schemaSha256() { return schemaSha256.clone(); }
        @Override public byte[] payload() { return payload.clone(); }
    }

    /** One accepted historical snapshot and its format-owned metadata. */
    @Experimental
    record Snapshot(TableState table, AcceptedSourceSnapshot reference) {
        public Snapshot {
            java.util.Objects.requireNonNull(table);
            java.util.Objects.requireNonNull(reference);
        }
    }

    Deployment deployment();
    void createSchema(String tenant, String schema);
    List<String> listSchemas(String tenant);
    List<SchemaTableName> listTables(String tenant);
    void create(TableState table);
    Optional<TableState> load(TableKey key);
    void drop(TableState expected);
    Snapshot snapshot(TableState selector);

    /** Durably allocates the sole candidate address before a Worker prepares any manifest. */
    SourcePublicationReservation reserve(TableState base, List<byte[]> fragments, DistributedWriteCommitContext intent);

    /** Commits an exact issuance before the selected Worker may start an SDK preparation call. */
    SourcePrepareInvocation issuePreparation(SourcePublicationReservation reservation, int pageIndex,
            String afterPath, String workerEndpoint, byte[] requestSha256);

    /** Records the matching actual SDK join response; RPC expiry or writer completion cannot call this. */
    void acknowledgePreparation(SourcePublicationReservation reservation, SourcePrepareInvocation invocation,
            byte[] responseSha256);

    /** Registers a real SDK new-object page under the same fenced candidate, retaining it on UNKNOWN. */
    void registerObjects(SourcePublicationReservation reservation, PreparedSourcePublication publication,
            SourceObjectPage page);

    /** Atomically accepts the prepared manifest, advances the head and commits the exact redo journal. */
    Snapshot accept(SourcePublicationReservation reservation, PreparedSourcePublication publication, TableState next);

    /** Reconciles exact durable write truth under current owner, original deployment and current write authority. */
    Optional<Snapshot> accepted(TableState base, List<byte[]> fragments, DistributedWriteCommitContext intent);

    /** Marks a bounded deletion set only after all issued SDK calls have durably joined and references are excluded. */
    Optional<SourceCleanupBatch> reserveCleanup(SourcePublicationReservation reservation);

    /** Acknowledges actual joined idempotent storage deletion of the exact reserved batch. */
    void completeCleanup(SourcePublicationReservation reservation, SourceCleanupBatch batch);

    /** Establishes an exact unpublished barrier; retained objects remain held until producer drain is proved. */
    DistributedWriteAbortReceipt abort(SourcePublicationReservation reservation);

    private static void text(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Source identity is required");
    }
}
