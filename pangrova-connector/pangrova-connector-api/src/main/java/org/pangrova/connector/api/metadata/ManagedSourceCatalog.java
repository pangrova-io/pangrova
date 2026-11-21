package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.commit.DistributedWriteCommitContext;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

import java.util.List;
import java.util.Optional;

/** Catalog-owner authority for immutable source manifests, sharing the execution and write journal transaction. */
@Experimental
public interface ManagedSourceCatalog {

    SourceCatalogDeployment deployment();
    void createSchema(String tenant, String schema);
    List<String> listSchemas(String tenant);
    List<SchemaTableName> listTables(String tenant);
    void create(SourceTableState table);
    Optional<SourceTableState> load(SourceTableKey key);
    void drop(SourceTableState expected);
    SourceSnapshot snapshot(SourceTableState selector);

    /** Durably allocates the sole candidate address before a Worker prepares any manifest. */
    SourcePublicationReservation reserve(SourceTableState base, List<byte[]> fragments,
            DistributedWriteCommitContext intent);

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
    SourceSnapshot accept(SourcePublicationReservation reservation, PreparedSourcePublication publication,
            SourceTableState next);

    /** Reconciles exact durable write truth under current owner, original deployment and current write authority. */
    Optional<SourceSnapshot> accepted(SourceTableState base, List<byte[]> fragments,
            DistributedWriteCommitContext intent);

    /** Marks a bounded deletion set only after all issued SDK calls have durably joined and references are excluded. */
    Optional<SourceCleanupBatch> reserveCleanup(SourcePublicationReservation reservation);

    /** Acknowledges actual joined idempotent storage deletion of the exact reserved batch. */
    void completeCleanup(SourcePublicationReservation reservation, SourceCleanupBatch batch);

    /** Establishes an exact unpublished barrier; retained objects remain held until producer drain is proved. */
    DistributedWriteAbortReceipt abort(SourcePublicationReservation reservation);

}
