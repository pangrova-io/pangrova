package org.pangrova.coordinator.query.task;

import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;

import java.util.List;
import java.util.Optional;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.governance.api.quality.WriteQualityGate;
import org.pangrova.governance.api.quality.WriteValidationResult;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.CopyFilePublicationMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.query.api.ownership.ExecutionAnchor;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.common.file.CopyFilePublication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns distributed write publication and its existing fenced redo journal, not task execution. */
@ThreadSafe
final class NativeTaskWriteCompletion {
    private static final Logger LOG = LoggerFactory.getLogger(NativeTaskWriteCompletion.class);
    private final CatalogRegistry catalogRegistry;
    private final NativeWriteCommitTracker writeCommitTracker;
    private final WriteCommitJournal writeCommitJournal;
    private volatile WriteCommitFencer writeCommitFencer;
    private volatile WriteQualityGate writeQualityGate;

    NativeTaskWriteCompletion(TaskStatusReceiverContext deps) {
        catalogRegistry = deps.catalogRegistry();
        writeCommitTracker = deps.writeCommitTracker();
        writeCommitJournal = deps.writeCommitJournal();
    }

    void setFencer(WriteCommitFencer fencer) { writeCommitFencer = fencer; }
    WriteCommitFencer fencer() { return writeCommitFencer; }

    /** Installs the governance pre-commit gate; absent means writes are not gated. */
    void setWriteQualityGate(WriteQualityGate gate) { writeQualityGate = gate; }

    void collectWriteCommitMetadata(String queryId, TaskStatusReport report) {
        if (writeCommitTracker == null) {
            return;
        }
        WriteResultDescriptor writeResult = report.writeResult();
        byte[] commitMetadata = writeResult != null ? writeResult.commitMetadata() : null;
        if (commitMetadata == null || commitMetadata.length == 0) {
            LOG.debug("event=write_task_no_commit_metadata queryId={} taskId={}",
                    queryId, report.taskId());
            return;
        }
        if (!writeCommitTracker.hasWriteContext(queryId)) {
            LOG.debug("event=write_commit_metadata_skipped queryId={} taskId={} reason=no_write_context",
                    queryId, report.taskId());
            return;
        }
        writeCommitTracker.addCommitFragment(queryId, commitMetadata);
        LOG.info("event=write_commit_metadata_collected queryId={} taskId={} metadataBytes={}",
                queryId, report.taskId(), commitMetadata.length);
    }

    /**
     * Executes the distributed write commit when all tasks for a write query have finished.
     * Retrieves the accumulated commit metadata fragments and calls
     * ConnectorMetadata.finishDistributedWrite() for a single atomic catalog commit.
     *
     * <p>Integrates with {@link WriteCommitJournal} for idempotent recovery:
     * already-committed writes (detected via journal) are skipped.</p>
     */
    boolean commitDistributedWriteIfNeeded(String queryId) {
        if (writeCommitTracker == null || catalogRegistry == null) {
            return true;
        }

        WriteCommitFencer fencer = this.writeCommitFencer;
        ExecutionAnchor anchor = fencer != null
                ? fencer.currentAnchor(queryId) : null;
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_journal_read")) {
            return false;
        }

        // Idempotent recovery: skip if the journal says this write is already committed
        if (writeCommitJournal.isCommitted(queryId)) {
            WriteCommitEntry committed = writeCommitJournal.getEntry(queryId).orElseThrow(() ->
                    new IllegalStateException("COMMITTED write lost its durable intent"));
            if (committed.copyFile() != null) {
                DefaultWriteCommitReconciler reconciler = new DefaultWriteCommitReconciler(
                        writeCommitJournal, name -> {
                            CatalogHandle catalog = catalogRegistry.getCatalog(name);
                            return catalog instanceof ConnectorCatalogHandle connector ? connector : null;
                        });
                if (reconciler.reconcile(committed)
                        != org.pangrova.coordinator.query.task.write.WriteCommitReconciler.RecoveryOutcome.COMMITTED) {
                    throw new IllegalStateException("COMMITTED COPY lost its current Iceberg snapshot proof");
                }
            }
            LOG.info("event=distributed_write_already_committed queryId={}", queryId);
            retireCommittedWrite(queryId);
            return true;
        }

        // The live write context must outlive both intent and catalog commit.
        NativeWriteCommitTracker.WriteCommitState writeState =
                writeCommitTracker.peekWriteState(queryId);
        if (writeState == null) {
            return true;
        }

        enforceWriteQuality(writeState, queryId);

        // Retain a replayable exact intent before touching the external catalog.
        List<byte[]> fragments = writeState.fragments();
        WriteCommitEntry intent = createWriteCommitIntent(queryId, writeState, fragments, anchor);
        writeCommitJournal.recordIntent(intent);

        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_catalog_commit")) {
            return false;
        }

        CopyFilePublication copyPublication = commitCatalog(writeState, intent);

        // A changed owner must reconcile the same intent, never publish another commit.
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "after_catalog_commit")) return false;
        if (copyPublication != null) {
            writeCommitJournal.recordCopyFilePublication(intent, copyPublication);
        }
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "after_copy_publication")) return false;
        writeCommitJournal.markCommitted(queryId);
        retireCommittedWrite(queryId);
        LOG.info("event=distributed_write_committed queryId={} catalog={} fragmentCount={}",
                queryId, writeState.catalogName(), fragments.size());
        return true;
    }

    private CopyFilePublication commitCatalog(NativeWriteCommitTracker.WriteCommitState writeState,
            WriteCommitEntry intent) {
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(writeState.catalogName());
        if (!(catalogHandle instanceof ConnectorCatalogHandle connectorCatalog)) {
            throw new IllegalStateException(
                    "Distributed write catalog is no longer a connector catalog");
        }
        var boundHandle = DefaultWriteCommitReconciler.decodeExactHandle(
                intent, writeState.outputHandle().targetTable().orElseThrow(),
                connectorCatalog);
        ConnectorMetadata metadata = catalogHandle.getMetadata(writeState.session());
        RequiredWritePublication.commit(writeCommitJournal, metadata, writeState.session(), boundHandle, intent);
        if (intent.copyFile() == null) {
            return null;
        }
        if (!(metadata instanceof CopyFilePublicationMetadata copyMetadata)) {
            throw new IllegalStateException("COPY target connector cannot prove snapshot publication");
        }
        return copyMetadata.confirmCopyFilePublication(
                writeState.session(), boundHandle, intent.commitContext());
    }

    /**
     * Evaluates the target table's enabled quality rules before the catalog commit.
     *
     * <p>An ERROR-severity failure blocks the write before any durable intent is
     * recorded, so the catalog is never asked to publish data that governance
     * rejects. A gate that cannot evaluate its rules is treated as a failure:
     * a write is never admitted on missing evidence.</p>
     */
    private void enforceWriteQuality(
            NativeWriteCommitTracker.WriteCommitState writeState, String queryId) {
        WriteQualityGate gate = writeQualityGate;
        if (gate == null) {
            return;
        }
        Optional<SchemaTableName> target = writeState.outputHandle().targetTable();
        if (target.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED,
                    "Write quality gate requires a recoverable target table for query " + queryId);
        }
        SchemaTableName table = target.get();
        WriteValidationResult result;
        try {
            result = gate.validate(writeState.catalogName(), table.schema(), table.table());
        } catch (RuntimeException failure) {
            throw new PangrovaException(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED,
                    "Write quality gate could not evaluate rules for "
                            + writeState.catalogName() + "." + table + ": " + failure.getMessage(),
                    failure);
        }
        if (!result.allowed()) {
            LOG.warn("event=distributed_write_quality_blocked queryId={} table={}.{} "
                            + "reason={}", queryId, writeState.catalogName(), table,
                    result.blockReason());
            throw new PangrovaException(PangrovaErrorCode.GOVERNANCE_QUALITY_CHECK_FAILED,
                    result.blockReason());
        }
    }

    private void retireCommittedWrite(String queryId) {
        acknowledgeOrdinaryWrite(queryId);
        writeCommitTracker.removeWriteState(queryId);
        writeCommitJournal.purge(queryId);
    }

    private void acknowledgeOrdinaryWrite(String queryId) {
        var entry = writeCommitJournal.getEntry(queryId).orElseThrow(() ->
                new IllegalStateException("COMMITTED write lost its declaration receipt"));
        if (!entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            writeCommitJournal.acknowledgeNoDeferredWork(entry.commitFingerprint());
        }
    }

    private static WriteCommitEntry createWriteCommitIntent(
            String queryId,
            NativeWriteCommitTracker.WriteCommitState writeState,
            List<byte[]> fragments,
            ExecutionAnchor anchor) {
        var outputHandle = writeState.outputHandle();
        String tableName = outputHandle.targetTable()
                .orElseThrow(() -> new IllegalStateException(
                        "Distributed write handle lacks a recoverable target table"))
                .toString();
        String tenantId = writeState.session().getProperties().get("tenant_id");
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalStateException(
                    "Distributed write commit requires tenant_id in the connector session");
        }
        long baseSnapshotId = outputHandle.writeBaseSnapshotId().orElse(-1L);
        byte[] writeOutputBinding = writeState.writeOutputBinding();
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        queryId,
                        tenantId,
                        writeState.catalogName(),
                        tableName,
                        outputHandle.writeMode(),
                        baseSnapshotId,
                        writeOutputBinding,
                        writeState.session().getUser(),
                        writeState.authorizationReceipt(), writeState.copyFile()),
                fragments);
        return new WriteCommitEntry(
                queryId,
                writeState.catalogName(),
                tableName,
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                outputHandle.writeMode(),
                fingerprint,
                tenantId,
                writeState.session().getUser(),
                writeState.authorizationReceipt(),
                baseSnapshotId,
                writeOutputBinding,
                fingerprint,
                anchor != null ? anchor.ownerId() : null,
                anchor != null ? anchor.ownerIncarnation() : null,
                anchor != null ? anchor.fencingToken() : 0L,
                null, writeState.copyFile());
    }

    private static boolean isCurrentExecutionOwner(
            String queryId,
            WriteCommitFencer fencer,
            ExecutionAnchor anchor,
            String phase) {
        if (fencer == null) {
            return true;
        }
        if (anchor != null && fencer.isCurrent(queryId, anchor.fencingToken())) {
            return true;
        }
        LOG.error("event=distributed_write_fenced queryId={} owner={} incarnation={} "
                        + "fencingToken={} phase={} reason=stale_execution_owner",
                queryId,
                anchor != null ? anchor.ownerId() : "missing",
                anchor != null ? anchor.ownerIncarnation() : "missing",
                anchor != null ? anchor.fencingToken() : 0L,
                phase);
        return false;
    }

    /**
     * An ambiguous catalog-commit failure must retain its durable PENDING intent for recovery.
     * Only the JVM-only handle is released; purging the redo record here would turn a crash or
     * timeout after catalog publication into silent data loss or duplicate publication.
     */
    void cleanupWriteStateAfterCommitFailure(String queryId) {
        if (writeCommitTracker != null) {
            writeCommitTracker.clearWriteState(queryId);
        }
    }

    /**
     * Cleans up write state when a query fails, preventing memory leaks.
     * Retains pending redo evidence because task failure cannot prove catalog abort.
     */
    void cleanupWriteStateOnFailure(String queryId) {
        if (writeCommitTracker != null) {
            writeCommitTracker.clearWriteState(queryId);
        }
        writeCommitJournal.purge(queryId);
    }

}
