package org.pangrova.coordinator.query.task;

import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;

import java.util.List;
import java.util.Optional;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.governance.api.quality.WriteQualityGate;
import org.pangrova.governance.api.quality.WriteValidationResult;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.protocol.rest.data.WriteResultDescriptor;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.CopyFilePublicationMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
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
        if (writeResult == null) {
            LOG.debug("event=write_task_no_commit_metadata queryId={} taskId={}",
                    queryId, report.taskId());
            return;
        }
        List<byte[]> fragments = writeResult.commitFragments();
        if (!fragments.isEmpty()) {
            if (!writeCommitTracker.hasWriteContext(queryId)) {
                LOG.debug("event=write_commit_metadata_skipped queryId={} taskId={} reason=no_write_context",
                        queryId, report.taskId());
                return;
            }
            fragments.forEach(fragment -> writeCommitTracker.addCommitFragment(queryId, fragment));
            LOG.info("event=write_commit_fragments_collected queryId={} taskId={} fragmentCount={}",
                    queryId, report.taskId(), fragments.size());
            return;
        }
        byte[] commitMetadata = writeResult.commitMetadata();
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
    boolean commitDistributedWriteIfNeeded(String queryId, boolean readOnlyTaskExecution) {
        if (writeCommitTracker == null || catalogRegistry == null) {
            return true;
        }

        WriteCommitFencer fencer = this.writeCommitFencer;
        ExecutionAnchor anchor = fencer != null
                ? fencer.currentAnchor(queryId) : null;
        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_query_finalization")) {
            return false;
        }
        if (readOnlyTaskExecution) {
            return true;
        }

        NativeWriteCommitTracker.WriteCommitState writeState = writeCommitTracker.peekWriteState(queryId);
        if (writeState == null) {
            finishFromTerminalReceipt(queryId, null);
            return true;
        }

        List<byte[]> fragments = writeState.fragments();
        WriteCommitEntry intent = createWriteCommitIntent(queryId, writeState, fragments, anchor);
        try {
            enforceWriteQuality(writeState, queryId);
        } catch (PangrovaException qualityFailure) {
            // An already-published outcome remains authoritative if quality rules changed before a replay.
            if (finishFromTerminalReceipt(queryId, intent)) {
                return true;
            }
            throw qualityFailure;
        }

        if (writeCommitJournal.recordIntent(intent) == WriteCommitState.COMMITTED) {
            finishCommittedWrite(queryId, intent.commitFingerprint(), needsSpecializedReplay(intent));
            return true;
        }

        if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_catalog_commit")) {
            return false;
        }

        CopyFilePublication copyPublication = commitCatalog(writeState, intent);

        if (copyPublication != null) {
            if (!isCurrentExecutionOwner(queryId, fencer, anchor, "before_copy_publication")) return false;
            writeCommitJournal.recordCopyFilePublication(intent, copyPublication);
        }
        if (anchor == null) {
            writeCommitJournal.markCommitted(queryId);
        } else if (!writeCommitJournal.recordTerminalOutcome(
                intent, anchor.fencingToken(), WriteCommitState.COMMITTED)) {
            LOG.warn("event=distributed_write_fenced queryId={} fencingToken={} phase=journal_outcome",
                    queryId, anchor.fencingToken());
            return false;
        }
        retireCommittedWrite(intent);
        LOG.info("event=distributed_write_committed queryId={} catalog={} fragmentCount={}",
                queryId, writeState.catalogName(), fragments.size());
        return true;
    }

    private boolean finishFromTerminalReceipt(String queryId, WriteCommitEntry candidate) {
        var receipt = writeCommitJournal.getReceipt(queryId).orElse(null);
        if (receipt == null) {
            return false;
        }
        if (receipt.state() != WriteCommitState.COMMITTED) {
            throw new IllegalStateException("Cannot complete an aborted write: " + queryId);
        }
        if (candidate != null && !receipt.commitFingerprint().equals(candidate.commitFingerprint())) {
            throw new IllegalStateException("Write replay differs from its terminal receipt: " + queryId);
        }
        finishCommittedWrite(queryId, receipt.commitFingerprint(), needsSpecializedReplay(candidate));
        return true;
    }

    private static boolean needsSpecializedReplay(WriteCommitEntry candidate) {
        return candidate != null && (candidate.copyFile() != null || candidate.cdcSource() != null);
    }

    private void finishCommittedWrite(String queryId, String fingerprint, boolean requireRedo) {
        WriteCommitEntry committed = writeCommitJournal.getEntry(queryId).orElse(null);
        if (committed == null) {
            if (requireRedo) {
                throw new IllegalStateException("Specialized write replay requires its source-specific proof");
            }
            writeCommitTracker.removeWriteState(queryId);
            return;
        }
        if (committed.state() != WriteCommitState.COMMITTED
                || !fingerprint.equals(committed.commitFingerprint())) {
            throw new IllegalStateException("Write terminal receipt differs from retained redo: " + queryId);
        }
        if (committed.copyFile() != null) {
            var reconciler = new DefaultWriteCommitReconciler(writeCommitJournal, name -> {
                CatalogHandle catalog = catalogRegistry.getCatalog(name);
                return catalog instanceof ConnectorCatalogHandle connector ? connector : null;
            });
            if (reconciler.reconcile(committed, committed.executionFencingToken())
                    != org.pangrova.coordinator.enums.WriteRecoveryOutcome.COMMITTED) {
                throw new IllegalStateException("COMMITTED COPY lost its current Iceberg snapshot proof");
            }
        }
        LOG.info("event=distributed_write_already_committed queryId={}", queryId);
        retireCommittedWrite(committed);
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
        boolean required = DynamicTableRewriter.isInternalExecution(writeState.session())
                && writeCommitJournal.requiresPublication(intent);
        RequiredWritePublication.commit(writeCommitJournal, metadata, writeState.session(), boundHandle, intent,
                intent.executionFencingToken(), required);
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

    private void retireCommittedWrite(WriteCommitEntry entry) {
        if (!entry.authorizationReceipt().declaration().hasGeneratedColumns()) {
            writeCommitJournal.acknowledgeNoDeferredWork(entry.commitFingerprint());
        }
        writeCommitTracker.removeWriteState(entry.queryId());
        writeCommitJournal.purge(entry.queryId());
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
                        writeState.authorizationReceipt(), writeState.copyFile(),
                        writeState.cdcSource()),
                fragments);
        return new WriteCommitEntry(
                queryId,
                writeState.catalogName(),
                tableName,
                WriteCommitState.PENDING,
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
                null, writeState.copyFile(), writeState.cdcSource());
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
