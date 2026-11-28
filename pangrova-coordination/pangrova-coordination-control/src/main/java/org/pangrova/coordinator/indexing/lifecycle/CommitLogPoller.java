package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.persistence.indexing.api.metadata.CommitLogEntry;
import org.pangrova.persistence.indexing.api.metadata.IndexCommitLog;
import org.pangrova.connector.api.enums.CommitEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Polls unprocessed commit events from the shared commit log and dispatches
 * them for processing. Enables multi-coordinator deployments where events
 * written by one coordinator are picked up by the maintenance leader.
 *
 * <p>Concurrency strategy: single-threaded scheduled executor. The
 * {@code pollingActive} flag gates new poll cycles; in-progress cycles
 * complete naturally. Each event checks the maintenance epoch before its
 * external effect and before acknowledgement. Effects remain at-least-once:
 * processors must tolerate replay after an effect succeeds but its ACK fails.</p>
 */
public final class CommitLogPoller implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CommitLogPoller.class);

    private final IndexCommitContext deps;
    private final MaintenanceLeaderElection leaderElection;
    private final int batchMax;
    private final Consumer<IndexCommitHandler.MergedCommitFiles> commitProcessor;
    private final ScheduledExecutorService scheduler;
    private volatile boolean pollingActive = true;

    /** Creates a leader-gated index commit-log poller. */
    public CommitLogPoller(IndexCommitContext deps,
                           MaintenanceLeaderElection leaderElection,
                           int batchMax,
                           Consumer<IndexCommitHandler.MergedCommitFiles> commitProcessor) {
        this.deps = deps;
        this.leaderElection = leaderElection;
        this.batchMax = batchMax;
        this.commitProcessor = commitProcessor;
        this.scheduler = ManagedExecutorFactory
                .newSingleThreadScheduledExecutor("index-commit-log-poller");
        this.scheduler.scheduleWithFixedDelay(
                this::pollCommitLogSafe, 10, 5, TimeUnit.SECONDS);
    }

    /**
     * Resumes commit-log polling. Called when this node becomes maintenance leader.
     * Idempotent — safe to call multiple times.
     */
    public void startPolling() {
        pollingActive = true;
        LOG.info("action=commit_polling_started");
    }

    /**
     * Pauses commit-log polling. Called when this node loses maintenance leadership.
     * In-progress poll cycles complete but no new cycles start.
     * Idempotent — safe to call multiple times.
     */
    public void stopPolling() {
        pollingActive = false;
        LOG.info("action=commit_polling_stopped");
    }

    private void pollCommitLogSafe() {
        if (!pollingActive) {
            return;
        }
        try {
            pollCommitLog();
        } catch (Exception e) {
            LOG.error("action=commit_log_poll_failed error={}", e.getMessage(), e);
        }
    }

    void pollCommitLog() {
        if (leaderElection == null) {
            throw new IllegalStateException("Commit log polling requires a maintenance lease");
        }
        OptionalLong leaseEpoch = leaderElection.acquireOrRenewEpoch();
        if (leaseEpoch.isEmpty()) {
            LOG.debug("action=commit_log_poll_skipped reason=not_leader");
            return;
        }
        IndexCommitLog store = deps.metadataStore();
        List<CommitLogEntry> entries = store.pollUnprocessedCommitEvents(
                batchMax, leaseEpoch.getAsLong());
        if (entries.isEmpty()) {
            return;
        }
        Set<String> blockedTables = new HashSet<>();
        int processed = 0;
        for (CommitLogEntry entry : entries) {
            OptionalLong currentEpoch = leaderElection.acquireOrRenewEpoch();
            if (currentEpoch.isEmpty() || currentEpoch.getAsLong() != leaseEpoch.getAsLong()) {
                LOG.warn("action=commit_log_batch_stopped reason=lease_changed "
                                + "batchEpoch={} currentEpoch={}",
                        leaseEpoch.getAsLong(), currentEpoch);
                return;
            }
            String tableKey = tableKey(entry);
            if (blockedTables.contains(tableKey)) {
                continue;
            }
            if (processCommitLogEntry(store, entry, leaseEpoch.getAsLong())) {
                processed++;
            } else {
                blockedTables.add(tableKey);
            }
        }
        LOG.debug("action=commit_log_poll_completed fetched={} processed={} blockedTables={}",
                entries.size(), processed, blockedTables.size());
    }

    private boolean processCommitLogEntry(
            IndexCommitLog store, CommitLogEntry entry, long leaseEpoch) {
        try {
            IndexCommitHandler.MergedCommitFiles files = toMergedCommitFiles(entry);
            commitProcessor.accept(files);
            OptionalLong currentEpoch = leaderElection.acquireOrRenewEpoch();
            if (currentEpoch.isEmpty() || currentEpoch.getAsLong() != leaseEpoch) {
                throw new IllegalStateException(
                        "Maintenance lease changed before commit-event acknowledgement");
            }
            // F-COORD-FEAT-07: mark as processed AFTER successful processing.
            // If processing fails, the entry remains unprocessed for retry
            // (at-least-once semantics). The previous order (mark-first) silently
            // discarded entries on processing failure.
            store.markCommitEventProcessed(entry.eventId(), leaseEpoch);
            return true;
        } catch (Exception e) {
            LOG.error("action=commit_log_entry_failed eventId={} error={}",
                    entry.eventId(), e.getMessage(), e);
            return false;
        }
    }

    private static String tableKey(CommitLogEntry entry) {
        return entry.tenantId() + '\u0000' + entry.catalogName() + '\u0000'
                + entry.schemaName() + '\u0000' + entry.tableName();
    }

    static IndexCommitHandler.MergedCommitFiles toMergedCommitFiles(CommitLogEntry entry) {
        List<String> addedFiles = parseFileList(entry.addedFilesJson());
        List<String> removedFiles = parseFileList(entry.removedFilesJson());
        CommitEventType eventType = parseEventType(entry.eventType());
        return new IndexCommitHandler.MergedCommitFiles(
                entry.tenantId(), entry.catalogName(), entry.schemaName(), entry.tableName(),
                addedFiles, removedFiles, eventType, entry.snapshotId(), entry.commitFingerprint());
    }

    static List<String> parseFileList(String csv) {
        return CommitFileListCodec.decode(csv);
    }

    static CommitEventType parseEventType(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Unknown commit event type: null");
        }
        try {
            return CommitEventType.valueOf(value);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Unknown commit event type: " + value, failure);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        LOG.info("action=commit_log_poller_shutdown");
    }
}
