package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.metadata.index.model.CommitLogEntry;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.connector.index.CommitEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
 * complete naturally.</p>
 */
public final class CommitLogPoller implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CommitLogPoller.class);

    private final IndexCommitContext deps;
    private final MaintenanceLeaderElection leaderElection;
    private final int batchMax;
    private final Consumer<IndexCommitHandler.MergedCommitFiles> commitProcessor;
    private final ScheduledExecutorService scheduler;
    private volatile boolean pollingActive = true;

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
        if (leaderElection != null && !leaderElection.tryAcquireOrRenew()) {
            LOG.debug("action=commit_log_poll_skipped reason=not_leader");
            return;
        }
        IndexMetadataStore store = deps.metadataStore();
        List<CommitLogEntry> entries = store.pollUnprocessedCommitEvents(batchMax);
        if (entries.isEmpty()) {
            return;
        }
        for (CommitLogEntry entry : entries) {
            processCommitLogEntry(store, entry);
        }
        LOG.debug("action=commit_log_poll_completed processed={}", entries.size());
    }

    private void processCommitLogEntry(IndexMetadataStore store, CommitLogEntry entry) {
        try {
            IndexCommitHandler.MergedCommitFiles files = toMergedCommitFiles(entry);
            commitProcessor.accept(files);
            // F-COORD-FEAT-07: mark as processed AFTER successful processing.
            // If processing fails, the entry remains unprocessed for retry
            // (at-least-once semantics). The previous order (mark-first) silently
            // discarded entries on processing failure.
            store.markCommitEventProcessed(entry.eventId());
        } catch (Exception e) {
            LOG.error("action=commit_log_entry_failed eventId={} error={}",
                    entry.eventId(), e.getMessage(), e);
        }
    }

    static IndexCommitHandler.MergedCommitFiles toMergedCommitFiles(CommitLogEntry entry) {
        List<String> addedFiles = parseFileList(entry.addedFilesJson());
        List<String> removedFiles = parseFileList(entry.removedFilesJson());
        CommitEventType eventType = parseEventType(entry.eventType());
        return new IndexCommitHandler.MergedCommitFiles(
                entry.catalogName(), entry.schemaName(), entry.tableName(),
                addedFiles, removedFiles, eventType, entry.snapshotId());
    }

    static List<String> parseFileList(String csv) {
        if (csv == null || csv.isEmpty()) {
            return List.of();
        }
        return List.of(csv.split(","));
    }

    static CommitEventType parseEventType(String value) {
        if (value == null || value.isEmpty()) {
            return CommitEventType.APPEND;
        }
        try {
            return CommitEventType.valueOf(value);
        } catch (IllegalArgumentException e) {
            return CommitEventType.APPEND;
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        LOG.info("action=commit_log_poller_shutdown");
    }
}
