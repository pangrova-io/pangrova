package org.pangrova.coordinator.dynamictable.write;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes incremental evaluation results to the Dynamic Table target table
 * ({@code __dt_NAME}) and invalidates related query cache entries.
 *
 * <p>Cache invalidation is fault-tolerant: failures are logged at WARN level
 * but do not block the refresh pipeline (R8 AC3).</p>
 */
public final class DynamicTableWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableWriter.class);
    private static final String TARGET_TABLE_PREFIX = "__dt_";
    private static final String CHECKPOINT_KEY = "pangrova.dt.source-checkpoint";
    private static final String CHECKPOINT_TYPE_KEY = "pangrova.dt.checkpoint-type";

    private final TableWriter tableWriter;
    private final QueryResultCache queryCache;
    private final CommitListener commitListener;
    private final String tenantId;

    public DynamicTableWriter(TableWriter tableWriter, QueryResultCache queryCache) {
        this(tableWriter, queryCache, null, null);
    }

    /** Writes dynamic table refresh results and notifies the commit listener. */
    public DynamicTableWriter(TableWriter tableWriter, QueryResultCache queryCache,
                              CommitListener commitListener, String tenantId) {
        this.tableWriter = Objects.requireNonNull(tableWriter, "tableWriter must not be null");
        this.queryCache = Objects.requireNonNull(queryCache, "queryCache must not be null");
        this.commitListener = commitListener;
        if (commitListener != null && (tenantId == null || tenantId.isBlank())) {
            throw new IllegalArgumentException(
                    "tenantId is required when commit notification is enabled");
        }
        this.tenantId = tenantId;
    }

    /**
     * Write incremental results to the target table and invalidate cache.
     *
     * @param dtName           Dynamic Table name (without prefix)
     * @param result           incremental evaluation result to write
     * @param sourceCheckpoint opaque checkpoint from the source (snapshot ID or CDC offset)
     * @return write statistics including the source checkpoint
     */
    public DynamicTableWriteResult write(String dtName, IncrementalResult result,
                                         String sourceCheckpoint) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(result, "result must not be null");

        String targetTable = TARGET_TABLE_PREFIX + dtName;

        // F-CHAIN-STREAM-06: Idempotent recovery — skip write if checkpoint
        // is already committed in the target table.
        if (sourceCheckpoint != null && isCheckpointAlreadyCommitted(targetTable, sourceCheckpoint)) {
            LOG.info("event=dt_write_skipped_idempotent dtName={} checkpoint={}",
                    dtName, sourceCheckpoint);
            return new DynamicTableWriteResult(0L, 0L, targetTable, sourceCheckpoint);
        }

        List<WriteRecord> records = toWriteRecords(result.records());

        // F-CHAIN-STREAM-06: Embed checkpoint atomically in commit properties.
        Map<String, String> commitProperties = buildCheckpointProperties(sourceCheckpoint);
        WriteContext context = commitProperties.isEmpty()
                ? WriteContext.selfManaged()
                : WriteContext.selfManagedWithProperties(commitProperties);

        WriteResult spiResult = tableWriter.write(targetTable, records, WriteMode.APPEND, context);

        notifyCommitListenerSafely(targetTable, spiResult);
        invalidateCacheSafely(targetTable, dtName);

        return new DynamicTableWriteResult(
                spiResult.recordCount() * estimateBytesPerRow(result),
                spiResult.recordCount(),
                targetTable,
                sourceCheckpoint);
    }

    /**
     * Checks if the given checkpoint is already committed in the target table.
     * Used for idempotent recovery after coordinator crash/restart.
     */
    private boolean isCheckpointAlreadyCommitted(String targetTable, String checkpoint) {
        try {
            return tableWriter.readCommitProperty(targetTable, CHECKPOINT_KEY)
                    .map(committed -> committed.equals(checkpoint))
                    .orElse(false);
        } catch (Exception e) {
            LOG.warn("event=dt_checkpoint_read_failed targetTable={} error={} action=proceed_with_write",
                    targetTable, e.getMessage());
            return false;
        }
    }

    private static Map<String, String> buildCheckpointProperties(String sourceCheckpoint) {
        if (sourceCheckpoint == null) {
            return Map.of();
        }
        return Map.of(
                CHECKPOINT_KEY, sourceCheckpoint,
                CHECKPOINT_TYPE_KEY, "SNAPSHOT");
    }

    private List<WriteRecord> toWriteRecords(List<Map<String, Object>> rows) {
        return rows.stream()
                .map(row -> WriteRecord.fromClient(
                        TypedRow.fromRawMap(row),
                        WriteRecord.WriteOperation.INSERT))
                .toList();
    }

    /**
     * Cache invalidation with fault tolerance (R8 AC3).
     * Failures are logged at WARN but never propagated.
     */
    private void invalidateCacheSafely(String targetTable, String dtName) {
        try {
            queryCache.invalidateByTable(targetTable);
        } catch (Exception e) {
            LOG.warn("event=dt_cache_invalidation_failed dtName={} targetTable={} error={}",
                    dtName, targetTable, e.getMessage());
        }
    }

    /**
     * Notifies the commit listener about new data files written to the DT target table.
     * Fault-tolerant: failures are logged but never block the refresh pipeline.
     */
    private void notifyCommitListenerSafely(String targetTable, WriteResult spiResult) {
        if (commitListener == null || spiResult.recordCount() == 0) {
            return;
        }
        try {
            CommitEvent event = new CommitEvent(
                    tenantId, "default", "default", targetTable,
                    List.of(targetTable + "/" + spiResult.commitTime().toEpochMilli()),
                    List.of(),
                    spiResult.commitTime(),
                    String.valueOf(spiResult.commitTime().toEpochMilli()),
                    CommitEventType.APPEND, null);
            commitListener.onCommit(event);
        } catch (Exception e) {
            LOG.warn("event=dt_commit_listener_failed targetTable={} error={}",
                    targetTable, e.getMessage());
        }
    }

    private long estimateBytesPerRow(IncrementalResult result) {
        if (result.records().isEmpty()) {
            return 0L;
        }
        // Rough estimate: count columns * average value size
        Map<String, Object> sample = result.records().get(0);
        return sample.size() * 64L;
    }
}
