package org.pangrova.coordinator.streaming;

import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.cdc.CdcRangeAllocator;
import org.pangrova.streaming.cdc.CdcTaskRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Plans canonical CDC tasks from committed source offsets and current log ends.
 *
 * <p>The planner never mutates the offset store and never dispatches a task.
 * Its emitted end-offset map is the exact acknowledgement boundary the caller
 * may publish only after the connector commit is durable.</p>
 */
public final class CdcTaskPlanner {

    private CdcTaskPlanner() {}

    /** Plans the full observed source interval, split into bounded tasks. */
    public static CdcTaskPlan plan(
            StreamState stream,
            CdcSourceReader reader,
            CdcWriteAuthority.CdcWritePlan writePlan,
            Map<TopicPartitionOffset, Long> committedOffsets,
            long maxRecordsPerRange,
            int rangesPerTask,
            TaskAttemptId baseAttempt,
            SecurityContext security,
            StageResourceGrant baseGrant,
            long deadlineEpochMs,
            String targetTableUuid,
            long baseSnapshotId) {
        return plan(stream, reader, writePlan, committedOffsets, maxRecordsPerRange, Long.MAX_VALUE, rangesPerTask,
                baseAttempt, security, baseGrant, deadlineEpochMs, targetTableUuid, baseSnapshotId);
    }

    /** Builds tasks for one stream without committing offsets or publishing data. */
    public static CdcTaskPlan plan(
            StreamState stream,
            CdcSourceReader reader,
            CdcWriteAuthority.CdcWritePlan writePlan,
            Map<TopicPartitionOffset, Long> committedOffsets,
            long maxRecordsPerRange,
            long maxOffsetsPerPartitionPerBatch,
            int rangesPerTask,
            TaskAttemptId baseAttempt,
            SecurityContext security,
            StageResourceGrant baseGrant,
            long deadlineEpochMs,
            String targetTableUuid,
            long baseSnapshotId) {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(writePlan, "writePlan");
        Objects.requireNonNull(baseAttempt, "baseAttempt");
        Objects.requireNonNull(security, "security");
        List<TopicPartitionOffset> partitions = reader.partitions();
        if (maxOffsetsPerPartitionPerBatch <= 0) {
            throw new IllegalArgumentException("CDC batch offset bound must be positive");
        }
        long batchLimit = maxOffsetsPerPartitionPerBatch;
        if (writePlan.outputHandle().requiresSingleCdcRange()) {
            if (partitions.size() != 1) {
                throw new UnsupportedOperationException("CDC mutation target requires a single source partition");
            }
            batchLimit = Math.min(maxRecordsPerRange, batchLimit);
        }
        Map<TopicPartitionOffset, Long> logEnds = reader.endOffsets(partitions);
        Map<TopicPartitionOffset, Long> starts = new HashMap<>();
        Map<TopicPartitionOffset, Long> admittedEnds = new HashMap<>();
        for (TopicPartitionOffset partition : partitions) {
            long start = committedOffsets.getOrDefault(partition, 0L);
            Long logEnd = logEnds.get(partition);
            if (start < 0 || logEnd == null || logEnd < start) {
                throw new IllegalArgumentException("Invalid CDC source interval for " + partition);
            }
            if (logEnd > start) {
                starts.put(partition, start);
                admittedEnds.put(partition, start + Math.min(logEnd - start, batchLimit));
            }
        }
        CdcRangeAllocator allocator = new CdcRangeAllocator(starts, admittedEnds, maxRecordsPerRange);
        List<List<CdcTaskRange>> grouped = allocator.tasks(rangesPerTask);
        List<TaskRequest> tasks = new ArrayList<>(grouped.size());
        for (int index = 0; index < grouped.size(); index++) {
            TaskAttemptId attempt = baseAttempt.toBuilder()
                    .setTaskId(baseAttempt.getTaskId() + "-" + index)
                    .setPartitionId(index)
                    .build();
            StageResourceGrant grant = baseGrant.toBuilder()
                    .setGrantId(baseGrant.getGrantId() + "-" + index)
                    .build();
            tasks.add(CdcTaskBindingFactory.create(attempt, security, grant,
                    deadlineEpochMs, stream, writePlan, grouped.get(index),
                    targetTableUuid, baseSnapshotId));
        }
        return new CdcTaskPlan(List.copyOf(tasks), Map.copyOf(admittedEnds));
    }

    /** Planned tasks plus the exact source boundary to acknowledge after publication. */
    public record CdcTaskPlan(List<TaskRequest> tasks, Map<TopicPartitionOffset, Long> endOffsets) {
        public CdcTaskPlan {
            tasks = List.copyOf(tasks);
            endOffsets = Map.copyOf(endOffsets);
        }
    }
}
