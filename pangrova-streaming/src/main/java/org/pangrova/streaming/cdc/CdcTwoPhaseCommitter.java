package org.pangrova.streaming.cdc;

import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcWriteResult;
import org.pangrova.spi.streaming.TopicPartitionOffset;
import org.pangrova.streaming.cdc.commit.CdcCommitCoordinator;
import org.pangrova.streaming.cdc.commit.CommitParticipant;
import org.pangrova.streaming.cdc.commit.OffsetRange;
import org.pangrova.streaming.cdc.commit.SourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Coordinates the table write and source-offset acknowledgement of a CDC batch.
 *
 * <p>For each partition in the current batch, the coordinator records the
 * offset range in JDBC. If all partitions are already committed (crash-recovery
 * replay), the write is skipped. Otherwise, two {@link CommitParticipant}s
 * are orchestrated:</p>
 * <ol>
 *   <li>{@code TableWriteParticipant}: writes envelopes to the lake table during
 *       the prepare phase.</li>
 *   <li>{@code OffsetCommitParticipant}: commits source offsets during the commit
 *       phase.</li>
 * </ol>
 *
 * <p>The table catalog and source offset store do not share an atomic transaction.
 * A failure after catalog publication is ambiguous and requires connector-level
 * reconciliation before the source can safely advance. The data write happens
 * once for the whole batch, not per-partition.</p>
 */
final class CdcTwoPhaseCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(CdcTwoPhaseCommitter.class);

    private final CdcPipelineConfig config;
    private final CdcPipelineLoopContext deps;

    CdcTwoPhaseCommitter(CdcPipelineConfig config, CdcPipelineLoopContext deps) {
        this.config = config;
        this.deps = deps;
    }

    /**
     * Commits the given envelopes with the two-phase protocol.
     *
     * @param envelopes       the converted CDC envelopes of the current batch
     * @param currentCycleOffsets next-to-consume offsets (max+1) of the current batch
     */
    void commitWithTwoPhaseProtocol(List<CdcEnvelope> envelopes,
                                    Map<TopicPartitionOffset, Long> currentCycleOffsets) {
        CdcCommitCoordinator coordinator = deps.commitCoordinator();
        SourceId source = new SourceId(config.streamName());

        // Build per-partition offset ranges from the batch.
        // currentCycleOffsets stores next-to-consume (max+1); extractMinOffsets
        // captures the first offset per partition from the raw batch.
        Map<TopicPartitionOffset, Long> maxOffsets = currentCycleOffsets;

        // A-EOS-01: the dedup key is a COLLISION-FREE fingerprint over ALL partitions of the batch,
        // not a single partition's range. Two distinct batches that happen to share a primary-
        // partition range therefore no longer collide (which previously mis-deduped the second batch
        // and silently lost its other-partition data). The fingerprint is deterministic, so a
        // crash-recovery replay of the same batch produces the same key and is correctly deduped.
        String batchKey = computeBatchFingerprint(maxOffsets);

        // The representative range (lowest partition, chosen deterministically) is still recorded so
        // findLastCommittedOffsets has a per-batch offset to restore from — it is NO LONGER the dedup key.
        Map.Entry<TopicPartitionOffset, Long> primary = maxOffsets.entrySet().stream()
                .min(java.util.Comparator
                        .comparingInt((Map.Entry<TopicPartitionOffset, Long> e) -> e.getKey().partition())
                        .thenComparing(e -> e.getKey().toString()))
                .orElseThrow();
        TopicPartitionOffset primaryTpo = primary.getKey();
        long primaryEndOffset = primary.getValue() - 1;

        OffsetRange primaryRange = new OffsetRange(
                primaryTpo.partition(), 0, primaryEndOffset);

        List<CommitParticipant> participants = List.of(
                new TableWriteParticipant(envelopes),
                new OffsetCommitParticipant(maxOffsets));

        boolean committed = coordinator.executeAtomicCommit(source, primaryRange, batchKey, participants);
        if (!committed) {
            LOG.info("event=cdc_batch_dedup_skipped stream={} partitions={}",
                    config.streamName(), maxOffsets.size());
            // Dedup: data was already committed in a prior run. Source offsets
            // may not have been committed (crash between COMMITTED and offset commit),
            // so commit them now to advance the consumer.
            deps.sourceReader().commitOffsets(maxOffsets);
        }
    }

    /**
     * A-EOS-01: computes a deterministic, collision-free fingerprint over ALL partitions of the
     * batch (sorted by partition identity), used as the exactly-once dedup key. Distinct batches
     * yield distinct fingerprints; a replay of the same batch yields the same fingerprint.
     */
    private static String computeBatchFingerprint(Map<TopicPartitionOffset, Long> maxOffsets) {
        String canonical = maxOffsets.entrySet().stream()
                .sorted(java.util.Comparator.comparing(
                        (Map.Entry<TopicPartitionOffset, Long> e) -> e.getKey().toString()))
                .map(e -> e.getKey().toString() + "=" + e.getValue())
                .collect(java.util.stream.Collectors.joining(","));
        return org.pangrova.common.util.DigestUtils.sha256Hex(canonical);
    }

    /** Estimates the batch size in bytes for quota enforcement accounting. */
    long estimateBatchBytes(List<CdcEnvelope> envelopes) {
        if (envelopes.isEmpty()) {
            return 0L;
        }
        // Use first envelope's column count for a batch-level estimate.
        // Average 64 bytes per column is a reasonable heuristic for mixed types.
        CdcEnvelope sample = envelopes.get(0);
        org.pangrova.spi.streaming.CdcRow row = sample.after() != null ? sample.after() : sample.before();
        int columnsPerRow = (row != null) ? row.columnCount() : 4;
        long bytesPerRow = (long) columnsPerRow * 64L;
        return (long) envelopes.size() * bytesPerRow;
    }

    /**
     * Participant that writes CDC envelopes to the lake table during the prepare phase.
     */
    private final class TableWriteParticipant implements CommitParticipant {
        private final List<CdcEnvelope> envelopes;
        private volatile org.pangrova.spi.streaming.CdcWriteSink.StagedWrite staged;

        TableWriteParticipant(List<CdcEnvelope> envelopes) {
            this.envelopes = envelopes;
        }

        @Override
        public String name() {
            return "table-write";
        }

        @Override
        public void prepare() {
            // Stage the write before publishing to the catalog. Crash recovery requires
            // a durable sink token; the generic in-memory default does not provide it.
            long estimatedBytes = estimateBatchBytes(envelopes);
            deps.quotaEnforcer().onWriteStart(estimatedBytes);
            try {
                staged = deps.writeSink().stage(envelopes);
                if (staged == null) {
                    throw new IllegalStateException(
                            "CdcWriteSink.stage(...) returned null; cannot proceed to 2PC commit");
                }
            } finally {
                deps.quotaEnforcer().onWriteComplete(estimatedBytes);
            }
        }

        @Override
        public void commit() throws Exception {
            // Publish through the sink; catalog atomicity and replay safety are sink-specific.
            CdcWriteResult result = deps.writeSink().publish(staged);
            WriteResult maintenanceResult = new WriteResult(
                config.targetTable(),
                result.rowsWritten(),
                result.dataFilesProduced(),
                0,
                0,
                Instant.now());
            deps.maintenanceScheduler().onWriteComplete(config.targetTable(), maintenanceResult);
        }

        @Override
        public void abort() {
            // Discard only files that the sink can prove were never published. The source
            // replays from the last committed offset.
            if (staged != null) {
                try {
                    deps.writeSink().discard(staged);
                } catch (RuntimeException e) {
                    LOG.warn("event=cdc_table_write_discard_failed stream={} error={}",
                            config.streamName(), e.getMessage());
                }
            }
            LOG.debug("event=cdc_table_write_aborted stream={}", config.streamName());
        }
    }

    /**
     * Participant that commits source offsets during the commit phase.
     */
    private final class OffsetCommitParticipant implements CommitParticipant {
        private final Map<TopicPartitionOffset, Long> offsets;

        OffsetCommitParticipant(Map<TopicPartitionOffset, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public String name() {
            return "offset-commit";
        }

        @Override
        public void prepare() {
            // Offset readiness is always true — nothing to validate
        }

        @Override
        public void commit() throws Exception {
            deps.sourceReader().commitOffsets(offsets);
        }

        @Override
        public void abort() {
            // No-op: offsets not committed = safe; source will replay from old position
        }
    }
}
