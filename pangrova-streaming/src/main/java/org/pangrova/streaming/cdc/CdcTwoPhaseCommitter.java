package org.pangrova.streaming.cdc;

import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.commit.CdcCommitCoordinator;
import org.pangrova.streaming.commit.CommitParticipant;
import org.pangrova.streaming.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.commit.SourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Stages and publishes one source batch, persists its complete checkpoint, then
 * acknowledges source offsets. Catalog publication and JDBC do not share a
 * transaction: ambiguous catalog outcomes still require durable reconciliation.
 */
final class CdcTwoPhaseCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(CdcTwoPhaseCommitter.class);

    private final CdcPipelineConfig config;
    private final CdcPipelineLoopContext deps;

    CdcTwoPhaseCommitter(CdcPipelineConfig config, CdcPipelineLoopContext deps) {
        this.config = config;
        this.deps = deps;
    }

    /** Publishes one complete batch before acknowledging any source offsets. */
    void commitWithTwoPhaseProtocol(List<CdcEnvelope> envelopes, CdcBatchCheckpoint checkpoint) {
        CdcCommitCoordinator coordinator = deps.commitCoordinator();
        SourceId source = config.sourceId();
        var write = new TableWriteParticipant(envelopes);
        boolean committed = coordinator.executeAtomicCommit(source, checkpoint, List.of(write));
        if (!committed) {
            LOG.info("event=cdc_batch_dedup_skipped stream={} partitions={}",
                    config.streamName(), checkpoint.ranges().size());
        }
        // JDBC now records publication and all source ranges. If acknowledgement
        // fails, restart seeks to this checkpoint without publishing the rows again.
        deps.sourceReader().commitOffsets(checkpoint.nextOffsets());
    }

    /** Estimates the batch size in bytes for quota enforcement accounting. */
    long estimateBatchBytes(List<CdcEnvelope> envelopes) {
        if (envelopes.isEmpty()) {
            return 0L;
        }
        // Use first envelope's column count for a batch-level estimate.
        // Average 64 bytes per column is a reasonable heuristic for mixed types.
        CdcEnvelope sample = envelopes.get(0);
        org.pangrova.streaming.api.cdc.CdcRow row = sample.after() != null ? sample.after() : sample.before();
        int columnsPerRow = (row != null) ? row.columnCount() : 4;
        long bytesPerRow = (long) columnsPerRow * 64L;
        return (long) envelopes.size() * bytesPerRow;
    }

    /**
     * Participant that writes CDC envelopes to the lake table during the prepare phase.
     */
    private final class TableWriteParticipant implements CommitParticipant {
        private final List<CdcEnvelope> envelopes;
        private volatile org.pangrova.streaming.api.cdc.CdcWriteSink.StagedWrite staged;

        TableWriteParticipant(List<CdcEnvelope> envelopes) {
            this.envelopes = envelopes;
        }

        @Override
        public String name() {
            return "table-write";
        }

        @Override
        public void prepare() {
            if (envelopes.isEmpty()) {
                return;
            }
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
            if (!envelopes.isEmpty()) {
                CdcWriteResult result = deps.writeSink().publish(staged);
                deps.maintenanceScheduler().onWriteComplete(config.targetTable(), new WriteResult(
                        config.targetTable(), result.rowsWritten(), result.dataFilesProduced(),
                        0, 0, Instant.now()));
            }
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

}
