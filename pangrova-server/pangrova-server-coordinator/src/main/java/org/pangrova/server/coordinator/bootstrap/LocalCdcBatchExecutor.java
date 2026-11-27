package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority;
import org.pangrova.coordinator.query.task.write.WriteCommitRecoveryService;
import org.pangrova.coordinator.streaming.CdcStreamExecution;
import org.pangrova.coordinator.streaming.CdcTaskPlanner.CdcTaskPlan;
import org.pangrova.coordinator.streaming.CdcWriteAuthority;
import org.pangrova.coordinator.streaming.CdcWriteCommitCoordinator;
import org.pangrova.coordinator.streaming.CdcWriteIntentFactory;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.transaction.WriteCommitQuarantineStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.cdc.BoundCdcPageSinkWriter;
import org.pangrova.streaming.cdc.CdcRangeIngestSpec;
import org.pangrova.streaming.cdc.CdcRangeIngestor;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.cdc.CdcTaskRange;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Stages exact CDC ranges in a maintenance-only process and publishes through the durable write journal. */
final class LocalCdcBatchExecutor {
    private static final int MAX_RECORDS_PER_POLL = 1_000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

    private final WriteCommitJournal journal;
    private final CoordinatorExecutionAuthority authority;
    private final WriteCommitRecoveryService recovery;
    private final WriteCommitQuarantineStore quarantine;
    private final CdcRecordConverterRegistry converters;

    LocalCdcBatchExecutor(WriteCommitJournal journal, CoordinatorExecutionAuthority authority,
            WriteCommitRecoveryService recovery, WriteCommitQuarantineStore quarantine) {
        this.journal = journal;
        this.authority = authority;
        this.recovery = recovery;
        this.quarantine = quarantine;
        converters = CdcRecordConverterRegistry.discover(LocalCdcBatchExecutor.class.getClassLoader());
    }

    void execute(StreamState stream, CatalogHandle catalog, ConnectorSession session,
            CdcSourceReader reader, CdcWriteAuthority.CdcWritePlan writePlan, CdcTaskPlan plan,
            CdcSourceIdentity source, ExecutionAnchor anchor, BooleanSupplier stagingCancelled, Runnable fence,
            CdcWriteCommitCoordinator.Publisher publisher, CdcStreamExecution.OffsetCommitter acknowledge) {
        var converter = converters.get(stream.sourceFormat());
        var writer = new BoundCdcPageSinkWriter(catalog.getPageSinkProvider(), session, writePlan.outputHandle());
        var allowed = CdcWriteAuthority.privileges(stream);
        var fragments = new ArrayList<byte[]>();
        for (var task : plan.tasks()) {
            var ranges = task.getCdcIngest().getRangesList().stream().map(range -> new CdcTaskRange(
                    range.getTopic(), range.getPartition(), range.getStartOffset(), range.getEndOffset())).toList();
            var spec = new CdcRangeIngestSpec(stream.streamName(), stream.checkpointId(), ranges,
                    MAX_RECORDS_PER_POLL, POLL_TIMEOUT, IDLE_TIMEOUT);
            // Only private files are staged here. Check durable ownership again before recording the intent.
            fragments.addAll(new CdcRangeIngestor(spec, reader, converter, envelopes -> {
                envelopes.forEach(envelope -> {
                    Privilege operation = Privilege.valueOf(envelope.operation().name());
                    if (!allowed.contains(operation)) {
                        throw new AccessDeniedException("CDC stream is not authorized for " + operation);
                    }
                });
                return writer.writeBatch(envelopes);
            }, () -> Thread.currentThread().isInterrupted() || stagingCancelled.getAsBoolean()
                    || System.currentTimeMillis() >= task.getDeadlineEpochMs())
                    .ingest(writer.requiresSingleRange()).commitFragments());
        }
        fence.run();
        var intent = CdcWriteIntentFactory.create(anchor.executionId(), stream, session, writePlan, source,
                fragments, writePlan.outputHandle().writeBaseSnapshotId().orElse(-1L), System.currentTimeMillis(),
                anchor.ownerId(), anchor.ownerIncarnation(), anchor.fencingToken());
        new CdcWriteCommitCoordinator(journal, publisher,
                (actual, fingerprint) -> acknowledge.commit(actual, plan.endOffsets(), fingerprint)).commit(intent);
    }

    void recover(WriteCommitEntry entry, Runnable fence) {
        if (quarantine.exists(entry.queryId(), entry.commitFingerprint())) {
            return;
        }
        fence.run();
        authority.acquire(entry.queryId()).ifPresent(anchor -> {
            try {
                recovery.recover(Map.of(entry.queryId(), anchor.fencingToken()), (id, token) -> {
                    fence.run();
                    return authority.backend().current(id).filter(current -> current.fencingToken() == token
                            && current.ownerIncarnation().equals(anchor.ownerIncarnation())).isPresent();
                });
            } catch (RuntimeException | Error failure) {
                try {
                    authority.yield(anchor);
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            authority.release(anchor);
        });
    }
}
