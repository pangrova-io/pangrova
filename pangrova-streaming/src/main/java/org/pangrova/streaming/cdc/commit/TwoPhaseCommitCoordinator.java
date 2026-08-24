package org.pangrova.streaming.cdc.commit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.spi.streaming.TopicPartitionOffset;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC-backed implementation of {@link CdcCommitCoordinator} that orchestrates
 * the two-phase commit protocol for CDC exactly-once semantics.
 *
 * <p>Deduplication is based on the (source_id, partition, start_offset, end_offset)
 * tuple persisted in the {@code cdc_commit_state} table. Duplicate prepare calls
 * for the same tuple return the existing token without side effects.</p>
 *
 * <p>Thread-safe: delegates all state mutations to {@link JdbcCdcCommitStateStore}
 * which acquires per-operation connections from the pool.</p>
 */
@ThreadSafe
public final class TwoPhaseCommitCoordinator implements CdcCommitCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitCoordinator.class);

    private static final Duration DEFAULT_PREPARE_DEADLINE = Duration.ofMinutes(5);

    private final JdbcCdcCommitStateStore stateStore;
    private final Duration prepareDeadline;

    public TwoPhaseCommitCoordinator(JdbcCdcCommitStateStore stateStore) {
        this(stateStore, DEFAULT_PREPARE_DEADLINE);
    }

    public TwoPhaseCommitCoordinator(JdbcCdcCommitStateStore stateStore, Duration prepareDeadline) {
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore must not be null");
        this.prepareDeadline = Objects.requireNonNull(prepareDeadline, "prepareDeadline must not be null");
    }

    @Override
    public PrepareToken prepare(SourceId source, OffsetRange range, String dedupKey) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(range, "range must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");

        Optional<PrepareToken> existing = stateStore.findDuplicate(source, dedupKey);
        if (existing.isPresent()) {
            LOG.info("event=cdc_prepare_idempotent source={} partition={} offset=[{},{}]",
                    source, range.partition(), range.startOffset(), range.endOffset());
            return existing.get();
        }

        PrepareToken token = new PrepareToken(
                UUID.randomUUID(),
                source,
                range,
                Instant.now().plus(prepareDeadline));

        boolean inserted = stateStore.insert(token, CdcCommitState.PREPARED, dedupKey);
        if (!inserted) {
            // Race condition: another thread inserted between findDuplicate and insert.
            // Re-read to get the existing token.
            return stateStore.findDuplicate(source, dedupKey)
                    .orElseThrow(() -> new PangrovaException(
                            PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                            "Failed to prepare CDC commit: concurrent insert detected but row not found"
                                    + " source=" + source + " range=" + range));
        }

        LOG.info("event=cdc_prepare_success id={} source={} partition={} offset=[{},{}]",
                token.id(), source, range.partition(), range.startOffset(), range.endOffset());
        return token;
    }

    @Override
    public void commit(PrepareToken token) {
        Objects.requireNonNull(token, "token must not be null");

        Optional<CdcCommitState> currentState = stateStore.findStateById(token.id());
        if (currentState.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Cannot commit unknown transaction id=" + token.id());
        }

        CdcCommitState state = currentState.get();
        if (state == CdcCommitState.COMMITTED) {
            LOG.info("event=cdc_commit_idempotent id={}", token.id());
            return;
        }
        if (state == CdcCommitState.ROLLED_BACK) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Cannot commit already rolled-back transaction id=" + token.id());
        }

        stateStore.updateState(token.id(), CdcCommitState.COMMITTING);

        try {
            stateStore.updateState(token.id(), CdcCommitState.COMMITTED);
            LOG.info("event=cdc_commit_success id={} source={} partition={} offset=[{},{}]",
                    token.id(), token.source(), token.range().partition(),
                    token.range().startOffset(), token.range().endOffset());
        } catch (RuntimeException e) {
            LOG.error("event=cdc_commit_failed id={} error={}", token.id(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void rollback(PrepareToken token) {
        Objects.requireNonNull(token, "token must not be null");

        Optional<CdcCommitState> currentState = stateStore.findStateById(token.id());
        if (currentState.isEmpty()) {
            LOG.warn("event=cdc_rollback_unknown id={}", token.id());
            return;
        }

        CdcCommitState state = currentState.get();
        if (state == CdcCommitState.ROLLED_BACK) {
            LOG.info("event=cdc_rollback_idempotent id={}", token.id());
            return;
        }
        if (state == CdcCommitState.COMMITTED) {
            throw new PangrovaException(PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                    "Cannot rollback already committed transaction id=" + token.id());
        }

        stateStore.updateState(token.id(), CdcCommitState.ROLLING_BACK);
        stateStore.updateState(token.id(), CdcCommitState.ROLLED_BACK);
        LOG.info("event=cdc_rollback_success id={} source={}", token.id(), token.source());
    }

    @Override
    public void rollback(PrepareToken token, List<CommitParticipant> participants) {
        Objects.requireNonNull(token, "token must not be null");
        // F-STREAM-2PC: abort participant side effects (delete prepared data files,
        // revert offsets) BEFORE marking the commit record rolled back. This
        // guarantees the CommitParticipant.abort() contract is honoured on every
        // synchronous rollback path, not only inside executeAtomicCommit.
        abortAll(participants != null ? participants : List.of());
        rollback(token);
    }

    @Override
    public List<PrepareToken> recover(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");

        List<PrepareToken> inFlight = stateStore.findInFlight(source);
        if (inFlight.isEmpty()) {
            LOG.info("event=cdc_recover_none source={}", source);
            return List.of();
        }

        List<PrepareToken> recovered = new ArrayList<>();
        Instant now = Instant.now();

        for (PrepareToken token : inFlight) {
            Optional<CdcCommitState> state = stateStore.findStateById(token.id());
            if (state.isEmpty()) {
                continue;
            }

            switch (state.get()) {
                case PREPARED -> {
                    if (now.isAfter(token.deadline())) {
                        // Cross-process recovery: participant runtime handles are gone,
                        // so only the commit record can be reclaimed here. Prepare-phase
                        // side effects (if any) MUST be reclaimable orphans per the
                        // CommitParticipant contract — uncommitted data files are never
                        // referenced by a snapshot and are collected by the storage
                        // layer's orphan/expire maintenance (F-STREAM-2PC).
                        LOG.warn("event=cdc_recover_rollback_expired id={} deadline={} "
                                        + "note=participant_abort_unavailable_cross_process",
                                token.id(), token.deadline());
                        rollback(token);
                    }
                    recovered.add(token);
                }
                case COMMITTING -> {
                    LOG.info("event=cdc_recover_retry_commit id={}", token.id());
                    commit(token);
                    recovered.add(token);
                }
                case ROLLING_BACK -> {
                    LOG.info("event=cdc_recover_retry_rollback id={}", token.id());
                    stateStore.updateState(token.id(), CdcCommitState.ROLLED_BACK);
                    recovered.add(token);
                }
                default -> { /* terminal states filtered by query */ }
            }
        }

        LOG.info("event=cdc_recover_complete source={} recovered={}", source, recovered.size());
        return Collections.unmodifiableList(recovered);
    }

    @Override
    public boolean executeAtomicCommit(SourceId source, OffsetRange range, String dedupKey,
                                       List<CommitParticipant> participants) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(range, "range must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
        if (participants == null || participants.isEmpty()) {
            throw new IllegalArgumentException("participants must not be null or empty");
        }

        // Phase 0: prepare the commit record (dedup check)
        PrepareToken token = prepare(source, range, dedupKey);

        // Dedup: if this offset range was already committed (crash-recovery replay), skip
        Optional<CdcCommitState> currentState = stateStore.findStateById(token.id());
        if (currentState.isPresent() && currentState.get() == CdcCommitState.COMMITTED) {
            LOG.info("event=cdc_atomic_commit_dedup source={} partition={} offset=[{},{}]",
                    source, range.partition(), range.startOffset(), range.endOffset());
            return false;
        }

        // Phase 1: prepare all participants
        List<CommitParticipant> prepared = prepareParticipants(participants, source, range, token);

        // Phase 2: commit all participants + mark committed
        return commitParticipants(prepared, source, range, token);
    }

    private List<CommitParticipant> prepareParticipants(List<CommitParticipant> participants,
                                                        SourceId source, OffsetRange range,
                                                        PrepareToken token) {
        List<CommitParticipant> prepared = new ArrayList<>(participants.size());
        try {
            for (CommitParticipant participant : participants) {
                participant.prepare();
                prepared.add(participant);
            }
        } catch (Exception e) {
            LOG.error("event=cdc_atomic_commit_prepare_failed source={} participant={} error={}",
                    source, prepared.size() < participants.size()
                            ? participants.get(prepared.size()).name() : "unknown",
                    e.getMessage(), e);
            rollback(token, prepared);
            throw new RuntimeException("CDC atomic commit prepare failed for source=" + source
                    + " range=" + range, e);
        }
        return prepared;
    }

    private boolean commitParticipants(List<CommitParticipant> prepared,
                                       SourceId source, OffsetRange range,
                                       PrepareToken token) {
        try {
            for (CommitParticipant participant : prepared) {
                participant.commit();
            }
            commit(token);
            LOG.info("event=cdc_atomic_commit_success source={} partition={} offset=[{},{}] participants={}",
                    source, range.partition(), range.startOffset(), range.endOffset(),
                    prepared.size());
            return true;
        } catch (Exception e) {
            LOG.error("event=cdc_atomic_commit_failed source={} error={}", source, e.getMessage(), e);
            rollback(token, prepared);
            throw new RuntimeException("CDC atomic commit failed for source=" + source
                    + " range=" + range, e);
        }
    }

    @Override
    public Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");

        Map<Integer, Long> partitionOffsets = stateStore.findLastCommittedOffsets(source);
        if (partitionOffsets.isEmpty()) {
            LOG.info("event=cdc_no_committed_offsets source={}", source);
            return Map.of();
        }

        // Convert partition number to TopicPartitionOffset using source id as topic
        Map<TopicPartitionOffset, Long> result = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
            result.put(new TopicPartitionOffset(source.value(), entry.getKey()),
                    entry.getValue());
        }

        LOG.info("event=cdc_last_committed_offsets source={} partitions={}", source, result.size());
        return Collections.unmodifiableMap(result);
    }

    private void abortAll(List<CommitParticipant> prepared) {
        for (CommitParticipant participant : prepared) {
            try {
                participant.abort();
            } catch (Exception abortEx) {
                LOG.warn("event=cdc_participant_abort_failed participant={} error={}",
                        participant.name(), abortEx.getMessage(), abortEx);
            }
        }
    }
}
