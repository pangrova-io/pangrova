package org.pangrova.streaming.commit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.streaming.api.enums.CdcCommitState;
import org.pangrova.streaming.api.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.api.commit.CdcCommitStateStore;
import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.streaming.api.commit.PrepareToken;

/**
 * JDBC-backed implementation of {@link CdcCommitCoordinator} that orchestrates
 * the CDC commit state machine. Catalog publication and source-offset acknowledgement
 * are not one atomic transaction; ambiguous publication remains in COMMITTING for
 * explicit reconciliation rather than being reported as exactly-once.
 *
 * <p>Deduplication is based on the source-scoped full-batch key persisted in
 * {@code cdc_commit_state}. Duplicate live or committed prepares return the
 * existing token; rolled-back prepares may be retried with a new token.</p>
 *
 * <p>Thread-safe: delegates all state mutations to the injected
 * {@link CdcCommitStateStore}, whose implementation acquires per-operation
 * connections from the pool.</p>
 */
@ThreadSafe
public final class TwoPhaseCommitCoordinator implements CdcCommitCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseCommitCoordinator.class);

    private static final Duration DEFAULT_PREPARE_DEADLINE = Duration.ofMinutes(5);

    private final CdcCommitStateStore stateStore;
    private final Duration prepareDeadline;

    public TwoPhaseCommitCoordinator(CdcCommitStateStore stateStore) {
        this(stateStore, DEFAULT_PREPARE_DEADLINE);
    }

    public TwoPhaseCommitCoordinator(CdcCommitStateStore stateStore, Duration prepareDeadline) {
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore must not be null");
        this.prepareDeadline = Objects.requireNonNull(prepareDeadline, "prepareDeadline must not be null");
    }

    @Override
    public PrepareToken prepare(SourceId source, CdcBatchCheckpoint checkpoint, String dedupKey) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");

        Optional<PrepareToken> existing = stateStore.findDuplicate(source, checkpoint, dedupKey);
        if (existing.isPresent()) {
            LOG.info("event=cdc_prepare_idempotent source={} partitions={}",
                    source, checkpoint.ranges().size());
            return existing.get();
        }

        PrepareToken token = new PrepareToken(
                UUID.randomUUID(),
                source,
                checkpoint,
                Instant.now().plus(prepareDeadline));

        boolean inserted = stateStore.insert(token, CdcCommitState.PREPARED, dedupKey);
        if (!inserted) {
            // Race condition: another thread inserted between findDuplicate and insert.
            // Re-read to get the existing token.
            return stateStore.findDuplicate(source, checkpoint, dedupKey)
                    .orElseThrow(() -> new PangrovaException(
                            PangrovaErrorCode.STREAMING_CDC_PARSE_ERROR,
                            "Failed to prepare CDC commit: concurrent insert detected but row not found"
                                    + " source=" + source + " checkpoint=" + checkpoint));
        }

        LOG.info("event=cdc_prepare_success id={} source={} partitions={}",
                token.id(), source, checkpoint.ranges().size());
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
        if (state != CdcCommitState.COMMITTING) {
            throw new IllegalStateException(
                    "CDC commit cannot be acknowledged before publication starts; id="
                            + token.id() + " state=" + state);
        }

        try {
            if (!stateStore.compareAndSetState(
                    token.id(), CdcCommitState.COMMITTING, CdcCommitState.COMMITTED)) {
                throw new IllegalStateException(
                        "CDC commit state changed before completion; id=" + token.id());
            }
            LOG.info("event=cdc_commit_success id={} source={} partitions={}",
                    token.id(), token.source(), token.checkpoint().ranges().size());
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
        if (state == CdcCommitState.COMMITTING) {
            throw new IllegalStateException(
                    "Cannot rollback CDC transaction after catalog publication may have started; "
                            + "reconciliation required for id=" + token.id());
        }

        if (!stateStore.compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.ROLLING_BACK)) {
            throw new IllegalStateException(
                    "CDC commit state changed before rollback; id=" + token.id());
        }
        stateStore.updateState(token.id(), CdcCommitState.ROLLED_BACK);
        LOG.info("event=cdc_rollback_success id={} source={}", token.id(), token.source());
    }

    @Override
    public void rollback(PrepareToken token, List<CommitParticipant> participants) {
        Objects.requireNonNull(token, "token must not be null");
        CdcCommitState state = stateStore.findStateById(token.id())
                .orElseThrow(() -> new IllegalStateException(
                        "Cannot abort unknown CDC transaction; id=" + token.id()));
        if (state == CdcCommitState.ROLLED_BACK) {
            return;
        }
        if (state != CdcCommitState.PREPARED) {
            throw new IllegalStateException(
                    "Cannot abort CDC participants outside PREPARED state; id="
                            + token.id() + " state=" + state);
        }
        // Claim rollback before touching staged files. A concurrent coordinator
        // may otherwise claim COMMITTING after the state read and publish files
        // while this caller is aborting them.
        if (!stateStore.compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.ROLLING_BACK)) {
            throw new IllegalStateException(
                    "CDC commit state changed before participant rollback; id=" + token.id());
        }
        // Only unpublished side effects may be aborted. An already-COMMITTING
        // record could reference its files in an external catalog.
        abortAll(participants != null ? participants : List.of());
        stateStore.updateState(token.id(), CdcCommitState.ROLLED_BACK);
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
                    throw new IllegalStateException(
                            "CDC catalog publication outcome is ambiguous; "
                                    + "durable connector reconciliation is required for id="
                                    + token.id());
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
    public boolean executeAtomicCommit(SourceId source, CdcBatchCheckpoint checkpoint, String dedupKey,
                                       List<CommitParticipant> participants) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
        if (participants == null || participants.isEmpty()) {
            throw new IllegalArgumentException("participants must not be null or empty");
        }

        // Phase 0: prepare the commit record (dedup check)
        PrepareToken token = prepare(source, checkpoint, dedupKey);

        // Dedup: if this offset range was already committed (crash-recovery replay), skip
        Optional<CdcCommitState> currentState = stateStore.findStateById(token.id());
        if (currentState.isPresent() && currentState.get() == CdcCommitState.COMMITTED) {
            LOG.info("event=cdc_atomic_commit_dedup source={} partitions={}",
                    source, checkpoint.ranges().size());
            return false;
        }
        if (currentState.isPresent() && currentState.get() == CdcCommitState.COMMITTING) {
            throw new IllegalStateException(
                    "CDC catalog publication outcome is ambiguous; "
                            + "durable connector reconciliation is required for id="
                            + token.id());
        }

        // Phase 1: prepare all participants
        List<CommitParticipant> prepared = prepareParticipants(participants, source, checkpoint, token);

        // Phase 2: claim publication, commit participants, then mark committed.
        return commitParticipants(prepared, source, checkpoint, token);
    }

    private List<CommitParticipant> prepareParticipants(List<CommitParticipant> participants,
                                                        SourceId source, CdcBatchCheckpoint checkpoint,
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
            abortAll(prepared);
            if (stateStore.compareAndSetState(
                    token.id(), CdcCommitState.PREPARED, CdcCommitState.ROLLING_BACK)) {
                stateStore.updateState(token.id(), CdcCommitState.ROLLED_BACK);
            }
            throw new RuntimeException("CDC atomic commit prepare failed for source=" + source
                    + " range=" + checkpoint, e);
        }
        return prepared;
    }

    private boolean commitParticipants(List<CommitParticipant> prepared,
                                       SourceId source, CdcBatchCheckpoint checkpoint,
                                       PrepareToken token) {
        if (!stateStore.compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING)) {
            abortAll(prepared);
            throw new IllegalStateException(
                    "CDC commit was claimed by another coordinator; id=" + token.id());
        }
        try {
            for (CommitParticipant participant : prepared) {
                participant.commit();
            }
            commit(token);
            LOG.info("event=cdc_atomic_commit_success source={} partitions={} participants={}",
                    source, checkpoint.ranges().size(),
                    prepared.size());
            return true;
        } catch (Exception e) {
            LOG.error("event=cdc_atomic_commit_ambiguous source={} id={} error={}",
                    source, token.id(), e.getMessage(), e);
            throw new IllegalStateException(
                    "CDC commit outcome is ambiguous; durable connector reconciliation "
                            + "required for source=" + source + " range=" + checkpoint, e);
        }
    }

    @Override
    public Map<TopicPartitionOffset, Long> findLastCommittedOffsets(SourceId source) {
        Objects.requireNonNull(source, "source must not be null");

        return stateStore.findLastCommittedOffsets(source);
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
