package org.pangrova.persistence.generation.api.execution;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate.PublicationRead;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore.ExchangeEvidence;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Durable stage operations guarded by current authority and the exact backend-issued execution owner. */
@Internal
public interface GenerationStageExecutionStore {
    int PARTITION_WINDOW = 8;

    /** Resolves the current job and immutable exchange evidence under the same authority boundary. */
    PreparedExecution prepareExecution(GenerationJobKey key, ExecutionAnchor anchor);

    /** Commits the original producer winner before the ordinary DAG observes a terminal task. */
    boolean completeProducer(ProducerCompletion completion);

    /**
     * Claims an exact partition and prepares immutable local metadata before releasing its transaction locks.
     * The callback must not perform network I/O. Any callback failure rolls back the claim and attempt.
     */
    default <T> Optional<T> preparePartition(PartitionClaim request, Function<PreparedPartition, T> prepare) {
        return preparePartitions(List.of(request), prepare).stream().findFirst();
    }

    /**
     * Prepares one ordered cohort of up to PARTITION_WINDOW exact partitions from one stage.
     * The cohort shares one authority, clock, lease duration and control term. Unclaimable partitions
     * yield an empty result before any new lease is issued. Callback failure rolls back the whole cohort.
     * Callbacks prepare local immutable data only. The result is available after commit.
     */
    <T> List<T> preparePartitions(List<PartitionClaim> requests, Function<PreparedPartition, T> prepare);

    /**
     * Locks one stage window and prepares stored winners and new claims together, in partition order.
     * Both callbacks prepare local data only. A blocked page or a callback failure issues no leases.
     */
    <T> List<T> prepareWindow(PartitionWindow window, BiFunction<Integer, NativeWinner, T> restore,
            Function<PreparedPartition, T> prepare);

    /** An exact contiguous physical window under a single durable execution anchor. */
    record PartitionWindow(GenerationJobKey job, ExecutionAnchor anchor, int stageId, int firstPartition,
            int count, long controlTerm, Duration leaseDuration, Instant now) {
        public PartitionWindow {
            requireClaimAuthority(job, anchor, controlTerm, leaseDuration, now);
            if (stageId < 0 || firstPartition < 0 || count < 1 || count > PARTITION_WINDOW) {
                throw new IllegalArgumentException("Invalid durable partition window");
            }
        }
    }

    /**
     * Revalidates an issued lease and prepares local metadata inside the same current-authority transaction.
     * Input evidence must use that transaction; the callback must not perform network I/O.
     */
    <T> T prepareLeasedPartition(GenerationLease lease, Instant now, Function<PreparedPartition, T> prepare);

    /** Current-authority graph view for the shared DAG runtime; no stored principal becomes a live session. */
    record PreparedExecution(CurrentGenerationAuthority authority, GenerationJob job,
            List<ExchangeEvidence> exchanges) {
        public PreparedExecution {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(job, "job");
            exchanges = List.copyOf(exchanges);
        }
    }

    /** Original terminal evidence belongs to the issued producer, never to the new replay owner. */
    record NativeWinner(String pageId, int attemptId, long fencingToken, long controlTerm,
            GenerationExecutionOwner executionOwner, byte[] originalWireBytes, String wireSha256) {
        public NativeWinner {
            originalWireBytes = Objects.requireNonNull(originalWireBytes,
                    "Persisted Native winner is missing original evidence; explicit rebuild required").clone();
            Objects.requireNonNull(executionOwner, "executionOwner");
            if (originalWireBytes.length == 0
                    || originalWireBytes.length > ProducerCompletion.MAX_ORIGINAL_REPORT_BYTES
                    || !org.pangrova.common.util.DigestUtils.matchesSha256(originalWireBytes, wireSha256)) {
                throw new IllegalStateException("Persisted Native winner has missing or corrupted original evidence");
            }
        }
        @Override public byte[] originalWireBytes() { return originalWireBytes.clone(); }
    }

    /** Exact persisted coordinates and actual backend-issued execution identity; control term is not a page fence. */
    record PartitionClaim(GenerationJobKey job, ExecutionAnchor anchor, int stageId, int partitionId,
            int pageOrdinal, long controlTerm, Duration leaseDuration, Instant now) {
        public PartitionClaim {
            requireClaimAuthority(job, anchor, controlTerm, leaseDuration, now);
            if (stageId < 0 || partitionId < 0 || pageOrdinal < 0) {
                throw new IllegalArgumentException("Durable page coordinates must be nonnegative");
            }
        }
    }

    /** Ephemeral current authority and immutable input metadata; no user/session impersonation or data rows. */
    record PreparedPartition(CurrentGenerationAuthority authority, GenerationJob job,
            GenerationLease lease, Map<PublicationRead, PublishedInput> publications,
            List<GenerationOutput> dependencyOutputs,
            List<ExchangeEvidence> exchanges) {
        public PreparedPartition {
            publications = Map.copyOf(publications);
            dependencyOutputs = List.copyOf(dependencyOutputs);
            exchanges = List.copyOf(exchanges);
        }
    }

    /** Original validated Native producer report and its dispatched attempt, not a fabricated artifact report. */
    record ProducerCompletion(GenerationJobKey job, GenerationExecutionOwner executionOwner,
            int stageId, int partitionId, long planEpoch, int attemptId, long fencingToken, long controlTerm,
            byte[] originalWireBytes, Instant completedAt) {
        private static final int MAX_ORIGINAL_REPORT_BYTES = 4 * 1024 * 1024;

        public ProducerCompletion {
            Objects.requireNonNull(job, "job");
            Objects.requireNonNull(executionOwner, "executionOwner");
            Objects.requireNonNull(completedAt, "completedAt");
            originalWireBytes = originalWireBytes.clone();
            requireProducerCoordinates(stageId, partitionId, planEpoch, attemptId, fencingToken, controlTerm);
            if (originalWireBytes.length == 0 || originalWireBytes.length > MAX_ORIGINAL_REPORT_BYTES) {
                throw new IllegalArgumentException(
                        "Native producer completion requires bounded original wire evidence");
            }
        }
        @Override public byte[] originalWireBytes() { return originalWireBytes.clone(); }
    }

    /** Immutable publication evidence already read on the claim connection, not a second pool borrow. */
    record PublishedInput(GenerationManifest manifest, List<GenerationOutput> outputs,
            Set<String> sourceObjectIds) {
        public PublishedInput {
            outputs = List.copyOf(outputs);
            sourceObjectIds = Set.copyOf(sourceObjectIds);
        }
    }

    private static void requireClaimAuthority(GenerationJobKey job, ExecutionAnchor anchor, long controlTerm,
            Duration leaseDuration, Instant now) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(anchor, "anchor");
        if (!job.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("Partition claim belongs to another durable execution");
        }
        if (controlTerm <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("Partition lease duration must be positive");
        }
        Objects.requireNonNull(now, "now");
    }

    private static void requireProducerCoordinates(int stage, int partition, long epoch,
            int attempt, long fence, long term) {
        if (stage < 0 || partition < 0 || epoch <= 0 || attempt < 0 || fence <= 0 || term <= 0) {
            throw new IllegalArgumentException("Native producer completion requires its exact issued attempt");
        }
    }
}
