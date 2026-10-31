package org.pangrova.databuild.store.jdbc.build;

import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.security.spi.jdbc.CurrentDirectRoleReader;
import org.pangrova.databuild.store.jdbc.execution.JdbcDataBuildExecutionFence;

import static org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries.loadSplits;
import static org.pangrova.databuild.store.jdbc.query.JdbcDataBuildQueries.loadDependencies;

import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobKey;
import org.pangrova.databuild.api.build.DataBuildLease;
import org.pangrova.databuild.api.execution.DataBuildExecutionOwner;
import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate.PublicationRead;
import org.pangrova.databuild.store.jdbc.execution.CurrentDeferredExecutionContext;
import org.pangrova.databuild.store.jdbc.exchange.JdbcDataBuildStageExchangeStore;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.query.api.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Same-authority transaction boundary for preparing one exact partition in the shared stage engine. */
@Internal
public final class JdbcDataBuildStageExecutionStore extends JdbcDataBuildStoreSupport {
    private final DataBuildTransactionAuthority transactionAuthority;
    private final JdbcDataBuildExecutionFence executionFence;
    private final CurrentDirectRoleReader currentRoles;

    /** Resolves current authority, execution fencing and roles on each stage transaction. */
    public JdbcDataBuildStageExecutionStore(
            DataSource dataSource, DataBuildTransactionAuthority transactionAuthority,
            JdbcDataBuildExecutionFence executionFence, CurrentDirectRoleReader currentRoles) {
        super(dataSource);
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.currentRoles = java.util.Objects.requireNonNull(currentRoles, "currentRoles");
    }

    /** Current-authority graph view for the shared DAG runtime; no stored principal becomes a live session. */
    public record PreparedExecution(CurrentDeferredExecutionContext authority, DataBuildJob job,
            List<JdbcDataBuildStageExchangeStore.ExchangeEvidence> exchanges) {
        public PreparedExecution {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(job, "job");
            exchanges = List.copyOf(exchanges);
        }
    }

    /** Original terminal evidence belongs to the issued producer, never to the new replay owner. */
    public record NativeWinner(String pageId, int attemptId, long fencingToken, long controlTerm,
            DataBuildExecutionOwner executionOwner, byte[] originalWireBytes, String wireSha256) {
        public NativeWinner {
            originalWireBytes = Objects.requireNonNull(originalWireBytes,
                    "Persisted Native winner is missing original evidence; explicit rebuild required").clone();
            Objects.requireNonNull(executionOwner, "executionOwner");
            if (originalWireBytes.length == 0 || originalWireBytes.length > 4 * 1024 * 1024
                    || !org.pangrova.common.util.DigestUtils.matchesSha256(originalWireBytes, wireSha256)) {
                throw new IllegalStateException("Persisted Native winner has missing or corrupted original evidence");
            }
        }
        @Override public byte[] originalWireBytes() { return originalWireBytes.clone(); }
    }

    /** One exact immutable page coordinate, with no loading of source rows or unbounded page enumeration. */
    public record PartitionProgress(int pageOrdinal, DataBuildPageKind kind, String state,
            Optional<NativeWinner> winner) {}

    /** Reads one partition's progress after current authority and actual owner validation on the same connection. */
    public PartitionProgress inspectPartition(DataBuildJobKey key, ExecutionAnchor anchor,
            int stageId, int partitionId) {
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("Partition inspection belongs to another durable execution");
        }
        return inTransaction("inspect current durable stage partition", connection -> {
            var context = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    DataBuildExecutionOwner.from(anchor));
            JdbcDataBuildStageExchangeStore.lockValidatedBarriers(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(stageId);
            if (partitionId < 0 || partitionId >= stage.parallelism()) {
                throw new IllegalArgumentException("Inspected partition differs from its immutable stage cohort");
            }
            var row = JdbcDataBuildPageSelection.findPartition(connection, key.tenantId(), key.jobId(),
                    stageId, partitionId).orElseThrow();
            if (row.kind() != stage.pageKind()) {
                throw new IllegalStateException("Persisted page kind differs from its immutable stage");
            }
            var winner = "SUCCEEDED".equals(row.state())
                    ? Optional.of(new NativeWinner(row.pageId(), row.attemptId(), row.fencingToken(), row.ownerEpoch(),
                            row.executionOwner(), row.reportBytes(), row.reportSha256()))
                    : Optional.<NativeWinner>empty();
            return new PartitionProgress(row.pageOrdinal(), row.kind(), row.state(), winner);
        });
    }

    /** Resolves the actual job and immutable exchange evidence through the same current transaction gate. */
    public PreparedExecution prepareExecution(DataBuildJobKey key, ExecutionAnchor anchor) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(anchor, "anchor");
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("DAG execution belongs to another durable job");
        }
        return inTransaction("prepare authorized durable stage graph", connection -> {
            var authority = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    DataBuildExecutionOwner.from(anchor));
            JdbcDataBuildStageExchangeStore.lockValidatedBarriers(connection, authority);
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            return new PreparedExecution(authority, job,
                    JdbcDataBuildStageExchangeStore.loadValidatedEvidence(connection, authority));
        });
    }

    /** Exact persisted coordinates and actual backend-issued execution identity; control term is not a page fence. */
    public record PartitionClaim(DataBuildJobKey job, ExecutionAnchor anchor, int stageId, int partitionId,
            int pageOrdinal, long controlTerm, Duration leaseDuration, Instant now) {
        public PartitionClaim {
            Objects.requireNonNull(job, "job");
            Objects.requireNonNull(anchor, "anchor");
            new JdbcDataBuildPageSelection.Coordinates(stageId, partitionId, pageOrdinal);
            if (!job.executionId().equals(anchor.executionId())) {
                throw new IllegalArgumentException("Partition claim belongs to another durable execution");
            }
            requireLeaseRequest(job.tenantId(), job.jobId(), anchor.ownerId(), controlTerm, leaseDuration, now);
        }
    }

    /** Ephemeral current authority and immutable input metadata; no user/session impersonation or data rows. */
    public record PreparedPartition(CurrentDeferredExecutionContext authority, DataBuildJob job,
            DataBuildLease lease, Map<PublicationRead, PublishedInput> publications,
            List<DataBuildOutput> dependencyOutputs,
            List<JdbcDataBuildStageExchangeStore.ExchangeEvidence> exchanges) {
        public PreparedPartition {
            publications = Map.copyOf(publications);
            dependencyOutputs = List.copyOf(dependencyOutputs);
            exchanges = List.copyOf(exchanges);
        }
    }

    /** Original validated Native producer report and its dispatched attempt, not a fabricated artifact report. */
    public record ProducerCompletion(DataBuildJobKey job, DataBuildExecutionOwner executionOwner,
            int stageId, int partitionId, long planEpoch, int attemptId, long fencingToken, long controlTerm,
            byte[] originalWireBytes, Instant completedAt) {
        public ProducerCompletion {
            Objects.requireNonNull(job, "job");
            Objects.requireNonNull(executionOwner, "executionOwner");
            Objects.requireNonNull(completedAt, "completedAt");
            originalWireBytes = originalWireBytes.clone();
            requireProducerCoordinates(stageId, partitionId, planEpoch, attemptId, fencingToken, controlTerm);
            if (originalWireBytes.length == 0 || originalWireBytes.length > 4 * 1024 * 1024) {
                throw new IllegalArgumentException(
                        "Native producer completion requires bounded original wire evidence");
            }
        }
        @Override public byte[] originalWireBytes() { return originalWireBytes.clone(); }
    }

    /** Commits the exact compute winner before the ordinary DAG observes a terminal task. */
    public boolean completeProducer(ProducerCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        return inTransaction("complete authorized durable Native producer", connection -> {
            var key = completion.job();
            var context = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    completion.executionOwner());
            JdbcDataBuildStageExchangeStore.lockValidatedBarriers(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(completion.stageId());
            if (stage.pageKind() != DataBuildPageKind.RELATIONAL_COMPUTE
                    || completion.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException(
                        "Native producer report does not belong to a persisted compute stage");
            }
            var allocations = JdbcDataBuildStageExchangeStore.loadValidatedEvidence(connection, context).stream()
                    .map(JdbcDataBuildStageExchangeStore.ExchangeEvidence::registration)
                    .filter(value -> value.producerStageId() == stage.stageId()).toList();
            if (allocations.size() != 1 || allocations.get(0).planEpoch() != completion.planEpoch()) {
                throw new IllegalStateException("Native producer winner differs from its original external allocation");
            }
            findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            var page = JdbcDataBuildPageSelection.findPartition(connection, key.tenantId(), key.jobId(),
                    completion.stageId(), completion.partitionId()).orElseThrow();
            if (!matchesIssuedProducer(page, completion)) return false;
            byte[] bytes = completion.originalWireBytes();
            var report = new DataBuildPageCompletion(key.tenantId(), key.jobId(), page.pageId(),
                    completion.fencingToken(),
                    completion.controlTerm(), bytes, org.pangrova.common.util.DigestUtils.sha256Hex(bytes), List.of(),
                    completion.completedAt(), completion.executionOwner());
            requireCompletion(report);
            return JdbcDataBuildCompletion.complete(transactionAuthority, executionFence, connection, report);
        });
    }

    private static void requireProducerCoordinates(int stage, int partition, long epoch,
            int attempt, long fence, long term) {
        if (stage < 0 || partition < 0 || epoch <= 0 || attempt < 0 || fence <= 0 || term <= 0) {
            throw new IllegalArgumentException("Native producer completion requires its exact issued attempt");
        }
    }

    private static boolean matchesIssuedProducer(JdbcDataBuildStoreMappings.PageRow page,
            ProducerCompletion completion) {
        return page.attemptId() == completion.attemptId() && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.controlTerm()
                && completion.executionOwner().equals(page.executionOwner());
    }

    /** Immutable publication evidence already read on the claim connection, not a second pool borrow. */
    public record PublishedInput(DataBuildGenerationManifest manifest, List<DataBuildOutput> outputs,
            Set<String> sourceObjectIds) {
        public PublishedInput {
            outputs = List.copyOf(outputs);
            sourceObjectIds = Set.copyOf(sourceObjectIds);
        }
    }

    /**
     * Prepares local immutable control metadata before the shared authority and owner locks are released.
     * The callback must not perform network I/O. A callback failure rolls back the page claim.
     */
    public <T> Optional<T> preparePartition(PartitionClaim request, Function<PreparedPartition, T> prepare) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(prepare, "prepare");
        return inTransaction("prepare authorized durable stage partition", connection -> {
            var key = request.job();
            var authority = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    DataBuildExecutionOwner.from(request.anchor()));
            JdbcDataBuildStageExchangeStore.lockValidatedBarriers(connection, authority);
            var stage = authority.dispatchSpec().stageGraph().requireStage(request.stageId());
            if (request.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException("Requested partition differs from its immutable stage cohort");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            if (terminal(job.state())) return Optional.empty();
            var coordinates = new JdbcDataBuildPageSelection.Coordinates(
                    request.stageId(), request.partitionId(), request.pageOrdinal());
            JdbcDataBuildPageSelection.requireCoordinates(connection, key.tenantId(), key.jobId(), coordinates);
            var selected = JdbcDataBuildPageSelection.selectClaimablePage(connection,
                    new JdbcDataBuildPageSelection.Selection(key.tenantId(), key.jobId(), request.now(),
                            Optional.of(coordinates)));
            if (selected.isEmpty()) return Optional.empty();
            var page = selected.orElseThrow();
            if (page.kind() != stage.pageKind()) {
                throw new IllegalStateException("Durable page kind differs from its graph");
            }
            var claimed = JdbcDataBuildPageClaim.claim(connection, page, request.anchor(), request.controlTerm(),
                    request.leaseDuration(), request.now());
            if (claimed.isEmpty()) return Optional.empty();
            var evidence = prepareEvidence(connection, authority, job, claimed.orElseThrow());
            requireConsumerEvidence(evidence);
            return Optional.of(Objects.requireNonNull(prepare.apply(evidence),
                    "partition preparation must return immutable metadata"));
        });
    }

    /** Revalidates an already issued exact lease and prepares local metadata without borrowing a second connection. */
    public <T> T prepareLeasedPartition(DataBuildLease lease, Instant now,
            Function<PreparedPartition, T> prepare) {
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(prepare, "prepare");
        return inTransaction("prepare current issued durable partition", connection -> {
            var authority = CurrentDeferredExecutionContext.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, lease.tenantId(), lease.jobId(),
                    DataBuildExecutionOwner.from(lease.executionAnchor()));
            JdbcDataBuildStageExchangeStore.lockValidatedBarriers(connection, authority);
            var job = findJob(connection, lease.tenantId(), lease.jobId(), true).orElseThrow();
            var page = JdbcDataBuildPageSelection.findPage(connection, lease.tenantId(), lease.jobId(),
                    lease.pageId(), true).orElseThrow();
            requireIssuedLease(page, lease, now);
            var actualSplits = loadSplits(connection, lease.tenantId(), lease.jobId(), lease.pageId());
            var actualDependencies = loadDependencies(connection, lease.tenantId(), lease.jobId(), lease.pageId());
            if (!actualSplits.equals(lease.splits()) || !actualDependencies.equals(lease.dependencyPageIds())) {
                throw new IllegalStateException("Issued durable partition inputs differ from immutable PG allocations");
            }
            var evidence = prepareEvidence(connection, authority, job, lease);
            requireConsumerEvidence(evidence);
            return Objects.requireNonNull(
                    prepare.apply(evidence), "partition preparation must return immutable metadata");
        });
    }

    private static void requireIssuedLease(JdbcDataBuildStoreMappings.PageRow page,
            DataBuildLease lease, Instant now) {
        boolean coordinates = page.stageId() == lease.stageId() && page.partitionId() == lease.partitionId()
                && page.pageOrdinal() == lease.pageOrdinal() && page.kind() == lease.kind();
        boolean owner = DataBuildExecutionOwner.from(lease.executionAnchor()).equals(page.executionOwner())
                && page.ownerEpoch() == lease.ownerEpoch() && page.fencingToken() == lease.fencingToken()
                && page.attemptId() == lease.attemptId();
        if (!coordinates || !owner || !"LEASED".equals(page.state())) {
            throw new IllegalStateException("Issued durable partition no longer has its exact current lease");
        }
        if (!now.isBefore(lease.expiresAt()) || page.leaseExpiresAt() == null
                || !now.isBefore(page.leaseExpiresAt()) || lease.expiresAt().isAfter(page.leaseExpiresAt())) {
            throw new IllegalStateException("Durable partition lease expired before dispatch");
        }
    }

    private static void requireConsumerEvidence(PreparedPartition evidence) {
        var graph = evidence.authority().dispatchSpec().stageGraph();
        for (var edge : graph.exchanges()) {
            if (edge.downstreamStageId() != evidence.lease().stageId()) continue;
            var matching = evidence.exchanges().stream().filter(value ->
                    value.registration().exchangeId().equals(edge.exchangeId())).toList();
            if (matching.size() != 1 || matching.get(0).finalization().isEmpty()) {
                throw new IllegalStateException("Durable consumer requires real persisted external finalize evidence");
            }
        }
    }

    private static PreparedPartition prepareEvidence(java.sql.Connection connection,
            CurrentDeferredExecutionContext authority, DataBuildJob job, DataBuildLease lease)
            throws java.sql.SQLException {
        var template = authority.dispatchSpec().stageGraph().requireStage(lease.stageId()).template();
        var publications = JdbcDataBuildPublishedInputs.load(connection, lease.tenantId(),
                template.publicationReads(), lease.splits());
        var dependencies = lease.kind() == DataBuildPageKind.DEPENDENCY_REDUCE
                || lease.kind() == DataBuildPageKind.GENERATION_FINALIZE
                ? JdbcDataBuildOutputQueries.loadDependencies(connection, lease.tenantId(), lease.jobId(),
                        lease.dependencyPageIds()) : List.<DataBuildOutput>of();
        return new PreparedPartition(authority, job, lease, publications, dependencies,
                JdbcDataBuildStageExchangeStore.loadValidatedEvidence(connection, authority));
    }
}
