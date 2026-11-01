package org.pangrova.generation.store.postgresql.job;

import org.pangrova.generation.spi.jdbc.GenerationTransactionAuthority;
import org.pangrova.generation.spi.storage.GenerationStageExecutionStore;
import org.pangrova.generation.spi.storage.GenerationStageExchangeStore.ExchangeEvidence;
import org.pangrova.persistence.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.generation.spi.jdbc.GenerationExecutionTransactionFence;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobKey;
import org.pangrova.generation.api.job.GenerationLease;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.job.GenerationOutput;
import org.pangrova.generation.api.job.GenerationPageCompletion;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.generation.store.postgresql.execution.PostgresCurrentGenerationAuthority;
import org.pangrova.generation.store.postgresql.exchange.PostgresGenerationStageExchangeStore;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.spi.query.coordination.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.function.Function;

/** Same-authority transaction boundary for preparing one exact partition in the shared stage engine. */
@Internal
public final class PostgresGenerationStageExecutionStore extends PostgresGenerationJobStoreSupport
        implements GenerationStageExecutionStore {
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;
    private final CurrentDirectRoleReader currentRoles;

    /** Resolves current authority, execution fencing and roles on each stage transaction. */
    public PostgresGenerationStageExecutionStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence, CurrentDirectRoleReader currentRoles) {
        super(dataSource);
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
        this.currentRoles = java.util.Objects.requireNonNull(currentRoles, "currentRoles");
    }

    /** Reads one partition's progress after current authority and actual owner validation on the same connection. */
    @Override
    public PartitionProgress inspectPartition(GenerationJobKey key, ExecutionAnchor anchor,
            int stageId, int partitionId) {
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("Partition inspection belongs to another durable execution");
        }
        return inTransaction("inspect current durable stage partition", connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(anchor));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(stageId);
            if (partitionId < 0 || partitionId >= stage.parallelism()) {
                throw new IllegalArgumentException("Inspected partition differs from its immutable stage cohort");
            }
            var row = PostgresGenerationPageSelection.findPartition(connection, key.tenantId(), key.jobId(),
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
    @Override
    public PreparedExecution prepareExecution(GenerationJobKey key, ExecutionAnchor anchor) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(anchor, "anchor");
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("DAG execution belongs to another durable job");
        }
        return inTransaction("prepare authorized durable stage graph", connection -> {
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(anchor));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, authority);
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            return new PreparedExecution(authority, job,
                    PostgresGenerationStageExchangeStore.loadValidatedEvidence(connection, authority));
        });
    }

    /** Commits the exact compute winner before the ordinary DAG observes a terminal task. */
    @Override
    public boolean completeProducer(ProducerCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        return inTransaction("complete authorized durable Native producer", connection -> {
            var key = completion.job();
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    completion.executionOwner());
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(completion.stageId());
            if (stage.pageKind() != GenerationPageKind.RELATIONAL_COMPUTE
                    || completion.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException(
                        "Native producer report does not belong to a persisted compute stage");
            }
            var allocations = PostgresGenerationStageExchangeStore.loadValidatedEvidence(connection, context).stream()
                    .map(ExchangeEvidence::registration)
                    .filter(value -> value.producerStageId() == stage.stageId()).toList();
            if (allocations.size() != 1 || allocations.get(0).planEpoch() != completion.planEpoch()) {
                throw new IllegalStateException("Native producer winner differs from its original external allocation");
            }
            findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            var page = PostgresGenerationPageSelection.findPartition(connection, key.tenantId(), key.jobId(),
                    completion.stageId(), completion.partitionId()).orElseThrow();
            if (!matchesIssuedProducer(page, completion)) return false;
            byte[] bytes = completion.originalWireBytes();
            var report = new GenerationPageCompletion(key.tenantId(), key.jobId(), page.pageId(),
                    completion.fencingToken(),
                    completion.controlTerm(), bytes, org.pangrova.common.util.DigestUtils.sha256Hex(bytes), List.of(),
                    completion.completedAt(), completion.executionOwner());
            requireCompletion(report);
            return PostgresGenerationCompletion.complete(transactionAuthority, executionFence, connection, report);
        });
    }

    private static boolean matchesIssuedProducer(PostgresGenerationJobStoreMappings.PageRow page,
            ProducerCompletion completion) {
        return page.attemptId() == completion.attemptId() && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.controlTerm()
                && completion.executionOwner().equals(page.executionOwner());
    }

    /**
     * Prepares local immutable control metadata before the shared authority and owner locks are released.
     * The callback must not perform network I/O. A callback failure rolls back the page claim.
     */
    @Override
    public <T> Optional<T> preparePartition(PartitionClaim request, Function<PreparedPartition, T> prepare) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(prepare, "prepare");
        return inTransaction("prepare authorized durable stage partition", connection -> {
            var key = request.job();
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(request.anchor()));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, authority);
            var stage = authority.dispatchSpec().stageGraph().requireStage(request.stageId());
            if (request.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException("Requested partition differs from its immutable stage cohort");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            if (terminal(job.state())) return Optional.empty();
            var coordinates = new PostgresGenerationPageSelection.Coordinates(
                    request.stageId(), request.partitionId(), request.pageOrdinal());
            PostgresGenerationPageSelection.requireCoordinates(connection, key.tenantId(), key.jobId(), coordinates);
            var selected = PostgresGenerationPageSelection.selectClaimablePage(connection,
                    new PostgresGenerationPageSelection.Selection(key.tenantId(), key.jobId(), request.now(),
                            Optional.of(coordinates)));
            if (selected.isEmpty()) return Optional.empty();
            var page = selected.orElseThrow();
            if (page.kind() != stage.pageKind()) {
                throw new IllegalStateException("Durable page kind differs from its graph");
            }
            var claimed = PostgresGenerationPageClaim.claim(connection, page, request.anchor(), request.controlTerm(),
                    request.leaseDuration(), request.now());
            if (claimed.isEmpty()) return Optional.empty();
            var evidence = prepareEvidence(connection, authority, job, claimed.orElseThrow());
            requireConsumerEvidence(evidence);
            return Optional.of(Objects.requireNonNull(prepare.apply(evidence),
                    "partition preparation must return immutable metadata"));
        });
    }

    /** Revalidates an already issued exact lease and prepares local metadata without borrowing a second connection. */
    @Override
    public <T> T prepareLeasedPartition(GenerationLease lease, Instant now,
            Function<PreparedPartition, T> prepare) {
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(prepare, "prepare");
        return inTransaction("prepare current issued durable partition", connection -> {
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, currentRoles, connection, lease.tenantId(), lease.jobId(),
                    GenerationExecutionOwner.from(lease.executionAnchor()));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, authority);
            var job = findJob(connection, lease.tenantId(), lease.jobId(), true).orElseThrow();
            var page = PostgresGenerationPageSelection.findPage(connection, lease.tenantId(), lease.jobId(),
                    lease.pageId(), true).orElseThrow();
            requireIssuedLease(page, lease, now);
            var actualSplits = INPUTS.loadSplits(connection, lease.tenantId(), lease.jobId(), lease.pageId());
            var actualDependencies = INPUTS.loadDependencies(
                    connection, lease.tenantId(), lease.jobId(), lease.pageId());
            if (!actualSplits.equals(lease.splits()) || !actualDependencies.equals(lease.dependencyPageIds())) {
                throw new IllegalStateException("Issued durable partition inputs differ from immutable PG allocations");
            }
            var evidence = prepareEvidence(connection, authority, job, lease);
            requireConsumerEvidence(evidence);
            return Objects.requireNonNull(
                    prepare.apply(evidence), "partition preparation must return immutable metadata");
        });
    }

    private static void requireIssuedLease(PostgresGenerationJobStoreMappings.PageRow page,
            GenerationLease lease, Instant now) {
        boolean coordinates = page.stageId() == lease.stageId() && page.partitionId() == lease.partitionId()
                && page.pageOrdinal() == lease.pageOrdinal() && page.kind() == lease.kind();
        boolean owner = GenerationExecutionOwner.from(lease.executionAnchor()).equals(page.executionOwner())
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
            PostgresCurrentGenerationAuthority authority, GenerationJob job, GenerationLease lease)
            throws java.sql.SQLException {
        var template = authority.dispatchSpec().stageGraph().requireStage(lease.stageId()).template();
        var publications = PostgresGenerationPublishedInputs.load(connection, lease.tenantId(),
                template.publicationReads(), lease.splits());
        var dependencies = lease.kind() == GenerationPageKind.DEPENDENCY_REDUCE
                || lease.kind() == GenerationPageKind.GENERATION_FINALIZE
                ? PostgresGenerationOutputQueries.loadDependencies(connection, lease.tenantId(), lease.jobId(),
                        lease.dependencyPageIds()) : List.<GenerationOutput>of();
        return new PreparedPartition(authority, job, lease, publications, dependencies,
                PostgresGenerationStageExchangeStore.loadValidatedEvidence(connection, authority));
    }
}
