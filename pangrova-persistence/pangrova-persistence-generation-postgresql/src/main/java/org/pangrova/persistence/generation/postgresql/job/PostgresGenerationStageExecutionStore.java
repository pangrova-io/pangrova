package org.pangrova.persistence.generation.postgresql.job;

import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore.ExchangeEvidence;
import org.pangrova.persistence.jdbc.api.generation.GenerationExecutionTransactionFence;

import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.execution.GenerationExecutionOwner;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate.PublicationRead;
import org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority;
import org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore;
import org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries.PageInputs;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.execution.api.ownership.ExecutionAnchor;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Same-authority transaction boundary for preparing exact partitions in the shared stage engine. */
@Internal
public final class PostgresGenerationStageExecutionStore extends PostgresGenerationJobStoreSupport
        implements GenerationStageExecutionStore {
    private final GenerationTransactionAuthority transactionAuthority;
    private final GenerationExecutionTransactionFence executionFence;

    /** Resolves current authority, execution fencing and roles on each stage transaction. */
    public PostgresGenerationStageExecutionStore(
            DataSource dataSource, GenerationTransactionAuthority transactionAuthority,
            GenerationExecutionTransactionFence executionFence) {
        super(dataSource);
        this.transactionAuthority = java.util.Objects.requireNonNull(transactionAuthority, "transactionAuthority");
        this.executionFence = java.util.Objects.requireNonNull(executionFence, "executionFence");
    }

    /** Checks the current authority and actual owner once per bounded window of exact partitions. */
    @Override
    public List<PartitionProgress> inspectPartitions(GenerationJobKey key, ExecutionAnchor anchor,
            int stageId, int firstPartition, int count) {
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("Partition inspection belongs to another durable execution");
        }
        if (firstPartition < 0 || count < 1 || count > PARTITION_INSPECTION_WINDOW) {
            throw new IllegalArgumentException("Invalid partition inspection window");
        }
        return inTransaction("inspect current durable stage partitions", connection -> {
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(anchor));
            PostgresGenerationStageExchangeStore.lockValidatedBarriers(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(stageId);
            if (count > stage.parallelism() - firstPartition) {
                throw new IllegalArgumentException("Inspected partitions differ from their immutable stage cohort");
            }
            return PostgresGenerationPageSelection.inspectWindow(connection, key.tenantId(), key.jobId(),
                    stageId, stage.pageKind(), firstPartition, count);
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
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(anchor));
            var evidence = PostgresGenerationStageExchangeStore.lockAndLoadValidatedEvidence(connection, authority);
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            return new PreparedExecution(authority, job, evidence);
        });
    }

    /** Commits the exact compute winner before the ordinary DAG observes a terminal task. */
    @Override
    public boolean completeProducer(ProducerCompletion completion) {
        Objects.requireNonNull(completion, "completion");
        return inTransaction("complete authorized durable Native producer", connection -> {
            var key = completion.job();
            var context = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    completion.executionOwner());
            var evidence = PostgresGenerationStageExchangeStore.lockAndLoadValidatedEvidence(connection, context);
            var stage = context.dispatchSpec().stageGraph().requireStage(completion.stageId());
            if (stage.pageKind() != GenerationPageKind.RELATIONAL_COMPUTE
                    || completion.partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException(
                        "Native producer report does not belong to a persisted compute stage");
            }
            var allocations = evidence.stream()
                    .map(ExchangeEvidence::registration)
                    .filter(value -> value.producerStageId() == stage.stageId()).toList();
            if (allocations.size() != 1 || allocations.get(0).planEpoch() != completion.planEpoch()) {
                throw new IllegalStateException("Native producer winner differs from its original external allocation");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            var page = PostgresGenerationPageSelection.findPartition(connection, key.tenantId(), key.jobId(),
                    completion.stageId(), completion.partitionId()).orElseThrow();
            if (!matchesIssuedProducer(page, completion)) return false;
            byte[] bytes = completion.originalWireBytes();
            var report = new GenerationPageCompletion(key.tenantId(), key.jobId(), page.pageId(),
                    completion.fencingToken(),
                    completion.controlTerm(), bytes, org.pangrova.common.util.DigestUtils.sha256Hex(bytes), List.of(),
                    completion.completedAt(), completion.executionOwner());
            requireCompletion(report);
            return PostgresGenerationCompletion.completeLockedProducer(
                    executionFence, connection, report, job, page);
        });
    }

    private static boolean matchesIssuedProducer(PostgresGenerationJobStoreMappings.PageRow page,
            ProducerCompletion completion) {
        return page.attemptId() == completion.attemptId() && page.fencingToken() == completion.fencingToken()
                && page.ownerEpoch() == completion.controlTerm()
                && completion.executionOwner().equals(page.executionOwner());
    }

    @Override
    public <T> List<T> preparePartitions(List<PartitionClaim> requests, Function<PreparedPartition, T> prepare) {
        var cohort = List.copyOf(requests);
        Objects.requireNonNull(prepare, "prepare");
        if (cohort.isEmpty()) return List.of();
        PartitionClaim first = validateCohort(cohort);
        return inTransaction("prepare authorized durable stage partition cohort", connection -> {
            var key = first.job();
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(first.anchor()));
            var exchanges = PostgresGenerationStageExchangeStore.lockAndLoadValidatedEvidence(
                    connection, authority);
            var stage = authority.dispatchSpec().stageGraph().requireStage(first.stageId());
            if (cohort.get(cohort.size() - 1).partitionId() >= stage.parallelism()) {
                throw new IllegalArgumentException("Requested partition differs from its immutable stage cohort");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            if (terminal(job.state())) return List.of();

            var pages = PostgresGenerationPageSelection.selectClaimableCohort(connection, cohort);
            if (pages.isEmpty()) return List.of();
            for (var page : pages) {
                if (page.kind() != stage.pageKind()) {
                    throw new IllegalStateException("Durable page kind differs from its graph");
                }
                if (PostgresGenerationPageClaim.attemptsExhausted(page)) {
                    failExhaustedPage(connection, page, first.now());
                    return List.of();
                }
            }

            requireConsumerEvidence(authority, first.stageId(), exchanges);
            if (job.state() == GenerationJobState.QUEUED) {
                markJobRunning(connection, key.tenantId(), key.jobId(), first.now());
            }
            var inputs = INPUTS.loadPageInputs(connection, key.tenantId(), key.jobId(),
                    pages.stream().map(PageRow::pageId).toList());
            var publications = PostgresGenerationPublishedInputs.load(connection, key.tenantId(),
                    authority.executionAnchor().executionId(), stage.template().publicationReads(),
                    inputs.stream().map(PageInputs::splits).toList());
            var result = new ArrayList<T>(pages.size());
            for (int i = 0; i < pages.size(); i++) {
                var lease = PostgresGenerationPageClaim.issue(connection, pages.get(i), inputs.get(i),
                        first.anchor(), first.controlTerm(), first.leaseDuration(), first.now());
                var evidence = prepareEvidence(connection, authority, job, lease, exchanges, publications.get(i));
                result.add(Objects.requireNonNull(prepare.apply(evidence),
                        "partition preparation must return immutable metadata"));
            }
            return List.copyOf(result);
        });
    }

    private static PartitionClaim validateCohort(List<PartitionClaim> cohort) {
        if (cohort.size() > PARTITION_INSPECTION_WINDOW) {
            throw new IllegalArgumentException("Durable partition claim cohort exceeds its window");
        }
        var first = cohort.get(0);
        int previousPartition = -1;
        for (var request : cohort) {
            if (!request.job().equals(first.job()) || !request.anchor().equals(first.anchor())
                    || request.stageId() != first.stageId() || request.controlTerm() != first.controlTerm()
                    || !request.leaseDuration().equals(first.leaseDuration()) || !request.now().equals(first.now())
                    || request.partitionId() <= previousPartition) {
                throw new IllegalArgumentException(
                        "Durable partition claim cohort must be ordered under one authority");
            }
            previousPartition = request.partitionId();
        }
        return first;
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
                    transactionAuthority, executionFence, connection, lease.tenantId(), lease.jobId(),
                    GenerationExecutionOwner.from(lease.executionAnchor()));
            var exchanges = PostgresGenerationStageExchangeStore.lockAndLoadValidatedEvidence(
                    connection, authority);
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
            requireConsumerEvidence(authority, lease.stageId(), exchanges);
            var template = authority.dispatchSpec().stageGraph().requireStage(lease.stageId()).template();
            var publications = PostgresGenerationPublishedInputs.load(connection, lease.tenantId(),
                    authority.executionAnchor().executionId(), template.publicationReads(), List.of(actualSplits));
            var evidence = prepareEvidence(connection, authority, job, lease, exchanges, publications.get(0));
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

    private static void requireConsumerEvidence(PostgresCurrentGenerationAuthority authority, int stage,
            List<ExchangeEvidence> exchanges) {
        var graph = authority.dispatchSpec().stageGraph();
        for (var edge : graph.exchanges()) {
            if (edge.downstreamStageId() != stage) continue;
            var matching = exchanges.stream().filter(value ->
                    value.registration().exchangeId().equals(edge.exchangeId())).toList();
            if (matching.size() != 1 || matching.get(0).finalization().isEmpty()) {
                throw new IllegalStateException("Durable consumer requires real persisted external finalize evidence");
            }
        }
    }

    private static PreparedPartition prepareEvidence(java.sql.Connection connection,
            PostgresCurrentGenerationAuthority authority, GenerationJob job, GenerationLease lease,
            List<ExchangeEvidence> exchanges, Map<PublicationRead, PublishedInput> publications)
            throws java.sql.SQLException {
        var dependencies = lease.kind() == GenerationPageKind.DEPENDENCY_REDUCE
                || lease.kind() == GenerationPageKind.GENERATION_FINALIZE
                ? PostgresGenerationOutputQueries.loadDependencies(connection, lease.tenantId(), lease.jobId(),
                        lease.dependencyPageIds()) : List.<GenerationOutput>of();
        return new PreparedPartition(authority, job, lease, publications, dependencies, exchanges);
    }
}
