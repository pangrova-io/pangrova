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
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
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
import java.util.function.BiFunction;
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
            var graph = context.dispatchSpec().stageGraph();
            var stage = graph.requireStage(completion.stageId());
            if (stage.pageKind() != GenerationPageKind.RELATIONAL_COMPUTE) {
                throw new IllegalArgumentException(
                        "Native producer report does not belong to a persisted compute stage");
            }
            var outgoing = graph.exchanges().stream()
                    .filter(edge -> edge.upstreamStageId() == stage.stageId()).toList();
            if (outgoing.size() != 1 || PostgresGenerationStageExchangeStore.lockRegistration(
                    connection, context, outgoing.get(0).exchangeId())
                    .filter(allocation -> allocation.planEpoch() == completion.planEpoch()
                            && completion.partitionId() < allocation.producerCount()).isEmpty()) {
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
            var stage = authority.dispatchSpec().stageGraph().requireStage(first.stageId());
            var exchanges = PostgresGenerationStageExchangeStore.lockAndLoadStageEvidence(
                    connection, authority, stage.stageId());
            if (cohort.get(cohort.size() - 1).partitionId() >= Math.max(stage.parallelism(),
                    producerCount(stage, exchanges))) {
                throw new IllegalArgumentException("Requested partition differs from its immutable stage cohort");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            if (terminal(job.state())) return List.of();

            var pages = PostgresGenerationPageSelection.selectClaimableCohort(
                    connection, cohort, stage.pageKind().isRelational());
            if (pages.isEmpty()) return List.of();
            return preparePages(connection, authority, job, stage, exchanges, pages, first, prepare);
        });
    }

    @Override
    public List<InputRange> freezeInputRanges(GenerationJobKey key, ExecutionAnchor anchor, int stageId,
            List<InputRange> proposed) {
        if (!key.executionId().equals(anchor.executionId())) {
            throw new IllegalArgumentException("Durable layout belongs to another execution");
        }
        var ranges = List.copyOf(proposed);
        return inTransaction("freeze durable Shuffle input ranges", connection -> {
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(anchor));
            return PostgresGenerationInputRanges.freeze(connection, authority, key, stageId, ranges);
        });
    }

    @Override
    public <T> List<T> prepareWindow(PartitionWindow window, BiFunction<Integer, NativeWinner, T> restore,
            Function<PreparedPartition, T> prepare) {
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(restore, "restore");
        Objects.requireNonNull(prepare, "prepare");
        return inTransaction("prepare durable stage window", connection -> {
            var key = window.job();
            var authority = PostgresCurrentGenerationAuthority.resolve(
                    transactionAuthority, executionFence, connection, key.tenantId(), key.jobId(),
                    GenerationExecutionOwner.from(window.anchor()));
            var stage = authority.dispatchSpec().stageGraph().requireStage(window.stageId());
            if (!stage.pageKind().isRelational()) {
                throw new IllegalArgumentException("Requested window differs from its relational stage cohort");
            }
            var exchanges = PostgresGenerationStageExchangeStore.lockAndLoadStageEvidence(
                    connection, authority, stage.stageId());
            if (window.count() > producerCount(stage, exchanges) - window.firstPartition()) {
                throw new IllegalArgumentException("Requested window differs from its physical stage cohort");
            }
            var job = findJob(connection, key.tenantId(), key.jobId(), true).orElseThrow();
            var rows = PostgresGenerationPageSelection.lockWindow(connection, key.tenantId(), key.jobId(),
                    stage.stageId(), stage.pageKind(), window.firstPartition(), window.count());
            var pending = rows.stream().filter(page -> !"SUCCEEDED".equals(page.state())).toList();
            if (!pending.isEmpty() && (terminal(job.state())
                    || pending.stream().anyMatch(page -> !claimable(page, window.now())))) {
                return List.of();
            }
            var winners = new ArrayList<T>(rows.size() - pending.size());
            for (var row : rows) {
                if ("SUCCEEDED".equals(row.state())) {
                    winners.add(Objects.requireNonNull(
                            restore.apply(row.partitionId(), PostgresGenerationPageSelection.nativeWinner(row)),
                            "partition restoration must return immutable metadata"));
                }
            }
            var fresh = List.<T>of();
            if (!pending.isEmpty()) {
                var first = pending.get(0);
                var claim = new PartitionClaim(key, window.anchor(), window.stageId(), first.partitionId(),
                        first.pageOrdinal(), window.controlTerm(), window.leaseDuration(), window.now());
                fresh = preparePages(connection, authority, job, stage, exchanges, pending, claim, prepare);
                if (fresh.size() != pending.size()) return List.of();
            }
            var result = new ArrayList<T>(rows.size());
            var issued = fresh.iterator();
            var restored = winners.iterator();
            for (var row : rows) {
                result.add("SUCCEEDED".equals(row.state()) ? restored.next() : issued.next());
            }
            return List.copyOf(result);
        });
    }

    private static int producerCount(StagePlan stage, List<ExchangeEvidence> exchanges) {
        return exchanges.stream().map(ExchangeEvidence::registration)
                .filter(registration -> registration.producerStageId() == stage.stageId())
                .findFirst().map(registration -> registration.producerCount()).orElse(stage.parallelism());
    }

    private static boolean claimable(PageRow page, Instant now) {
        return switch (page.state()) {
            case "PENDING" -> !page.nextAvailableAt().isAfter(now);
            case "LEASED" -> page.leaseExpiresAt() != null && page.leaseExpiresAt().isBefore(now);
            default -> false;
        };
    }

    private static <T> List<T> preparePages(java.sql.Connection connection,
            PostgresCurrentGenerationAuthority authority, GenerationJob job, StagePlan stage,
            List<ExchangeEvidence> exchanges, List<PageRow> pages, PartitionClaim claim,
            Function<PreparedPartition, T> prepare) throws java.sql.SQLException {
        var stageInputs = stageInputs(authority, stage.stageId(), exchanges);
        if (stageInputs == StageInputs.BLOCKED) return List.of();
        for (var page : pages) {
            if (page.kind() != stage.pageKind()) {
                throw new IllegalStateException("Durable page kind differs from its graph");
            }
            if (PostgresGenerationPageClaim.attemptsExhausted(page)) {
                failExhaustedPage(connection, page, claim.now());
                return List.of();
            }
        }

        var key = claim.job();
        if (job.state() == GenerationJobState.QUEUED) {
            markJobRunning(connection, key.tenantId(), key.jobId(), claim.now());
        }
        var inputs = stage.pageKind().isRelational() && stageInputs == StageInputs.FINALIZED
                ? java.util.Collections.nCopies(pages.size(), new PageInputs(List.of(), List.of()))
                : INPUTS.loadPageInputs(connection, key.tenantId(), key.jobId(),
                        pages.stream().map(PageRow::pageId).toList(), stage.pageKind());
        var publications = PostgresGenerationPublishedInputs.load(connection, key.tenantId(),
                authority.executionAnchor().executionId(), stage.template().publicationReads(),
                inputs.stream().map(PageInputs::splits).toList());
        var result = new ArrayList<T>(pages.size());
        for (int i = 0; i < pages.size(); i++) {
            var lease = PostgresGenerationPageClaim.issue(connection, pages.get(i), inputs.get(i),
                    claim.anchor(), claim.controlTerm(), claim.leaseDuration(), claim.now());
            var evidence = prepareEvidence(connection, authority, job, lease, exchanges,
                    publications.get(i), pages.get(i));
            result.add(Objects.requireNonNull(prepare.apply(evidence),
                    "partition preparation must return immutable metadata"));
        }
        return List.copyOf(result);
    }

    private static PartitionClaim validateCohort(List<PartitionClaim> cohort) {
        if (cohort.size() > PARTITION_WINDOW) {
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
            var exchanges = PostgresGenerationStageExchangeStore.lockAndLoadStageEvidence(
                    connection, authority, lease.stageId());
            var job = findJob(connection, lease.tenantId(), lease.jobId(), true).orElseThrow();
            var page = PostgresGenerationPageSelection.findPage(connection, lease.tenantId(), lease.jobId(),
                    lease.pageId(), true).orElseThrow();
            requireIssuedLease(page, lease, now);
            var stageInputs = stageInputs(authority, lease.stageId(), exchanges);
            if (stageInputs == StageInputs.BLOCKED) {
                throw new IllegalStateException("Durable consumer requires real persisted external finalize evidence");
            }
            var inputs = PostgresGenerationPageClaim.readInputs(connection, page,
                    !page.kind().isRelational() || stageInputs != StageInputs.FINALIZED);
            if (!inputs.splits().equals(lease.splits()) || !inputs.dependencies().equals(lease.dependencyPageIds())) {
                throw new IllegalStateException("Issued durable partition inputs differ from immutable PG allocations");
            }
            var template = authority.dispatchSpec().stageGraph().requireStage(lease.stageId()).template();
            var publications = PostgresGenerationPublishedInputs.load(connection, lease.tenantId(),
                    authority.executionAnchor().executionId(), template.publicationReads(), List.of(inputs.splits()));
            var evidence = prepareEvidence(connection, authority, job, lease, exchanges, publications.get(0), page);
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

    private enum StageInputs { SOURCE, FINALIZED, BLOCKED }

    private static StageInputs stageInputs(PostgresCurrentGenerationAuthority authority, int stage,
            List<ExchangeEvidence> exchanges) {
        var graph = authority.dispatchSpec().stageGraph();
        boolean consumesExchange = false;
        for (var edge : graph.exchanges()) {
            if (edge.downstreamStageId() != stage) continue;
            consumesExchange = true;
            var matching = exchanges.stream().filter(value ->
                    value.registration().exchangeId().equals(edge.exchangeId())).toList();
            if (matching.size() != 1 || matching.get(0).finalization().isEmpty()) {
                return StageInputs.BLOCKED;
            }
        }
        return consumesExchange ? StageInputs.FINALIZED : StageInputs.SOURCE;
    }

    private static PreparedPartition prepareEvidence(java.sql.Connection connection,
            PostgresCurrentGenerationAuthority authority, GenerationJob job, GenerationLease lease,
            List<ExchangeEvidence> exchanges, Map<PublicationRead, PublishedInput> publications, PageRow page)
            throws java.sql.SQLException {
        var dependencies = lease.kind() == GenerationPageKind.DEPENDENCY_REDUCE
                || lease.kind() == GenerationPageKind.GENERATION_FINALIZE
                ? PostgresGenerationOutputQueries.loadDependencies(connection, lease.tenantId(), lease.jobId(),
                        lease.dependencyPageIds()) : List.<GenerationOutput>of();
        var range = page.inputRange();
        if (range == null) {
            range = new InputRange(lease.partitionId(), Math.incrementExact(lease.partitionId()));
        }
        return new PreparedPartition(authority, job, lease, publications, dependencies, exchanges, range);
    }
}
