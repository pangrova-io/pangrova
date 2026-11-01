package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.coordinator.generation.template.PersistedStagePlan;
import org.pangrova.coordinator.generation.template.FrozenStageBindings;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.spi.generation.job.GenerationJobKey;
import org.pangrova.spi.generation.job.GenerationLease;
import org.pangrova.spi.generation.execution.GenerationStageGraph;
import org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore;
import org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore.PreparedExecution;
import org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore.PartitionProgress;
import org.pangrova.persistence.api.generation.exchange.GenerationStageExchangeStore;
import org.pangrova.planner.ShuffleType;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.StageTransfer;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.common.api.annotation.Internal;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/** Current durable control for the shared DAG and leaf engine; replay never compiles SQL or enumerates a connector. */
@Internal
public final class DurableStageRuntime {
    private final PreparedExecution execution;
    private final Dependencies dependencies;
    private final GenerationJobKey job;
    private final StageExecutionContext.Durable control;

    /** Runtime dependencies do not become part of the immutable plan or persisted identity. */
    public record Dependencies(GenerationStageExecutionStore stages, WorkerManager workers,
            TaskScheduler scheduler, LongSupplier controlTerm, Duration leaseDuration,
            BiConsumer<PreparedTask, CompletableFuture<DispatchResult>> dispatched,
            GenerationStageExchangeStore exchanges) {
        public Dependencies {
            java.util.Objects.requireNonNull(stages, "stages");
            java.util.Objects.requireNonNull(workers, "workers");
            java.util.Objects.requireNonNull(scheduler, "scheduler");
            java.util.Objects.requireNonNull(controlTerm, "controlTerm");
            java.util.Objects.requireNonNull(dispatched, "dispatched");
            java.util.Objects.requireNonNull(exchanges, "exchanges");
            if (leaseDuration == null || leaseDuration.isNegative() || leaseDuration.isZero()) {
                throw new IllegalArgumentException("Actual partition lease duration must be positive");
            }
        }
    }

    /** The exact claim, local Worker snapshot and canonical request, returned only after the PG transaction commits. */
    public record PreparedTask(GenerationLease lease, WorkerInfo worker, SubmitTaskRequest request) {}

    /** Uses only the current authority returned by the same transactional graph preparation port. */
    public DurableStageRuntime(PreparedExecution execution, Dependencies dependencies) {
        this.execution = java.util.Objects.requireNonNull(execution, "execution");
        this.dependencies = java.util.Objects.requireNonNull(dependencies, "dependencies");
        var spec = execution.authority().dispatchSpec();
        job = new GenerationJobKey(spec.tenantId(), spec.jobId());
        if (!execution.job().tenantId().equals(job.tenantId()) || !execution.job().jobId().equals(job.jobId())
                || spec.stageGraph().purpose() != GenerationStageGraph.Purpose.RELATIONAL_EXPORT) {
            throw new IllegalArgumentException("Durable stage runtime differs from its actual relational job");
        }
        var epochs = new HashMap<Integer, Long>();
        execution.exchanges().forEach(value -> epochs.put(value.registration().producerStageId(),
                value.registration().planEpoch()));
        control = new StageExecutionContext.Durable(execution.authority(), epochs);
    }

    /** Immutable authority and allocation epochs owning this exact replay, not a synthetic query session. */
    public StageExecutionContext.Durable control() {
        return control;
    }

    /** Hooks real register/finalize RPCs into the existing current-authority metadata barrier. */
    public org.pangrova.coordinator.generation.DurableStageShuffleEvidence shuffleEvidence() {
        return new org.pangrova.coordinator.generation.DurableStageShuffleEvidence(dependencies.exchanges(),
                execution.authority().dispatchSpec(), execution.job().sourceSnapshot(),
                () -> org.pangrova.spi.generation.execution.GenerationExecutionOwner.from(
                        execution.authority().executionAnchor()));
    }

    /** No live SQL, time-travel override, write declaration or discovery intent is admitted during replay. */
    public DAGQueryContext queryContext() {
        return new DAGQueryContext(control, Map.of(), null, List.of(), null, List.of(),
                Optional.empty(), Optional.empty());
    }

    /** Creates ordinary Stage entities from the same persisted graph's exact parallelism and exchange shape. */
    public StageDAG dag() {
        var graph = execution.authority().dispatchSpec().stageGraph();
        validateExchangeContracts(graph);
        List<StageNode> nodes = graph.stages().stream().map(stage -> new StageNode(stage.stageId(),
                new PersistedStagePlan(stage), stage.parallelism(), stage.publicationOutput() ? "OUTPUT" : "COMPUTE",
                List.of())).toList();
        List<StageEdge> edges = graph.exchanges().stream().map(edge -> new StageEdge(edge.upstreamStageId(),
                edge.downstreamStageId(), ShuffleType.valueOf(edge.distribution().name()), edge.distributionKeys(),
                edge.exchangeId(), edge.partitionCount(),
                BatchSerde.valueOf(edge.batchSerde()), edge.requireComplete()))
                .toList();
        Map<Integer, List<Integer>> upstream = new HashMap<>();
        edges.forEach(edge -> upstream.computeIfAbsent(edge.downstreamStageId(), ignored -> new ArrayList<>())
                .add(edge.upstreamStageId()));
        upstream.replaceAll((stage, inputs) -> List.copyOf(inputs));
        return StageDAG.fromNodes(nodes, upstream, edges).withStages(control.executionId());
    }

    /** Opaque persisted bytes are inspected only at this execution boundary, never by the metadata store. */
    static void validateExchangeContracts(GenerationStageGraph graph) {
        if (graph.exchanges().isEmpty()) return;
        Map<Integer, SerializedPlanFragment> plans = new HashMap<>();
        graph.stages().forEach(stage -> plans.put(stage.stageId(),
                new SerializedPlanFragment(stage.planBytes(), List.of(), List.of())));
        var transfers = graph.exchanges().stream().map(edge -> new StageTransfer(edge.upstreamStageId(),
                edge.downstreamStageId(), ShuffleType.valueOf(edge.distribution().name()), edge.distributionKeys(),
                edge.exchangeId(), edge.partitionCount(), BatchSerde.valueOf(edge.batchSerde()),
                edge.requireComplete())).toList();
        try {
            AuthorizedStageGraph.validateExchangeContracts(plans, transfers);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "Persisted exchange wire contract is invalid; rebuild required", failure);
        }
    }

    /** Reads only frozen bytes and exact model contracts; does not resolve a new model version or publication. */
    public BoundExecutionPlan boundPlan(StageNode node) {
        var graph = execution.authority().dispatchSpec().stageGraph();
        if (!(node.stagePlan() instanceof PersistedStagePlan persisted)
                || !persisted.stage().equals(graph.requireStage(node.stageId()))) {
            throw new IllegalStateException("Runtime stage differs from its immutable graph envelope");
        }
        return FrozenStageBindings.read(persisted.stage(), graph.modelSnapshots());
    }

    /** Current progress is read by exact coordinates, without an unbounded source-page materialization. */
    public PartitionProgress inspect(int stage, int partition) {
        return dependencies.stages().inspectPartition(job, execution.authority().executionAnchor(), stage, partition);
    }

    /** Captures Worker/registry state before taking a PG connection; the claim callback below is purely local. */
    public Optional<PreparedTask> prepare(int stage, int partition, PartitionProgress progress,
            DurablePartitionRequest.ExchangeAccess exchange, Instant now) {
        List<WorkerInfo> snapshot = List.copyOf(dependencies.workers().getActiveWorkers());
        long term = dependencies.controlTerm().getAsLong();
        var claim = new GenerationStageExecutionStore.PartitionClaim(job, execution.authority().executionAnchor(),
                stage, partition, progress.pageOrdinal(), term, dependencies.leaseDuration(), now);
        return dependencies.stages().preparePartition(claim, evidence -> {
            var request = DurablePartitionRequest.prepare(evidence, exchange, now, control.traceContext());
            var bindings = request.taskRequest().getOperatorBindingsList();
            long memory = request.taskRequest().getResourceGrant().getNativeMemoryBytes();
            var selected = bindings.isEmpty() ? dependencies.scheduler().selectWorker(memory, snapshot)
                    : dependencies.scheduler().selectWorkerWithBindings(memory, snapshot, bindings);
            var worker = selected.orElseThrow(() -> new IllegalStateException(
                    "No actual Worker satisfies the frozen stage capabilities and admitted resource grant"));
            return new PreparedTask(evidence.lease(), worker, request);
        });
    }

    /** Registers lease renewal and the HTTP acknowledgement only after the exact claim has committed. */
    public void dispatched(PreparedTask task, CompletableFuture<DispatchResult> acknowledgement) {
        dependencies.dispatched().accept(task, acknowledgement);
    }

    /** Validates the stored whole TaskReport; an old format or changed winner is an explicit rebuild error. */
    public TaskReport winner(int stage, int partition, PartitionProgress progress) {
        var stored = progress.winner().orElseThrow();
        try {
            var report = TaskReport.parseFrom(stored.originalWireBytes());
            var attempt = report.getAttempt();
            boolean identity = attempt.getQueryOrJobId().equals(job.executionId())
                    && attempt.getStageId() == stage && attempt.getPartitionId() == partition
                    && exactIssuedAttempt(stored, attempt);
            if (!identity || attempt.getTaskId().isBlank()) {
                throw new IllegalStateException(
                        "Persisted Native winner differs from the actual issued attempt; rebuild required");
            }
            requireNativeCompletion(report);
            execution.exchanges().stream().map(value -> value.registration())
                    .filter(value -> value.producerStageId() == stage).forEach(value -> {
                        if (attempt.getPlanEpoch() != value.planEpoch()) {
                            throw new IllegalStateException(
                                    "Stored producer winner changed its original allocation epoch");
                        }
                    });
            return report;
        } catch (com.google.protobuf.InvalidProtocolBufferException malformed) {
            throw new IllegalStateException(
                    "Persisted Native winner is not the canonical whole report; rebuild required", malformed);
        }
    }

    private static void requireNativeCompletion(TaskReport report) {
        var coverage = report.getCoverage();
        if (report.getState() != TaskState.TASK_STATE_FINISHED || report.hasError()
                || report.getReportOrigin() != org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE
                || !completeCoverage(coverage)) {
            throw new IllegalStateException(
                    "Persisted winner is not complete original Native evidence; rebuild required");
        }
    }

    private static boolean completeCoverage(org.pangrova.protocol.v2.CoverageReport coverage) {
        return coverage.getCompletionState() == org.pangrova.protocol.v2.CompletionState.COMPLETION_STATE_COMPLETE
                && coverage.getExpectedWorkUnits() >= 0
                && coverage.getExpectedWorkUnits() == coverage.getCompletedWorkUnits()
                && coverage.getFailedWorkUnitIdsCount() == 0 && !coverage.getAllWorkUnitsFailed()
                && coverage.getCoverageRatio() == 1.0;
    }

    private static boolean exactIssuedAttempt(GenerationStageExecutionStore.NativeWinner stored,
            org.pangrova.protocol.v2.TaskAttemptId attempt) {
        var owner = attempt.getExecutionOwner();
        return attempt.getAttemptId() == stored.attemptId() && attempt.getFencingToken() == stored.fencingToken()
                && attempt.getControlEpoch() == stored.controlTerm()
                && owner.getOwnerId().equals(stored.executionOwner().ownerId())
                && owner.getOwnerIncarnation().equals(stored.executionOwner().ownerIncarnation())
                && owner.getFencingToken() == stored.executionOwner().fencingToken();
    }
}
