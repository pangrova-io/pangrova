package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.coordinator.query.execution.dispatch.shuffle.generation.DurableStageShuffleEvidence;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;
import org.pangrova.coordinator.generation.template.PersistedStagePlan;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.execution.split.SplitPlacementWeight;
import org.pangrova.coordinator.scheduler.placement.WorkerPlacementWork;
import org.pangrova.scheduler.placement.TaskScheduler;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.generation.api.identity.GenerationJobKey;
import org.pangrova.generation.api.page.GenerationLease;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PreparedExecution;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PartitionWindow;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PreparedPartition;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.NativeWinner;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.InputRange;
import org.pangrova.persistence.generation.api.exchange.GenerationStageExchangeStore;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.adaptive.PartitionGroup;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.analysis.StageTransfer;
import org.pangrova.protocol.execution.identity.WorkerExecutionIdentity;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

/** Current durable control for the shared DAG and leaf engine; replay never compiles SQL or enumerates a connector. */
@Internal
public final class DurableStageRuntime {
    private final PreparedExecution execution;
    private final Dependencies dependencies;
    private final GenerationJobKey job;
    private final StageExecutionContext.Durable control;
    private final Map<Integer, Integer> physicalCohorts = new ConcurrentHashMap<>();

    /** Runtime dependencies do not become part of the immutable plan or persisted identity. */
    public record Dependencies(GenerationStageExecutionStore stages, WorkerManager workers,
            TaskScheduler scheduler, LongSupplier controlTerm, Duration leaseDuration,
            BiConsumer<PreparedTask, CompletableFuture<DispatchResult>> dispatched,
            GenerationStageExchangeStore exchanges, ExecutionTraceContextFactory traceContexts,
            ToIntFunction<WorkerInfo> localPlacementLoad) {
        public Dependencies {
            java.util.Objects.requireNonNull(traceContexts, "traceContexts");
            java.util.Objects.requireNonNull(localPlacementLoad, "localPlacementLoad");
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
    public record PreparedTask(GenerationLease lease, WorkerInfo worker, SubmitTaskRequest request)
            implements WindowPartition {}

    public sealed interface WindowPartition permits PreparedTask, RestoredTask {}

    /** An original Native terminal report, restored without reissuing its attempt. */
    public record RestoredTask(TaskReport report) implements WindowPartition {}

    /** Uses only the current authority returned by the same transactional graph preparation port. */
    public DurableStageRuntime(PreparedExecution execution, Dependencies dependencies) {
        this.execution = java.util.Objects.requireNonNull(execution, "execution");
        this.dependencies = java.util.Objects.requireNonNull(dependencies, "dependencies");
        var spec = execution.authority().dispatchSpec();
        job = new GenerationJobKey(spec.tenantId(), spec.jobId());
        if (!execution.job().tenantId().equals(job.tenantId()) || !execution.job().jobId().equals(job.jobId())
                || spec.stageGraph().purpose() != GenerationGraphPurpose.RELATIONAL_EXPORT) {
            throw new IllegalArgumentException("Durable stage runtime differs from its actual relational job");
        }
        var epochs = new HashMap<Integer, Long>();
        execution.exchanges().forEach(value -> epochs.put(value.registration().producerStageId(),
                value.registration().planEpoch()));
        control = new StageExecutionContext.Durable(execution.authority(), epochs,
                dependencies.traceContexts().capture());
    }

    /** Immutable authority and allocation epochs owning this exact replay, not a synthetic query session. */
    public StageExecutionContext.Durable control() {
        return control;
    }

    /** Hooks real register/finalize RPCs into the existing current-authority metadata barrier. */
    public DurableStageShuffleEvidence shuffleEvidence() {
        return new DurableStageShuffleEvidence(dependencies.exchanges(),
                execution.authority().dispatchSpec(), execution.job().sourceSnapshot(),
                () -> org.pangrova.generation.api.execution.GenerationExecutionOwner.from(
                        execution.authority().executionAnchor()),
                stage -> physicalCohorts.getOrDefault(stage,
                        execution.authority().dispatchSpec().stageGraph().requireStage(stage).parallelism()));
    }

    /** No live SQL, time-travel override, write declaration or discovery intent is admitted during replay. */
    public QueryExecutionScope queryContext() {
        return new QueryExecutionScope(control, Map.of(), null, List.of(), null, List.of(),
                Optional.empty(), Optional.empty());
    }

    /** Installs the committed ranges before the stage's outgoing allocation or Worker claims. */
    public CoalesceResult freezeInputRanges(Stage stage, StageDAG dag, CoalesceResult proposal) {
        var frozen = execution.authority().dispatchSpec().stageGraph().requireStage(stage.stageId());
        var output = dag.findDownstreamEdge(stage.stageId());
        if (!frozen.coalescibleHashInputs() || output == null || output.shuffleType() == ShuffleType.BROADCAST) {
            return CoalesceResult.NO_OP;
        }
        var requested = proposal.groups().stream().map(group ->
                new InputRange(group.startInclusive(), group.endExclusive())).toList();
        var ranges = dependencies.stages().freezeInputRanges(job, execution.authority().executionAnchor(),
                stage.stageId(), requested);
        if (ranges.isEmpty()) return CoalesceResult.NO_OP;
        physicalCohorts.put(stage.stageId(), ranges.size());
        if (ranges.equals(requested)) return proposal;
        var receipts = dag.incomingEdges(stage.stageId()).stream()
                .map(edge -> dag.findStage(edge.upstreamStageId()).requireShuffleReceipt()).toList();
        var groups = new ArrayList<PartitionGroup>(ranges.size());
        for (var range : ranges) {
            long bytes = 0;
            for (var receipt : receipts) {
                for (int partition = range.startInclusive(); partition < range.endExclusive(); partition++) {
                    bytes += Math.min(Long.MAX_VALUE - bytes, receipt.partitions().get(partition).bytes());
                }
            }
            groups.add(new PartitionGroup(range.startInclusive(), range.endExclusive(), bytes));
        }
        return new CoalesceResult(groups, frozen.parallelism(), groups.size());
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
    public DurablePartitionRequest.StageRequestPlan prepareRequestPlan(StageNode node) {
        var graph = execution.authority().dispatchSpec().stageGraph();
        if (!(node.stagePlan() instanceof PersistedStagePlan persisted)
                || !persisted.stage().equals(graph.requireStage(node.stageId()))) {
            throw new IllegalStateException("Runtime stage differs from its immutable graph envelope");
        }
        return new DurablePartitionRequest.StageRequestPlan(persisted.stage(), graph.modelSnapshots());
    }

    @FunctionalInterface
    public interface StageWindows {
        List<WindowPartition> prepare(int firstPartition, int count, Instant now);
    }

    /** Places a batch once and restores original winners on the same authority transaction. */
    public StageWindows bindStageWindows(int stage, DurablePartitionRequest.StageRequestPlan plan,
            IntFunction<DurablePartitionRequest.ExchangeAccess> input) {
        var placement = dependencies.scheduler().bindTenant(job.tenantId());
        return (first, count, now) -> dependencies.stages().<WindowPartition>prepareWindow(
                new PartitionWindow(job, execution.authority().executionAnchor(), stage, first, count,
                        dependencies.controlTerm().getAsLong(), dependencies.leaseDuration(), now),
                (partition, stored) -> new RestoredTask(winner(stage, partition, stored)),
                taskPreparation(plan, input, now, placement));
    }

    private Function<PreparedPartition, WindowPartition> taskPreparation(DurablePartitionRequest.StageRequestPlan plan,
            IntFunction<DurablePartitionRequest.ExchangeAccess> input, Instant now, TaskScheduler placement) {
        long memory = execution.authority().dispatchSpec().maxMemoryBytes();
        var eligible = placement.eligibleWorkersForTenant(memory, dependencies.workers().getActiveWorkers(),
                job.tenantId());
        Map<WorkerExecutionIdentity, Double> windowWork = new HashMap<>();
        return evidence -> {
            var request = DurablePartitionRequest.prepare(evidence, plan,
                    input.apply(evidence.lease().partitionId()), now, control.traceContext());
            var task = request.taskRequest();
            if (task.getResourceGrant().getNativeMemoryBytes() != memory) {
                throw new IllegalStateException("Durable partition resource grant differs from its bound stage");
            }
            long work = placementWork(task);
            var leastLoaded = WorkerPlacementWork.leastLoaded(eligible, task.getOperatorBindingsList(),
                    work, windowWork, dependencies.localPlacementLoad());
            var worker = placement.selectWorker(memory, leastLoaded).orElseThrow(() -> new IllegalStateException(
                    "No actual Worker satisfies the frozen stage capabilities and admitted resource grant"));
            windowWork.merge(worker.workerExecutionIdentity(), (double) work, Double::sum);
            return new PreparedTask(evidence.lease(), worker, request);
        };
    }

    private static long placementWork(TaskRequest task) {
        long work = 0;
        for (var split : task.getSplitsList()) {
            work += Math.min(Long.MAX_VALUE - work, SplitPlacementWeight.forBoundSplit(split));
        }
        for (var input : task.getExchangeInputsList()) {
            for (var partition : input.getExternal().getPublicationsList()) {
                work += Math.min(Long.MAX_VALUE - work, Math.max(0L, partition.getTotalBytes()));
            }
        }
        return Math.max(1L, work);
    }

    /** Registers lease renewal and the HTTP acknowledgement only after the exact claim has committed. */
    public void dispatched(PreparedTask task, CompletableFuture<DispatchResult> acknowledgement) {
        dependencies.dispatched().accept(task, acknowledgement);
    }

    private TaskReport winner(int stage, int partition, NativeWinner stored) {
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
