package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.split.QuerySplit;
import org.pangrova.coordinator.query.execution.split.StageSplitSource;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.task.QueryOperatorStatsRegistry;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SourceFreeQueryFixture;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.execution.stage.QueryExecutionScope;
import org.pangrova.planner.analysis.CompiledStagePlan;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.profiling.OperatorStatsSource.StageDispatchWork;
import org.pangrova.query.profiling.enums.DispatchPhase;
import org.pangrova.scheduler.api.exception.NoAvailableWorkerException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Observes actual live engine dispatch failures before any unavailable Native Worker is needed. */
class DAGDispatchPhaseObservationTest {

    private static final StageSplitSource EMPTY = (fragment, sources, overrides, session, granularity) -> List.of();

    @Test
    void serializationFailureRetainsItsMeasuredPhaseWithoutInventingLaterPhases() {
        var failure = new NoClassDefFoundError("fixture serializer linkage");
        SqlCompiler compiler = new SqlCompiler() {
            @Override
            public CompiledStagePlan compileStagePlan(RelNode relation, List<String> names) {
                throw failure;
            }
        };
        var work = failDispatch(compiler, EMPTY, "fixture serializer linkage");
        assertPhases(work, DispatchPhase.SERIALIZE_PLAN);
    }

    @Test
    void connectorEnumerationTimeoutRetainsItsMeasuredFailedPhase() {
        var work = failDispatch(new SqlCompiler(), (fragment, sources, overrides, session, granularity) -> {
            throw new PangrovaException(PangrovaErrorCode.CONNECTOR_SPLIT_FETCH_TIMEOUT, "fixture enumeration timeout");
        }, "fixture enumeration timeout");
        assertPhases(work, DispatchPhase.SERIALIZE_PLAN, DispatchPhase.ENUMERATE_SPLITS);
    }

    @Test
    void invalidCanonicalSplitRetainsEnumerationCountAndFailedBindingPhase() {
        var work = failDispatch(new SqlCompiler(), (fragment, sources, overrides, session, granularity) ->
                List.of(new QuerySplit(DataSplit.getDefaultInstance(), List.of())), "Invalid canonical split identity");
        assertPhases(work, DispatchPhase.SERIALIZE_PLAN, DispatchPhase.ENUMERATE_SPLITS,
                DispatchPhase.VALIDATE_AND_BIND_PLAN);
        assertThat(work.get(1).itemCount()).isEqualTo(1);
    }

    @Test
    void workerAdmissionRejectionRetainsFailedSubmissionWithoutAcknowledgement() {
        var work = failDispatch(new SqlCompiler(), EMPTY, "fixture worker admission rejected");
        assertPhases(work, DispatchPhase.SERIALIZE_PLAN, DispatchPhase.ENUMERATE_SPLITS,
                DispatchPhase.VALIDATE_AND_BIND_PLAN, DispatchPhase.SUBMIT_TASKS);
        assertThat(work.get(1).itemCount()).isZero();
    }

    private static List<StageDispatchWork> failDispatch(SqlCompiler compiler, StageSplitSource source,
            String expectedMessage) {
        var session = SourceFreeQueryFixture.session("phase-observation");
        var dag = singleStage(session.queryId());
        var registry = new QueryOperatorStatsRegistry();
        var context = new DAGDispatchContext((request, worker) -> {
            throw new AssertionError("A failed preparation must not dispatch a task");
        }, new CoordinatorTaskTracker(), null, null, 30, null, null, null, PipelineConfig.DISABLED,
                null, null, null);
        WorkerSelector rejectAdmission = new WorkerSelector() {
            @Override
            public WorkerInfo select(String tenantId) {
                throw new NoAvailableWorkerException("fixture worker admission rejected");
            }

            @Override
            public StageResourceGrant computeResourceLimit(int parallelism, int concurrentStages, WorkerInfo worker) {
                throw new AssertionError("A rejected Worker cannot receive a grant");
            }
        };

        try (var engine = new DAGDispatchEngine(context, List.of(), source, null, null)) {
            engine.setOperatorStatsRegistry(registry);
            assertThatThrownBy(() -> engine.start(session, dag, compiler, rejectAdmission, 5000,
                    QueryExecutionScope.readOnly(session), ignored -> { }).toCompletableFuture().join())
                    .hasStackTraceContaining(expectedMessage);
            assertThat(dag.findStage(1).stateMachine().getState()).isEqualTo(StageState.FAILED);
            return registry.executionGraph(session.queryId()).stageDispatchWork();
        }
    }

    private static void assertPhases(List<StageDispatchWork> work, DispatchPhase... phases) {
        assertThat(work).extracting(StageDispatchWork::phase).containsExactly(phases);
        for (int i = 0; i < work.size(); i++) {
            var event = work.get(i);
            assertThat(event.stageId()).isEqualTo(1);
            assertThat(event.generation()).isZero();
            assertThat(event.startedNanos()).isNotNegative();
            assertThat(event.finishedNanos()).isGreaterThanOrEqualTo(event.startedNanos());
            if (i > 0) assertThat(event.startedNanos()).isGreaterThanOrEqualTo(work.get(i - 1).finishedNanos());
            assertThat(event.successful()).isEqualTo(i != work.size() - 1);
        }
        assertThat(work.get(work.size() - 1).itemCount()).isNull();
    }

    private static StageDAG singleStage(String queryId) {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var fragment = new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
        var node = new StageNode(1, fragment, 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(List.of(node), Map.of(), List.of()).withStages(queryId);
    }
}
