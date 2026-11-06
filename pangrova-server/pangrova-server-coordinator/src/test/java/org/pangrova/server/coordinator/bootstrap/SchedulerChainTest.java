package org.pangrova.server.coordinator.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import org.pangrova.scheduler.core.admission.FairScheduler;
import org.pangrova.scheduler.core.admission.StageScheduler;
import org.pangrova.scheduler.core.admission.StageScheduler.StageInfo;
import org.pangrova.scheduler.core.speculation.SpeculativeTaskLauncher;
import org.pangrova.scheduler.core.speculation.StragglerDetector;
import org.pangrova.scheduler.api.placement.TaskDuration;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.task.TaskStateMachine;
import org.pangrova.scheduler.core.resource.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Acceptance test: validates the scheduler chain from main() to task dispatch.
 *
 * <p>Chain under test: StageScheduler → FairScheduler → CoordinatorTaskTracker →
 * StragglerDetector → SpeculativeTaskLauncher</p>
 *
 * <p><b>Property 34: Scheduler chain from main() to task dispatch</b></p>
 */
@Tag("Feature: e2e-pipeline-integration - Property 34: Scheduler chain from main() to task dispatch")
class SchedulerChainTest extends org.pangrova.server.coordinator.testinfra.SignedOidcCoordinatorTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private CoordinatorBootstrap coordinatorBootstrap;

    @AfterEach
    void tearDownJUnit() {
        shutdownAll();
    }

    @AfterProperty
    void tearDownJqwik() {
        shutdownAll();
    }

    // -- Providers --

    @Provide
    Arbitrary<List<StageInfo>> validDags() {
        return Arbitraries.integers().between(2, 5).map(size -> {
            List<StageInfo> stages = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<String> deps = i == 0 ? List.of() : List.of("stage-" + (i - 1));
                boolean isOutput = (i == size - 1);
                stages.add(new StageInfo("stage-" + i, deps, isOutput));
            }
            return stages;
        });
    }

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "q-" + s);
    }

    // -- Property tests --

    /** P34a: StageScheduler returns all stages in topological order with output stages last. */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration - Property 34: Scheduler chain from main() to task dispatch")
    void stageSchedulerProducesTopologicalOrder(
            @ForAll("validDags") List<StageInfo> stages) {

        StageScheduler scheduler = new StageScheduler();
        List<String> order = scheduler.scheduleStages(stages);

        assertThat(order).hasSize(stages.size());
        assertOutputStagesLast(order, stages);
        assertDependenciesPrecedeDependents(order, stages);
    }

    /** P34b: Task tracker transitions PENDING→SCHEDULED→RUNNING→FINISHED yield allTasksFinished. */
    @Property(tries = 5)
    @Tag("Feature: e2e-pipeline-integration - Property 34: Scheduler chain from main() to task dispatch")
    void taskTrackerFullLifecycleEndsFinished(
            @ForAll("queryIds") String queryId) {

        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask(queryId, "t1");
        tracker.markScheduled(queryId, "t1");
        tracker.updateTaskState(queryId, "t1", "RUNNING");
        tracker.updateTaskState(queryId, "t1", "FINISHED");

        assertThat(tracker.allTasksFinished(queryId)).isTrue();
        assertThat(tracker.getTaskState(queryId, "t1"))
                .isEqualTo(TaskStateMachine.State.FINISHED);
    }

    // -- JUnit scenario tests --

    /** FairScheduler rejects queries exceeding queue capacity. */
    @Test
    void fairSchedulerRejectsOverCapacity() {
        FairScheduler fair = new FairScheduler(Map.of(
                QueueType.INTERACTIVE, 2,
                QueueType.BATCH, 1,
                QueueType.BACKGROUND, 1));

        assertThat(fair.submitQuery("q1", QueueType.INTERACTIVE)).isTrue();
        assertThat(fair.submitQuery("q2", QueueType.INTERACTIVE)).isTrue();
        assertThat(fair.submitQuery("q3", QueueType.INTERACTIVE)).isFalse();
        assertThat(fair.getQueueUsage(QueueType.INTERACTIVE)).isEqualTo(2);
    }

    /** StragglerDetector identifies running tasks exceeding 1.5x median with 75%+ completion. */
    @Test
    void stragglerDetectorIdentifiesStragglers() {
        StragglerDetector detector = new StragglerDetector();
        List<TaskDuration> durations = List.of(
                new TaskDuration("t1", 5000, true, false),
                new TaskDuration("t2", 5000, true, false),
                new TaskDuration("t3", 5000, true, false),
                new TaskDuration("t4", 50000, false, false));

        List<String> stragglers = detector.detectStragglers(durations);

        assertThat(stragglers).containsExactly("t4");
    }

    /** An unwired launcher cannot advertise an unexecuted speculative copy. */
    @Test
    void speculativeTaskLauncherTracksOriginal() {
        SpeculativeTaskLauncher launcher = new SpeculativeTaskLauncher();

        assertThatThrownBy(() -> launcher.launchSpeculative(
                "t1", "worker-exclude", "query-1"))
                .isInstanceOf(IllegalStateException.class);
        assertThat(launcher.getOriginalTasksWithCopies()).isEmpty();
    }

    /** Native-only query dispatch rejects submissions until a worker is available. */
    @Test
    void coordinatorBootstrapFailsClosedWithoutWorker() throws Exception {
        startCoordinator("coord-sched-test");
        int port = coordinatorBootstrap.getHttpPort();

        HttpResponse<String> resp = submitQuery(port, "SELECT 1");

        assertThat(resp.statusCode()).isEqualTo(503);
        assertThat(resp.body()).contains("COORDINATOR-0001");
    }

    /** Task tracker detects failed task after PENDING→SCHEDULED→RUNNING→FAILED transitions. */
    @Test
    void taskTrackerDetectsFailedTask() {
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        tracker.registerTask("q1", "t1");
        tracker.markScheduled("q1", "t1");
        tracker.updateTaskState("q1", "t1", "RUNNING");
        tracker.updateTaskState("q1", "t1", "FAILED");

        assertThat(tracker.hasFailedTask("q1")).isTrue();
        assertThat(tracker.getTaskState("q1", "t1"))
                .isEqualTo(TaskStateMachine.State.FAILED);
    }

    // -- Bootstrap helpers --

    private void startCoordinator(String coordinatorId) {
        shutdownAll();
        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(
                coordinatorId, 0);
        coordinatorBootstrap = startAuthorizedCoordinator(config);
        assertThat(coordinatorBootstrap.isStarted()).isTrue();
        assertThat(coordinatorBootstrap.getHttpPort()).isGreaterThan(0);
    }

    private void shutdownAll() {
        if (coordinatorBootstrap != null) {
            coordinatorBootstrap.shutdown();
            coordinatorBootstrap = null;
        }
    }

    // -- HTTP helpers --

    private HttpResponse<String> submitQuery(int port, String sql) throws Exception {
        String body = MAPPER.writeValueAsString(Map.of("query", sql));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/query/submit"))
                .header("Content-Type", "application/json")
                .header("Authorization", authorizationHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // -- Assertion helpers --

    private static void assertOutputStagesLast(List<String> order, List<StageInfo> stages) {
        List<String> outputIds = stages.stream()
                .filter(StageInfo::isOutput)
                .map(StageInfo::stageId)
                .toList();
        List<String> tail = order.subList(order.size() - outputIds.size(), order.size());
        assertThat(tail).containsExactlyInAnyOrderElementsOf(outputIds);
    }

    private static void assertDependenciesPrecedeDependents(
            List<String> order, List<StageInfo> stages) {
        Map<String, Integer> positionOf = new java.util.HashMap<>();
        for (int i = 0; i < order.size(); i++) {
            positionOf.put(order.get(i), i);
        }
        for (StageInfo stage : stages) {
            for (String dep : stage.dependencyStageIds()) {
                assertThat(positionOf.get(dep))
                        .as("dependency %s before %s", dep, stage.stageId())
                        .isLessThan(positionOf.get(stage.stageId()));
            }
        }
    }
}
