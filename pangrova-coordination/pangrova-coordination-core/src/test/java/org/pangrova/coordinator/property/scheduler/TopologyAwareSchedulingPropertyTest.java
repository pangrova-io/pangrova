package org.pangrova.coordinator.property.scheduler;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for topology-aware scheduling in TaskScheduler.
 *
 * <p><b>Validates: Requirements 7.3-7.5</b></p>
 */
class TopologyAwareSchedulingPropertyTest {

    private final TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
    private static final long SMALL_TASK = 256L * 1024 * 1024;

    // -- Arbitraries --

    @Provide
    Arbitrary<String> rackIds() {
        return Arbitraries.of("rack-1", "rack-2", "rack-3");
    }

    @Provide
    Arbitrary<Integer> workerCounts() {
        return Arbitraries.integers().between(1, 5);
    }

    @Provide
    Arbitrary<String> locations() {
        return Arbitraries.of(
                "/data/node1/table/part-0001",
                "/data/node2/table/part-0002",
                "/data/node3/table/part-0003");
    }

    // -- Property 1: Data-local worker is preferred when available --

    /**
     * When a worker's dataLocality contains a preferred location,
     * that worker must be selected over non-local workers.
     *
     * <p><b>Validates: Requirements 7.3</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: topology-aware-scheduling, Property 1: data-local preference")
    void dataLocalWorkerPreferredWhenAvailable(
            @ForAll("workerCounts") int remoteCount) {

        String preferredLoc = "/data/node1/table/part-0001";
        WorkerInfo localWorker = TestWorkerInfoBuilder.withTopology(
                "local-w", "host-local", 8080, 9090,
                "rack-1", List.of(preferredLoc));

        List<WorkerInfo> candidates = new ArrayList<>();
        candidates.add(localWorker);
        for (int i = 0; i < remoteCount; i++) {
            candidates.add(TestWorkerInfoBuilder.withTopology(
                    "remote-w-" + i, "host-remote-" + i, 8080 + i + 1, 9090,
                    "rack-2", List.of("/data/other/part-" + i)));
        }

        Optional<WorkerInfo> selected = scheduler.selectWorkerWithLocality(
                SMALL_TASK, candidates, null, List.of(preferredLoc));

        assertThat(selected).isPresent();
        assertThat(selected.get().workerId()).isEqualTo("local-w");
    }

    // -- Property 2: Same-rack fallback when no data-local worker --

    /**
     * When no worker has matching dataLocality but a worker shares the same rack
     * (extracted from /rack/ prefix in preferred locations), that rack-local worker
     * must be selected.
     *
     * <p><b>Validates: Requirements 7.4</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: topology-aware-scheduling, Property 2: same-rack fallback")
    void sameRackFallbackWhenNoDataLocalWorker(
            @ForAll("workerCounts") int otherRackCount) {

        String rackHintLocation = "/rack/rack-1/data/table/part-0001";
        WorkerInfo sameRackWorker = TestWorkerInfoBuilder.withRack(
                "rack-w", "host-rack", 8080, 9090, "rack-1");

        List<WorkerInfo> candidates = new ArrayList<>();
        candidates.add(sameRackWorker);
        for (int i = 0; i < otherRackCount; i++) {
            candidates.add(TestWorkerInfoBuilder.withRack(
                    "other-w-" + i, "host-other-" + i, 8081 + i, 9090, "rack-2"));
        }

        Optional<WorkerInfo> selected = scheduler.selectWorkerWithLocality(
                SMALL_TASK, candidates, null, List.of(rackHintLocation));

        assertThat(selected).isPresent();
        assertThat(selected.get().workerId()).isEqualTo("rack-w");
    }

    // -- Property 3: Load-balanced fallback when no same-rack worker --

    /**
     * When no worker has matching dataLocality and no worker shares the same rack,
     * the scheduler falls back to load-balanced selection (best score wins).
     *
     * <p><b>Validates: Requirements 7.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: topology-aware-scheduling, Property 3: load-balanced fallback")
    void loadBalancedFallbackWhenNoRackMatch(
            @ForAll("workerCounts") int candidateCount) {

        String preferredLoc = "/rack/rack-99/data/table/part-0001";
        List<WorkerInfo> candidates = new ArrayList<>();
        for (int i = 0; i < candidateCount; i++) {
            candidates.add(TestWorkerInfoBuilder.withRack(
                    "w-" + i, "host-" + i, 8080 + i, 9090, "rack-" + i));
        }

        Optional<WorkerInfo> selected = scheduler.selectWorkerWithLocality(
                SMALL_TASK, candidates, null, List.of(preferredLoc));

        assertThat(selected).isPresent();
        assertThat(candidates.stream()
                .map(WorkerInfo::workerId)
                .toList())
                .contains(selected.get().workerId());
    }

    // -- Property 4: Empty preferredLocations falls back to load-balanced --

    /**
     * When preferredLocations is empty or null, the scheduler must fall back
     * to standard load-balanced selection without locality preference.
     *
     * <p><b>Validates: Requirements 7.3-7.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: topology-aware-scheduling, Property 4: empty locations fallback")
    void emptyPreferredLocationsFallsBackToLoadBalanced(
            @ForAll("workerCounts") int candidateCount) {

        List<WorkerInfo> candidates = new ArrayList<>();
        for (int i = 0; i < candidateCount; i++) {
            candidates.add(TestWorkerInfoBuilder.active(
                    "w-" + i, "host-" + i, 8080 + i, 9090));
        }

        Optional<WorkerInfo> withEmpty = scheduler.selectWorkerWithLocality(
                SMALL_TASK, candidates, null, List.of());
        Optional<WorkerInfo> withNull = scheduler.selectWorkerWithLocality(
                SMALL_TASK, candidates, null, null);

        assertThat(withEmpty).isPresent();
        assertThat(withNull).isPresent();
    }
}
