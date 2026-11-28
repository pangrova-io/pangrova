package org.pangrova.coordinator.query.task;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.query.profiling.enums.DispatchPhase;
import org.pangrova.query.profiling.enums.PreparationOutcome;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryStageMetricsTest {
    @Test
    void aggregatesTheSameProfileIntervalsAcrossQueriesWithBoundedDimensions() {
        var provider = new RecordingProvider();
        var registry = new QueryOperatorStatsRegistry(provider);
        var first = begin(registry, "first");
        var second = begin(registry, "second");
        first.recordDispatchWork(7, 3, DispatchPhase.ENUMERATE_SPLITS, first.elapsedNanos(), 2, true);
        second.recordDispatchWork(9, 4, DispatchPhase.ENUMERATE_SPLITS, second.elapsedNanos(), null, false);

        var firstWork = first.submitPreparation();
        firstWork.start();
        firstWork.finish(PreparationOutcome.COMPLETED);
        var secondWork = second.submitPreparation();
        secondWork.start();
        secondWork.finish(PreparationOutcome.SKIPPED);
        var failed = second.submitPreparation();
        failed.start();
        failed.finish(PreparationOutcome.FAILED);
        second.submitPreparation().finish(PreparationOutcome.REJECTED);

        assertThat(provider.created).hasSize(4).doesNotHaveDuplicates();
        var firstGraph = registry.executionGraph("first");
        var secondGraph = registry.executionGraph("second");
        var dispatch = provider.named("pangrova.coordinator.stage.dispatch.latency_ms");
        assertThat(dispatch).containsExactly(
                sample(firstGraph.stageDispatchWork().get(0), "SUCCESS"),
                sample(secondGraph.stageDispatchWork().get(0), "FAILED"));

        assertThat(provider.named("pangrova.coordinator.stage.preparation.outcomes"))
                .extracting(Sample::attributes)
                .containsExactly(Map.of("outcome", "COMPLETED"), Map.of("outcome", "SKIPPED"),
                        Map.of("outcome", "FAILED"), Map.of("outcome", "REJECTED"));
        var queue = provider.named("pangrova.coordinator.stage.preparation.queue_ms");
        var run = provider.named("pangrova.coordinator.stage.preparation.run_ms");
        var started = new ArrayList<>(firstGraph.downstreamPreparations());
        started.addAll(secondGraph.downstreamPreparations().subList(0, 2));
        assertThat(queue).containsExactlyElementsOf(started.stream().map(work ->
                new Sample("pangrova.coordinator.stage.preparation.queue_ms",
                        millis(work.startedNanos() - work.submittedNanos()),
                        Map.of("outcome", work.outcome().name()))).toList());
        assertThat(run).containsExactlyElementsOf(started.stream().map(work ->
                new Sample("pangrova.coordinator.stage.preparation.run_ms",
                        millis(work.finishedNanos() - work.startedNanos()),
                        Map.of("outcome", work.outcome().name()))).toList());
        assertThat(provider.samples).hasSize(12);
    }

    @Test
    void recordingFailureCannotChangeProfileOutcome() {
        var provider = new RecordingProvider();
        provider.failRecording = true;
        var registry = new QueryOperatorStatsRegistry(provider);
        var observations = begin(registry, "failure");
        observations.recordDispatchWork(0, 0, DispatchPhase.SERIALIZE_PLAN, observations.elapsedNanos(), null, false);
        observations.submitPreparation().finish(PreparationOutcome.REJECTED);
        var graph = registry.executionGraph("failure");
        assertThat(graph.stageDispatchWork()).singleElement().satisfies(work ->
                assertThat(work.successful()).isFalse());
        assertThat(graph.downstreamPreparations()).singleElement().satisfies(work ->
                assertThat(work.outcome()).isEqualTo(PreparationOutcome.REJECTED));
    }

    private static QueryStageObservations begin(QueryOperatorStatsRegistry registry, String queryId) {
        return registry.beginExecution(queryId, StageDAG.fromNodes(List.of(), Map.of(), List.of()));
    }

    private static Sample sample(org.pangrova.query.profiling.OperatorStatsSource.StageDispatchWork work,
            String outcome) {
        return new Sample("pangrova.coordinator.stage.dispatch.latency_ms",
                millis(work.finishedNanos() - work.startedNanos()),
                Map.of("phase", work.phase().name(), "outcome", outcome));
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private record Sample(String name, double value, Map<String, String> attributes) {}

    private static final class RecordingProvider extends NoOpMetricsProvider {
        private final List<String> created = new ArrayList<>();
        private final List<Sample> samples = new ArrayList<>();
        private boolean failRecording;

        @Override
        public DoubleHistogram createHistogram(String name, String description, String unit, List<Double> boundaries) {
            created.add(name);
            return new DoubleHistogram() {
                @Override
                public void record(double value) {
                    throw new AssertionError("Expected bounded dimensions");
                }

                @Override
                public void record(double value, MetricAttributes attributes) {
                    capture(name, value, attributes);
                }
            };
        }

        @Override
        public LongCounter createCounter(String name, String description, String unit) {
            created.add(name);
            return new LongCounter() {
                @Override
                public void add(long value) {
                    throw new AssertionError("Expected bounded dimensions");
                }

                @Override
                public void add(long value, MetricAttributes attributes) {
                    capture(name, value, attributes);
                }
            };
        }

        private void capture(String name, double value, MetricAttributes attributes) {
            if (failRecording) throw new IllegalStateException("Metrics unavailable");
            samples.add(new Sample(name, value, attributes.asMap()));
        }

        private List<Sample> named(String name) {
            return samples.stream().filter(sample -> sample.name().equals(name)).toList();
        }
    }
}
