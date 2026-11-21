package org.pangrova.scheduler.strategies.placement;

import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.api.placement.WorkerSelectionCandidate;
import org.pangrova.scheduler.api.placement.WorkerSelectionRequest;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategyFactory;
import java.util.List;
import java.util.Collections;
import java.util.stream.IntStream;
import static org.assertj.core.api.Assertions.assertThat;

class LoadBalancedStrategyTest {
    @Test
    void rankingUsesCurrentRequestHeadroom() {
        var strategy = new LoadBalancedStrategyFactory().create();
        assertThat(strategy.select(request(new WorkerSelectionCandidate("cpu", 0, .9),
                new WorkerSelectionCandidate("memory", .9, 0), new WorkerSelectionCandidate("headroom", .1, .1))))
                .contains("headroom");
        assertThat(strategy.select(request(new WorkerSelectionCandidate("replacement", .9, .9))))
                .contains("replacement");
        assertThat(strategy.select(request())).isEmpty();
    }

    @Test
    void concurrentTiesRotateWithoutDependingOnInputOrder() {
        var strategy = new LoadBalancedStrategyFactory().create();
        var request = request(new WorkerSelectionCandidate("z", .1, .1), new WorkerSelectionCandidate("a", .1, .1));
        var selected = IntStream.range(0, 1000).parallel()
                .mapToObj(i -> strategy.select(request).orElseThrow()).toList();
        assertThat(Collections.frequency(selected, "a")).isEqualTo(500);
        assertThat(Collections.frequency(selected, "z")).isEqualTo(500);
    }

    @Test
    void factoryCreatesIndependentCoordinatorScopes() {
        var factory = new LoadBalancedStrategyFactory();
        assertThat(factory.providerId()).isEqualTo("load-balanced");
        assertThat(factory.apiVersion()).isEqualTo(WorkerSelectionStrategyFactory.API_VERSION);
        var first = factory.create();
        var second = factory.create();
        var request = request(new WorkerSelectionCandidate("z", .1, .1), new WorkerSelectionCandidate("a", .1, .1));
        assertThat(first.select(request)).contains("a");
        assertThat(first.select(request)).contains("z");
        assertThat(second.select(request)).contains("a");
    }

    private static WorkerSelectionRequest request(WorkerSelectionCandidate... candidates) {
        return new WorkerSelectionRequest(List.of(candidates));
    }
}
