package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.MemoryPressure;
import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.scheduler.api.enums.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategy;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategyFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuerySchedulingStrategyFactoryTest {
    @Test
    void incompatibleFactoryIsRejectedBeforeCreation() {
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, "test", List.of(new IncompatibleFactory("test", 0))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported placement API version");
    }

    @Test
    void emptyProviderIdentityIsRejectedBeforeCreation() {
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, "test", List.of(new IncompatibleFactory(" ", 1))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("providerId");
    }

    @Test
    void builtinIsSelectedFromItsPackagedDescriptor() {
        try (var wiring = new QuerySchedulingWiring(null, "load-balanced")) {
            var busy = worker("busy", .8);
            var idle = worker("idle", 0);
            assertThat(wiring.scheduler().selectWorker(1, List.of(busy, idle))).containsSame(idle);
        }
    }

    @Test
    void missingProviderFailsWithoutSubstitutingTheBuiltin() {
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, "missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing").hasMessageContaining("load-balanced");
    }

    @Test
    void duplicateProviderIsRejectedBeforeEitherCreatesAStrategy() {
        var factory = new IncompatibleFactory("duplicate", WorkerSelectionStrategyFactory.API_VERSION);
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, "duplicate", List.of(factory, factory)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate placement provider");
    }

    @Test
    void configuredTestPluginCanSelectAnotherEligibleWorker() {
        var config = PangrovaCoordinatorMain.buildBootstrapConfig(PangrovaConfiguration.fromYaml(
                Map.of("coordinator.query.placement-provider", "test-first-eligible")));
        var busy = worker("busy", .8);
        var idle = worker("idle", 0);
        try (var wiring = new QuerySchedulingWiring(null, config.query().placementProvider())) {
            assertThat(wiring.scheduler().selectWorker(1, List.of(busy, idle))).containsSame(busy);
        }
    }

    public static final class FirstEligibleFactory implements WorkerSelectionStrategyFactory {
        @Override public String providerId() { return "test-first-eligible"; }
        @Override public int apiVersion() { return API_VERSION; }
        @Override public WorkerSelectionStrategy create() {
            return request -> Optional.of(request.candidates().get(0).workerId());
        }
    }

    private static Worker worker(String id, double cpuUtilization) {
        return new Worker(id, "localhost", 8080, HealthState.ACTIVE, MemoryPressure.NONE,
                cpuUtilization, 0, 1024, null, null, List.of());
    }

    private record Worker(String workerId, String host, int httpPort, HealthState healthState,
                          MemoryPressure memoryPressure, double cpuUtilization, int runningTaskCount,
                          long availableMemoryBytes, String resourcePoolId, String rackId,
                          List<String> dataLocality) implements PlacementWorker {}

    private record IncompatibleFactory(String providerId, int apiVersion) implements WorkerSelectionStrategyFactory {
        @Override
        public WorkerSelectionStrategy create() {
            throw new AssertionError("Invalid factory must not be called");
        }
    }
}
