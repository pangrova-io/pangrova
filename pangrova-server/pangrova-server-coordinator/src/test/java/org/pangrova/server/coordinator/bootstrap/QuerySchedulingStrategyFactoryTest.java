package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategy;
import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategyFactory;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuerySchedulingStrategyFactoryTest {
    @Test
    void incompatibleFactoryIsRejectedBeforeCreation() {
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, new IncompatibleFactory("test", 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported placement API version");
    }

    @Test
    void emptyProviderIdentityIsRejectedBeforeCreation() {
        assertThatThrownBy(() -> new QuerySchedulingWiring(null, new IncompatibleFactory(" ", 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("providerId");
    }

    private record IncompatibleFactory(String providerId, int apiVersion) implements WorkerSelectionStrategyFactory {
        @Override
        public WorkerSelectionStrategy create() {
            throw new AssertionError("Invalid factory must not be called");
        }
    }
}
