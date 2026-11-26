package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DAGTaskDeadlineConfigurationTest {
    @Test
    void unsetProcessTimeoutIsResolvedOnceToABoundedPositiveConfiguration() {
        assertThat(context(0).taskTimeoutSeconds()).isEqualTo(300);
        assertThat(context(17).taskTimeoutSeconds()).isEqualTo(17);
    }

    @Test
    void negativeConfigurationIsRejectedRatherThanBecomingAnUnboundedDeadline() {
        assertThatThrownBy(() -> context(-1)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    private static DAGDispatchContext context(long timeoutSeconds) {
        TaskDispatcher uninvoked = (request, worker) -> {
            throw new AssertionError("Configuration validation must not dispatch tasks");
        };
        return new DAGDispatchContext(uninvoked, new CoordinatorTaskTracker(), null, null,
                timeoutSeconds, null, null, null, PipelineConfig.DISABLED,
                null, null, null);
    }
}
