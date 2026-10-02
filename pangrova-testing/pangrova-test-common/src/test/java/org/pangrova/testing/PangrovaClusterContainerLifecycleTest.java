package org.pangrova.testing;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration failures must clean up the owned cluster even before the first container starts. */
class PangrovaClusterContainerLifecycleTest {
    @Test
    void invalidConstructionFailsBeforeAllocatingClusterResources() {
        assertThatThrownBy(() -> new PangrovaClusterContainer(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PangrovaClusterContainer(Integer.MIN_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
        var image = DockerImageName.parse("pangrova/pangrova:latest");
        assertThatThrownBy(() -> new PangrovaClusterContainer(image, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PangrovaClusterContainer(null, 1)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void coordinatorAccessorBorrowsTheConfiguredContainerWithoutReplacingIdentity() {
        var original = new IllegalArgumentException("stop before Docker startup");
        try (var cluster = new PangrovaClusterContainer(1)) {
            assertThat(cluster.getCoordinator()).isNull();
            cluster.configureCoordinator(container -> {
                assertThat(cluster.getCoordinator()).isSameAs(container);
                container.withEnv("BORROWED_HANDLE_TEST", "configured");
                throw original;
            });
            assertThatThrownBy(cluster::start).isSameAs(original);
            assertThat(cluster.getCoordinator().getEnvMap()).containsEntry("BORROWED_HANDLE_TEST", "configured");
            assertThat(cluster.isStarted()).isFalse();
        }
    }
    @Test
    void configurationFailureClosesClusterAndPreservesOriginalFailure() {
        var original = new IllegalArgumentException("invalid production configuration");
        try (var cluster = new PangrovaClusterContainer(2)) {
            cluster.configureCoordinator(container -> { throw original; });
            assertThatThrownBy(cluster::start).isSameAs(original);
            assertThat(cluster.isStarted()).isFalse();
            assertThat(cluster.getWorkers()).isEmpty();
            assertThatThrownBy(cluster::start).isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cluster is closed");
        }
    }

    @Test
    void closedClusterRejectsMutationAndRepeatedCloseIsSafe() {
        var cluster = new PangrovaClusterContainer(2);
        cluster.close();
        cluster.close();
        assertThatThrownBy(() -> cluster.configureCoordinator(ignored -> {}))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cluster.configureWorkers((number, worker) -> {}))
                .isInstanceOf(IllegalStateException.class);
    }
}
