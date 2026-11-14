package org.pangrova.testing.testkit;

import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.Network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration failures must clean up the owned cluster even before the first container starts. */
class PangrovaClusterContainerLifecycleTest {
    @Test
    void stoppingAClusterReleasesOnlyResourcesItOwns() {
        // The cluster was never started. A lookup or close on its borrowed network is an ownership error.
        Network borrowed = new Network() {
            @Override public String getId() { throw new AssertionError("unused network was opened"); }
            @Override public void close() { throw new AssertionError("borrowed network was closed"); }
            @Override public org.junit.runners.model.Statement apply(org.junit.runners.model.Statement base,
                    org.junit.runner.Description description) { return base; }
        };
        try (var cluster = new PangrovaClusterContainer(DockerImageName.parse("pangrova/engine:test"), 1, borrowed)) {
            assertThat(cluster.getNetwork()).isSameAs(borrowed);
        }
    }

    @Test
    void coordinatorStartupProbeRequiresItsOwnHttpCheck() {
        String pendingCluster = """
                {"ready":false,"nodeType":"COORDINATOR",
                 "checks":{"http":true,"hasWorkers":false,"hasCatalogs":false}}
                """;
        assertThat(PangrovaClusterContainer.coordinatorHttpServing(pendingCluster))
                .isTrue();
        assertThat(PangrovaClusterContainer.coordinatorHttpServing(
                """
                {"ready":true,"nodeType":"COORDINATOR","checks":{"http":true}}
                """))
                .isTrue();
        assertThat(PangrovaClusterContainer.coordinatorHttpServing(
                """
                {"ready":false,"nodeType":"COORDINATOR","checks":{"http":false}}
                """))
                .isFalse();
        assertThat(PangrovaClusterContainer.coordinatorHttpServing(
                """
                {"ready":true,"nodeType":"WORKER","checks":{"http":true}}
                """))
                .isFalse();
        assertThat(PangrovaClusterContainer.coordinatorHttpServing(
                """
                {"ready":true,"nodeType":"COORDINATOR","checks":{"http":"true"}}
                """))
                .isFalse();
        assertThat(PangrovaClusterContainer.coordinatorHttpServing("{invalid"))
                .isFalse();
    }

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
                assertThat(container.getEnvMap()).containsEntry("PANGROVA_COORDINATOR_ID", "e2e-coordinator")
                        .doesNotContainKey("COORDINATOR_ID");
                container.withEnv("BORROWED_HANDLE_TEST", "configured");
                throw original;
            });
            assertThatThrownBy(cluster::start).isSameAs(original);
            assertThat(cluster.getCoordinator().getEnvMap()).containsEntry("BORROWED_HANDLE_TEST", "configured");
            assertThat(cluster.isStarted()).isFalse();
        }
    }

    @Test
    void workerEnvironmentProvidesDistinctReachableProductionIdentity() {
        var first = PangrovaClusterContainer.workerEnvironment(1);
        var second = PangrovaClusterContainer.workerEnvironment(2);
        assertThat(first).containsEntry("PANGROVA_WORKER_ID", "e2e-worker-1")
                .containsEntry("PANGROVA_COORDINATOR_URL", "http://coordinator:8080")
                .containsEntry("PANGROVA_ADVERTISED_HOST", "worker-1")
                .doesNotContainKeys("WORKER_ID", "WORKER_COORDINATOR_URL");
        assertThat(second).containsEntry("PANGROVA_WORKER_ID", "e2e-worker-2")
                .containsEntry("PANGROVA_ADVERTISED_HOST", "worker-2");
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
