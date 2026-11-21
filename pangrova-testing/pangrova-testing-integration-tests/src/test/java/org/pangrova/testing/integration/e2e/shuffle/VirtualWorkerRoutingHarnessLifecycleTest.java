package org.pangrova.testing.integration.e2e.shuffle;

import org.pangrova.testing.testkit.TestCluster;
import org.pangrova.testing.integration.e2e.E2ETestFramework;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;
import org.pangrova.testing.integration.e2e.setup.TestDataSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests of harness ownership, not Native or distributed execution acceptance. */
class VirtualWorkerRoutingHarnessLifecycleTest {

    private SharedState original;
    private SharedState shared;
    private final AtomicInteger sharedClosed = new AtomicInteger();

    @BeforeEach
    void installSharedFixture() {
        original = SharedProbe.capture();
        TestCluster cluster = new TestCluster(
                () -> sharedClosed::incrementAndGet, () -> sharedClosed::incrementAndGet);
        E2ETestFramework framework = new E2ETestFramework(cluster, sql -> List.of());
        SqlTestRunner runner = new SqlTestRunner(framework);
        shared = new SharedState(cluster, framework, runner, new TestDataSetup(runner));
        SharedProbe.restore(shared);
        framework.setup();
    }

    @AfterEach
    void restoreSharedFixture() {
        try {
            VirtualWorkerRoutingIntegrationTestBase.stopVirtualWorkerRoutingHarness();
        } finally {
            SharedProbe.restore(original);
            shared.framework().teardown();
        }
    }

    @Test
    void routingSetupAndStopNeverReplaceOrCloseTheSharedFixture() {
        AtomicInteger routingClosed = new AtomicInteger();
        TestCluster routing = new TestCluster(
                () -> routingClosed::incrementAndGet, () -> routingClosed::incrementAndGet);

        VirtualWorkerRoutingIntegrationTestBase.initializeRoutingHarness(routing);
        assertSharedFixtureOwned();
        assertThat(VirtualWorkerRoutingIntegrationTestBase.routingSqlRunner)
                .isNotNull().isNotSameAs(shared.runner());

        VirtualWorkerRoutingIntegrationTestBase.stopVirtualWorkerRoutingHarness();
        assertSharedFixtureOwned();
        assertThat(routing.isRunning()).isFalse();
        assertThat(routingClosed).hasValue(2);
        assertThat(VirtualWorkerRoutingIntegrationTestBase.routingSqlRunner).isNull();
    }

    @Test
    void failedWorkerStartupCannotPoisonTheSharedRunner() {
        AtomicInteger coordinatorClosed = new AtomicInteger();
        IllegalStateException startupFailure = new IllegalStateException("owned routing worker failed");
        TestCluster routing = new TestCluster(() -> coordinatorClosed::incrementAndGet,
                () -> { throw startupFailure; });

        assertThatThrownBy(() -> VirtualWorkerRoutingIntegrationTestBase.initializeRoutingHarness(routing))
                .isSameAs(startupFailure);
        VirtualWorkerRoutingIntegrationTestBase.stopVirtualWorkerRoutingHarness();

        assertSharedFixtureOwned();
        assertThat(coordinatorClosed).hasValue(1);
        assertThat(VirtualWorkerRoutingIntegrationTestBase.routingSqlRunner).isNull();
    }

    @Test
    void aSecondOverlayCannotDiscardTheFirstOwnedHarness() {
        AtomicInteger firstClosed = new AtomicInteger();
        TestCluster first = new TestCluster(() -> firstClosed::incrementAndGet,
                () -> firstClosed::incrementAndGet);
        AtomicBoolean secondStarted = new AtomicBoolean();
        TestCluster second = new TestCluster(() -> {
            secondStarted.set(true);
            return () -> { };
        }, () -> () -> { });
        VirtualWorkerRoutingIntegrationTestBase.initializeRoutingHarness(first);
        SqlTestRunner firstRunner = VirtualWorkerRoutingIntegrationTestBase.routingSqlRunner;

        assertThatThrownBy(() -> VirtualWorkerRoutingIntegrationTestBase.initializeRoutingHarness(second))
                .isInstanceOf(IllegalStateException.class).hasMessage("A routing harness is already owned");

        assertThat(secondStarted).isFalse();
        assertThat(firstClosed).hasValue(0);
        assertThat(first.isRunning()).isTrue();
        assertThat(VirtualWorkerRoutingIntegrationTestBase.routingSqlRunner).isSameAs(firstRunner);
        assertSharedFixtureOwned();
        VirtualWorkerRoutingIntegrationTestBase.stopVirtualWorkerRoutingHarness();
        assertThat(firstClosed).hasValue(2);
    }

    private void assertSharedFixtureOwned() {
        SharedState actual = SharedProbe.capture();
        assertThat(actual.cluster()).isSameAs(shared.cluster());
        assertThat(actual.framework()).isSameAs(shared.framework());
        assertThat(actual.runner()).isSameAs(shared.runner());
        assertThat(actual.setup()).isSameAs(shared.setup());
        assertThat(shared.cluster().isRunning()).isTrue();
        assertThat(sharedClosed).hasValue(0);
    }

    private record SharedState(TestCluster cluster, E2ETestFramework framework,
                               SqlTestRunner runner, TestDataSetup setup) { }

    private abstract static class SharedProbe extends SqlE2ETestBase {
        static SharedState capture() {
            return new SharedState(testCluster, framework, sqlRunner, dataSetup);
        }

        static void restore(SharedState state) {
            testCluster = state.cluster();
            framework = state.framework();
            sqlRunner = state.runner();
            dataSetup = state.setup();
        }
    }
}
