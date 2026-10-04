package org.pangrova.coordinator.query;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import static org.assertj.core.api.Assertions.assertThat;

/** Real journal failure plus actual SQL/JNI task and throttle ownership, not a fake permit release. */
@Tag("native-required")
@Tag("routing-integration")
@ResourceLock("pangrova-packaged-native-runtime")
class QueryTerminalJournalPermitIntegrationTest {
    @TempDir Path directory;

    @Test
    void terminalJournalRetryReleasesPermitExactlyOnce() throws Exception {
        try (var fixture = new QueryTerminalNativeFixture(directory)) {
            fixture.submit("SELECT 42");
            fixture.store.denyQueryInsert();
            fixture.consume();
            fixture.awaitLogicalTerminal(QueryStateMachine.State.FINISHED);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isZero());
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isZero();
            fixture.releaseActualWorkerOwners();
            fixture.exitFrontend();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.manager.getActiveQueryCount()).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isZero();
            fixture.store.allowQueryInsert();
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isEqualTo(1);
            assertThat(fixture.manager.getActiveQueryCount()).isZero();
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
            fixture.retryPhysicalCleanup();
            assertThat(fixture.store.terminalWrites(fixture.queryId, "FINISHED")).isEqualTo(1);
            assertThat(fixture.throttle.availableGlobalPermits()).isEqualTo(1);
        }
    }
}
