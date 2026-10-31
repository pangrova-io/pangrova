package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.client.core.protocol.ResultEndpoint;
import org.pangrova.testing.testkit.result.TestResultEndpoint;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JDBC cancellation against real HTTP protocol inputs and actual result transport, not Native E2E. */
class PangrovaStatementCancelTest {
    @Test
    void cancelSendsCancelRequestDuringExecution() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("q-d41-cancel-001", null);
            var execution = peer.executeAsync();
            assertThat(query.polled.await(5, TimeUnit.SECONDS)).as("Actual JDBC status polling started").isTrue();
            peer.statement().cancel();
            assertThat(query.cancelRequests.get()).as("Server received cancel request").isGreaterThanOrEqualTo(1);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
            assertThat((Throwable) execution.get(5, TimeUnit.SECONDS)).isInstanceOf(SQLException.class)
                    .hasMessageContaining("cancelled");
        }
    }

    @Test
    void cancelIsNoOpWhenIdle() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            peer.statement().cancel();
            assertThat(peer.requests()).isZero();
        }
    }

    @Test
    void currentQueryIdClearedAfterCompletion() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var endpoint = result.endpoint();
            var query = peer.query("q-d41-clear-001", new ResultEndpoint(
                    endpoint.host(), endpoint.port(), "unknown-ticket", endpoint.protocol()));
            assertThatThrownBy(() -> peer.statement().executeQuery("SELECT 1")).isInstanceOf(SQLException.class);
            assertThat(query.cancelRequests.get()).as("Failure cleanup sent the original cancel").isEqualTo(1);
            peer.statement().cancel();
            assertThat(query.cancelRequests.get()).as("Finished execution no longer admits explicit cancel").isEqualTo(1);
        }
    }
}
