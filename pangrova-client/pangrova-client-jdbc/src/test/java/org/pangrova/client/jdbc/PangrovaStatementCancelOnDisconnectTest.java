package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.testing.testkit.result.TestResultEndpoint;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real JDBC/HTTP/result transport component boundaries; does not simulate Native SQL success. */
class PangrovaStatementCancelOnDisconnectTest {
    @Test
    void cancelSentWhenFetchResultFails() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var endpoint = result.endpoint();
            var query = peer.query("q-cancel-test-001", new ResultEndpoint(
                    endpoint.host(), endpoint.port(), "unknown-ticket", endpoint.protocol()));
            assertThatThrownBy(() -> peer.statement().executeQuery("SELECT 1")).isInstanceOf(SQLException.class);
            assertThat(peer.submissions()).isEqualTo(1);
            assertThat(query.cancelRequests.get()).as("Cancel sent after actual transport rejects ticket").isEqualTo(1);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
        }
    }

    @Test
    void cancelSentWhenPollingFails() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("q-cancel-test-002", null);
            query.statusFailure.set(new StatementCancellationPeer.Reply(500,
                    "{\"error_code\":\"INTERNAL_ERROR\",\"message\":\"coordinator crashed\"}"));
            assertThatThrownBy(() -> peer.statement().executeQuery("SELECT 1")).isInstanceOf(SQLException.class);
            assertThat(peer.submissions()).isEqualTo(1);
            assertThat(query.cancelRequests.get()).as("Cancel sent after actual HTTP polling error").isEqualTo(1);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
        }
    }

    @Test
    void cancelNotSentOnSuccess() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var query = peer.query("q-cancel-test-003", result.endpoint());
            try (var rows = peer.statement().executeQuery("SELECT 42")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(42);
                assertThat(rows.next()).as("Actual result stream reached EOF").isFalse();
            }
            assertThat(peer.submissions()).isEqualTo(1);
            assertThat(query.cancelRequests.get()).isZero();
            peer.statement().cancel();
            assertThat(query.cancelRequests.get()).isZero();
        }
    }
}
