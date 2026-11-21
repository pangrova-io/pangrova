package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.result.TestResultEndpoint;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Actual JDBC/HTTP/result component ownership; does not prove Coordinator or Native execution. */
class PreparedStatementExecutionOwnerTest {
    @Test
    void preparedCancelAddressesItsActualInFlightQuery() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("prepared-cancel", null);
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            var execution = peer.executePreparedAsync(prepared);
            try {
                assertThat(query.polled.await(5, TimeUnit.SECONDS)).isTrue();
                prepared.cancel();
                assertThat(query.cancelRequests.get()).isEqualTo(1);
                assertThat(query.lastCancelled.get()).isEqualTo(query.id);
                assertThat((Throwable) execution.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(SQLException.class).hasMessageContaining("cancelled");
            } finally {
                query.state.set("CANCELLED");
                execution.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void preparedTimeoutIsAppliedToTheActualPollingExecution() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("prepared-deadline", null);
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            prepared.setQueryTimeout(1);
            var execution = peer.executePreparedAsync(prepared);
            try {
                assertThat(query.polled.await(5, TimeUnit.SECONDS)).isTrue();
                SQLException failure;
                try {
                    failure = execution.get(3, TimeUnit.SECONDS);
                } catch (TimeoutException lateExecution) {
                    fail("Prepared timeout did not bound its actual polling execution", lateExecution);
                    return;
                }
                assertThat((Throwable) failure).isInstanceOf(SQLTimeoutException.class);
                assertThat(query.cancelRequests.get()).isGreaterThanOrEqualTo(1);
                assertThat(query.lastCancelled.get()).isEqualTo(query.id);
            } finally {
                query.state.set("CANCELLED");
                execution.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void timeoutCancellationAcknowledgementRetiresIdentityWithoutASecondCancel() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var timedOut = peer.query("prepared-timeout-ack", null);
            timedOut.replies.add(StatementCancellationPeer.accepted(timedOut.id));
            timedOut.replies.add(StatementCancellationPeer.absent(timedOut.id));
            var next = peer.query("prepared-after-timeout", result.endpoint());
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            prepared.setQueryTimeout(1);

            assertThrows(SQLTimeoutException.class, prepared::executeQuery);

            prepared.setQueryTimeout(0);
            try (var rows = prepared.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(42);
                assertThat(rows.next()).as("Actual result stream reached EOF").isFalse();
                assertThat(rows.getStatement()).isSameAs(prepared);
            }
            assertThat(timedOut.cancelRequests.get()).isEqualTo(1);
            assertThat(timedOut.lastCancelled.get()).isEqualTo(timedOut.id);
            assertThat(next.cancelRequests.get()).isZero();
            assertThat(peer.submissions()).isEqualTo(2);
            prepared.close();
            assertThat(prepared.isClosed()).isTrue();
            assertThat(timedOut.cancelRequests.get()).isEqualTo(1);
        }
    }

    @Test
    void actualResultRetainsItsPublicPreparedProducerIdentity() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var query = peer.query("prepared-producer", result.endpoint());
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            try (var rows = prepared.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt(1)).isEqualTo(42);
                assertThat(rows.next()).as("Actual result stream reached EOF").isFalse();
                assertThat(prepared.getResultSet()).isSameAs(rows);
                assertThat(rows.getStatement()).isSameAs(prepared);
                assertThat(rows.getStatement().getConnection()).isSameAs(prepared.getConnection());
            }
            assertThat(query.cancelRequests.get()).isZero();
        }
    }

    @Test
    void closeRetiresTheActualProducerAndItsCompletedResult() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var query = peer.query("prepared-close", result.endpoint());
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            var rows = prepared.executeQuery();
            var actualProducer = rows.getStatement();
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(42);
            assertThat(rows.next()).isFalse();

            prepared.close();

            assertThat(prepared.isClosed()).isTrue();
            assertThat(rows.isClosed()).isTrue();
            assertThat(actualProducer.isClosed()).isTrue();
            assertThat(query.cancelRequests.get()).isZero();
        }
    }

    @Test
    void closeCannotReportReleaseWhileActualExecutionIsStillOwned() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = peer.query("prepared-close-busy", null);
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            var execution = peer.executePreparedAsync(prepared);
            try {
                assertThat(query.polled.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(prepared::close).isInstanceOf(SQLException.class);
                assertThat(execution.isDone()).isFalse();
                assertThatThrownBy(prepared::executeQuery).isInstanceOf(SQLException.class)
                        .hasMessageContaining("closed");
                assertThat(peer.submissions()).isEqualTo(1);
            } finally {
                query.state.set("CANCELLED");
                assertThat((Throwable) execution.get(5, TimeUnit.SECONDS)).isInstanceOf(SQLException.class);
                prepared.close();
            }
        }
    }

    @Test
    void reexecutionClosesOldResultAndPreservesTheSamePublicProducer() throws Exception {
        try (var result = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var first = peer.query("prepared-first", result.endpoint());
            var second = peer.query("prepared-second", result.endpoint());
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            var previous = prepared.executeQuery();
            assertThat(previous.next()).isTrue();
            assertThat(previous.getInt(1)).isEqualTo(42);
            assertThat(previous.next()).isFalse();
            prepared.clearParameters();
            prepared.setInt(1, 42);
            try (var current = prepared.executeQuery()) {
                assertThat(previous.isClosed()).isTrue();
                assertThat(current).isNotSameAs(previous);
                assertThat(current.next()).isTrue();
                assertThat(current.getInt(1)).isEqualTo(42);
                assertThat(current.next()).isFalse();
                assertThat(current.getStatement()).isSameAs(prepared);
                assertThat(prepared.getResultSet()).isSameAs(current);
            }
            assertThat(peer.submissions()).isEqualTo(2);
            assertThat(first.cancelRequests.get()).isZero();
            assertThat(second.cancelRequests.get()).isZero();
        }
    }

    @Test
    void failedPreparedAutoCancelRetainsIdentityForActualExplicitRetry() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = failedPollingWithRejectedCancel(peer, "prepared-retry");
            var prepared = peer.prepare("SELECT ?");
            prepared.setInt(1, 42);
            SQLException failure = assertThrows(SQLException.class, prepared::executeQuery);
            assertRejectedCleanupIsRetained(failure, query.id);
            assertThat(query.cancelRequests.get()).isEqualTo(1);

            prepared.cancel();

            assertThat(query.cancelRequests.get()).isEqualTo(2);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
        }
    }

    @Test
    void failedStatementAutoCancelRetainsIdentityForActualExplicitRetry() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var query = failedPollingWithRejectedCancel(peer, "statement-retry");
            SQLException failure = assertThrows(SQLException.class,
                    () -> peer.statement().executeQuery("SELECT 42"));
            assertRejectedCleanupIsRetained(failure, query.id);
            assertThat(query.cancelRequests.get()).isEqualTo(1);

            peer.statement().cancel();

            assertThat(query.cancelRequests.get()).isEqualTo(2);
            assertThat(query.lastCancelled.get()).isEqualTo(query.id);
        }
    }

    private static void assertRejectedCleanupIsRetained(SQLException failure, String queryId) {
        assertThat((Throwable) failure).hasMessageContaining("coordinator polling failed");
        assertThat(failure.getCause()).hasMessageContaining("coordinator polling failed");
        assertThat(failure.getCause().getSuppressed()).singleElement().satisfies(cleanup -> {
            assertThat(cleanup).isInstanceOf(SQLException.class)
                    .hasMessageContaining("Failed to cancel query queryId=" + queryId);
            assertThat(cleanup.getCause()).hasMessageContaining("cancel denied");
        });
    }

    private static StatementCancellationPeer.Query failedPollingWithRejectedCancel(
            StatementCancellationPeer peer, String queryId) {
        var query = peer.query(queryId, null);
        query.statusFailure.set(new StatementCancellationPeer.Reply(500,
                "{\"error_code\":\"INTERNAL_ERROR\",\"message\":\"coordinator polling failed\"}"));
        query.replies.add(new StatementCancellationPeer.Reply(403,
                "{\"error_code\":\"FORBIDDEN\",\"message\":\"cancel denied\"}"));
        return query;
    }
}
