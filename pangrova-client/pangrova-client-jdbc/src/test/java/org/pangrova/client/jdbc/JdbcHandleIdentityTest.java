package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.result.TestResultEndpoint;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JDBC/HTTP/result handle contracts; not Coordinator or Native SQL execution. */
class JdbcHandleIdentityTest {
    @Test
    void closingConnectionReleasesItsPlainAndPreparedStatementsAndFetchedResults() throws Exception {
        try (var endpoint = new TestResultEndpoint(42); var peer = new StatementCancellationPeer()) {
            var connection = peer.statement().getConnection();
            var statement = connection.createStatement();
            var prepared = connection.prepareStatement("SELECT ?");
            peer.query("parent-close-results", endpoint.endpoint());
            var rows = statement.executeQuery("SELECT 42");

            connection.close();
            assertThat(connection.isClosed()).isTrue();
            assertThat(statement.isClosed()).isTrue();
            assertThat(prepared.isClosed()).isTrue();
            assertThat(rows.isClosed()).isTrue();
            assertThatThrownBy(() -> prepared.setInt(1, 42)).isInstanceOf(SQLException.class);
            connection.close();
        }
    }

    @Test
    void aRefusedCancelDuringConnectionCloseRetainsTheHttpOwnerForRetry() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var connection = peer.statement().getConnection();
            var query = peer.query("close-in-flight", null);
            query.replies.add(new StatementCancellationPeer.Reply(403,
                    "{\"error_code\":\"FORBIDDEN\",\"message\":\"cancel denied\"}"));
            var execution = peer.executeAsync();
            assertThat(query.polled.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(connection::close).isInstanceOf(SQLException.class).hasMessageContaining(query.id);
            assertThatThrownBy(connection::createStatement).isInstanceOf(SQLException.class);
            peer.statement().cancel();
            assertThat((Throwable) execution.get(5, java.util.concurrent.TimeUnit.SECONDS))
                    .isInstanceOf(SQLException.class).hasMessageContaining("cancelled");
            connection.close();
            assertThat(peer.statement().isClosed()).isTrue();
        }
    }

    @Test
    void metadataKeepsItsProducingConnectionIdentityWithoutInventingClosedGetterRules() throws Exception {
        try (var peer = new StatementCancellationPeer()) {
            var connection = peer.statement().getConnection();
            var metadata = connection.getMetaData();
            assertThat(metadata.getConnection()).isSameAs(connection);
            connection.close();
            assertThat(connection.isClosed()).isTrue();
            // DatabaseMetaData.getConnection does not specify Statement's closed-handle rejection.
            assertThat(metadata.getConnection()).isSameAs(connection);
            assertThatThrownBy(connection::getMetaData).isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Test
    void statementReturnsItsActualConnectionAndRejectsIdentityGettersAfterClose() throws Exception {
        try (var peer = new StatementCancellationPeer();
             var statement = peer.statement().getConnection().createStatement()) {
            var connection = statement.getConnection();
            assertThat(connection.getMetaData().getConnection()).isSameAs(connection);
            assertThat(statement.getConnection()).isSameAs(connection);
            assertThat(statement.getResultSet()).isNull();
            statement.close();
            assertThat(statement.isClosed()).isTrue();
            assertThatThrownBy(statement::getConnection).isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
            assertThatThrownBy(statement::getResultSet).isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Test
    void actualReexecutionReplacesCurrentResultWithoutReplacingItsPublicProducer() throws Exception {
        try (var endpoint = new TestResultEndpoint(42); var peer = new StatementCancellationPeer();
             var statement = peer.statement().getConnection().createStatement()) {
            var first = peer.query("identity-first", endpoint.endpoint());
            var second = peer.query("identity-second", endpoint.endpoint());
            var connection = statement.getConnection();
            var previous = statement.executeQuery("SELECT 42");
            assertThat(statement.getResultSet()).isSameAs(previous);
            assertThat(previous.getStatement()).isSameAs(statement);
            assertThat(previous.getStatement().getConnection()).isSameAs(connection);
            assertCompleteResult(previous);

            try (var current = statement.executeQuery("SELECT 42")) {
                assertThat(previous.isClosed()).isTrue();
                assertThatThrownBy(previous::getStatement).isInstanceOf(SQLException.class)
                        .hasMessageContaining("closed");
                assertThat(current).isNotSameAs(previous);
                assertThat(statement.getResultSet()).isSameAs(current);
                assertThat(statement.getResultSet()).as("Repeated getter preserves the current handle")
                        .isSameAs(current);
                assertThat(current.getStatement()).isSameAs(statement);
                assertThat(current.getStatement().getConnection()).isSameAs(connection);
                assertCompleteResult(current);
            }
            assertThat(peer.submissions()).isEqualTo(2);
            assertThat(first.cancelRequests.get()).isZero();
            assertThat(second.cancelRequests.get()).isZero();
        }
    }

    @Test
    void actualResultCloseRejectsProducerGetterButDoesNotCloseItsStatement() throws Exception {
        try (var endpoint = new TestResultEndpoint(42); var peer = new StatementCancellationPeer();
             var statement = peer.statement().getConnection().createStatement()) {
            var query = peer.query("identity-result-close", endpoint.endpoint());
            var rows = statement.executeQuery("SELECT 42");
            assertThat(rows.getStatement()).isSameAs(statement);
            assertCompleteResult(rows);
            rows.close();
            assertThat(rows.isClosed()).isTrue();
            assertThatThrownBy(rows::getStatement).isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
            assertThat(statement.isClosed()).isFalse();
            assertThat(statement.getResultSet()).isSameAs(rows);
            statement.close();
            assertThatThrownBy(statement::getResultSet).isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
            assertThat(query.cancelRequests.get()).isZero();
        }
    }

    private static void assertCompleteResult(ResultSet rows) throws SQLException {
        assertThat(rows.next()).isTrue();
        assertThat(rows.getInt(1)).isEqualTo(42);
        assertThat(rows.next()).as("Actual result stream reached EOF").isFalse();
    }
}
