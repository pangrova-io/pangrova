package org.pangrova.shuffle.server.persistence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class JdbcTransactionsTest extends ContainerTestBase {
    private DataSource source;

    @BeforeEach
    void createProbe() throws SQLException {
        source = SharedPostgres.isolatedDataSource();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE transaction_probe (id INTEGER PRIMARY KEY)");
        }
    }

    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedMutationNeverCommits(Throwable failure, SQLException rollbackFailure) throws SQLException {
        DataSource failingSource = JdbcMutationFault.afterMutation(source,
                "INSERT INTO transaction_probe ", failure, rollbackFailure);
        Throwable caught = catchThrowable(() -> JdbcTransactions.transaction(failingSource,
                JdbcTransactionsTest::insertProbe));

        assertProbeCount(0);
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(IllegalStateException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }

    @Test
    void successfulWorkCommitsAndReturnsItsResult() throws SQLException {
        assertThat(JdbcTransactions.transaction(source, JdbcTransactionsTest::insertProbe)).isEqualTo(1);
        assertProbeCount(1);
    }

    @Test
    void checkedWorkFailureRollsBackAndPreservesItsCause() throws SQLException {
        Exception failure = new Exception("work failed");
        Throwable caught = catchThrowable(() -> JdbcTransactions.transaction(source, connection -> {
            insertProbe(connection);
            throw failure;
        }));
        assertProbeCount(0);
        assertThat(caught).isInstanceOf(IllegalStateException.class).hasCause(failure);
    }

    private static int insertProbe(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO transaction_probe (id) VALUES (?)")) {
            statement.setInt(1, 1);
            return statement.executeUpdate();
        }
    }

    private void assertProbeCount(int expected) throws SQLException {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM transaction_probe")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getInt(1)).isEqualTo(expected);
        }
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after mutation"), new IllegalStateException("after mutation"),
                new AssertionError("after mutation")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
    }
}
