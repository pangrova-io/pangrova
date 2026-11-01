package org.pangrova.query.store.jdbc.anchor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.query.coordination.ExecutionOwnerLostException;
import org.pangrova.spi.query.coordination.ExecutionOwnerLostException.Reason;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Actual PostgreSQL transaction, lock and error semantics of Query's consumer-facing fence. */
class JdbcExecutionTransactionFenceIntegrationTest extends ContainerTestBase {
    private final JdbcExecutionTransactionFence fence = new JdbcExecutionTransactionFence();
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgres.isolatedDataSource();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE execution_anchor (execution_id VARCHAR(128) PRIMARY KEY, "
                    + "owner_id VARCHAR(128) NOT NULL, owner_incarnation VARCHAR(64) NOT NULL, "
                    + "fencing_token BIGINT NOT NULL CHECK (fencing_token>0), acquired_at TIMESTAMPTZ(6) NOT NULL, "
                    + "expires_at TIMESTAMPTZ(6) NOT NULL, version BIGINT NOT NULL DEFAULT 1)");
            statement.execute("INSERT INTO execution_anchor VALUES "
                    + "('execution','owner','process',7,clock_timestamp()-interval '1 minute',"
                    + "clock_timestamp()+interval '10 minutes',1)");
        }
    }

    @Test
    void usesTheCallersUncommittedStateWithoutCommittingRollingBackOrClosingIt() throws Exception {
        try (var caller = dataSource.getConnection(); var observer = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            try (var update = caller.createStatement()) {
                update.executeUpdate("UPDATE execution_anchor SET fencing_token=8, "
                        + "expires_at=expires_at+interval '1 hour'");
            }
            var actual = fence.requireCurrent(caller, "execution", "owner", "process", 8);
            assertThat(actual.fencingToken()).isEqualTo(8);
            try (var statement = caller.createStatement(); var row = statement.executeQuery(
                    "SELECT acquired_at,expires_at FROM execution_anchor")) {
                assertThat(row.next()).isTrue();
                assertThat(actual.acquiredAt()).isEqualTo(row.getTimestamp(1).toInstant());
                assertThat(actual.expiresAt()).isEqualTo(row.getTimestamp(2).toInstant());
            }
            assertThat(caller.isClosed()).isFalse();
            assertThat(caller.getAutoCommit()).isFalse();
            assertThat(token(observer)).isEqualTo(7);
            caller.rollback();
            assertThat(token(caller)).isEqualTo(7);
        }
    }

    @Test
    void retainsTheSharedAnchorLockUntilTheCallerEndsTheTransaction() throws Exception {
        try (var caller = dataSource.getConnection(); var contender = dataSource.getConnection()) {
            caller.setAutoCommit(false);
            fence.requireCurrent(caller, "execution", "owner", "process", 7);
            try (var update = contender.createStatement()) {
                var locked = assertThrows(SQLException.class,
                        () -> update.executeQuery("SELECT * FROM execution_anchor FOR UPDATE NOWAIT"));
                assertThat(locked.getSQLState()).isEqualTo("55P03");
                caller.commit();
                try (var row = update.executeQuery("SELECT * FROM execution_anchor FOR UPDATE NOWAIT")) {
                    assertThat(row.next()).isTrue();
                }
            }
        }
    }

    @Test
    void distinguishesMissingReplacedAndExpiredOwners() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            assertLoss(connection, "missing", "owner", "process", 7, Reason.ABSENT);
            assertLoss(connection, "execution", "other-owner", "process", 7, Reason.REPLACED);
            assertLoss(connection, "execution", "owner", "previous-process", 7, Reason.REPLACED);
            assertLoss(connection, "execution", "owner", "process", 8, Reason.REPLACED);
            try (var update = connection.createStatement()) {
                update.executeUpdate("UPDATE execution_anchor SET expires_at=clock_timestamp()-interval '1 second'");
            }
            assertLoss(connection, "execution", "owner", "process", 7, Reason.EXPIRED);
            connection.rollback();
        }
    }

    @Test
    void rejectsAutocommitAndInvalidOwnerInputWithoutClaimingOwnershipLoss() throws Exception {
        try (var connection = dataSource.getConnection()) {
            assertThatThrownBy(() -> fence.requireCurrent(connection, "execution", "owner", "process", 7))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requires a transaction");
            connection.setAutoCommit(false);
            assertThatThrownBy(() -> fence.requireCurrent(connection, "execution", "owner", "process", 0))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive");
            assertThatThrownBy(() -> fence.requireCurrent(connection, "execution", "owner", " ", 7))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ownerIncarnation");
            connection.rollback();
        }
    }

    @Test
    void storageFailureAndCorruptAnchorAreNotStaleOwnerRejections() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate("UPDATE execution_anchor SET expires_at=acquired_at");
            assertThatThrownBy(() -> fence.requireCurrent(connection, "execution", "owner", "process", 7))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("expiresAt");
            connection.rollback();
            statement.execute("DROP TABLE execution_anchor");
            var failure = assertThrows(SQLException.class,
                    () -> fence.requireCurrent(connection, "execution", "owner", "process", 7));
            assertThat(failure.getSQLState()).isEqualTo("42P01");
            connection.rollback();
        }
    }

    private void assertLoss(Connection connection, String execution, String owner, String incarnation,
            long token, Reason reason) {
        var failure = assertThrows(ExecutionOwnerLostException.class,
                () -> fence.requireCurrent(connection, execution, owner, incarnation, token));
        assertThat(failure.reason()).isEqualTo(reason);
    }

    private static long token(Connection connection) throws SQLException {
        try (var statement = connection.createStatement();
             var row = statement.executeQuery("SELECT fencing_token FROM execution_anchor")) {
            assertThat(row.next()).isTrue();
            return row.getLong(1);
        }
    }
}
