package org.pangrova.persistence.indexing.postgresql.build;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import org.pangrova.testing.testkit.ContainerTestBase;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.pangrova.persistence.catalog.api.error.MetastoreException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcIndexBuildClaimOwnershipTest extends ContainerTestBase {
    private DataSource dataSource;
    private JdbcIndexBuildClaimRepository claims;

    @BeforeEach
    void initialize() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("claim_" + UUID.randomUUID().toString().replace("-", ""));
        claims = new JdbcIndexBuildClaimRepository(dataSource);
    }

    @Test
    void delayedOldReleaseMustNotDeleteReplacementClaim() {
        String index = UUID.randomUUID().toString();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertThat(claims.tryClaim(index, 0, "old-attempt", now)).isTrue();
        assertThat(claims.expireBefore(now.plusSeconds(1))).isEqualTo(1);
        assertThat(claims.tryClaim(index, 0, "new-attempt", now.plusSeconds(2))).isTrue();
        // Represents the finally block of the old attempt returning after replacement.
        assertThat(claims.release(index, 0, "old-attempt")).isFalse();
        assertThat(claims.tryClaim(index, 0, "third-attempt", now.plusSeconds(3))).isFalse();
    }

    @Test
    void delayedOldHeartbeatMustNotRenewReplacementClaim() throws SQLException {
        String index = UUID.randomUUID().toString();
        Instant now = Instant.parse("2026-01-02T00:00:00Z");
        assertThat(claims.tryClaim(index, 0, "old-attempt", now)).isTrue();
        // Expire exactly this test's row; no dependence on another test's clock or lifecycle.
        try (var connection = dataSource.getConnection();
                var delete = connection.prepareStatement("DELETE FROM index_build_claims WHERE index_id=?")) {
            delete.setString(1, index);
            assertThat(delete.executeUpdate()).isEqualTo(1);
        }
        Instant replacementHeartbeat = now.plusSeconds(2);
        assertThat(claims.tryClaim(index, 0, "new-attempt", replacementHeartbeat)).isTrue();
        assertThat(claims.heartbeat(index, 0, "old-attempt", now.plusSeconds(30))).isFalse();
        try (var connection = dataSource.getConnection();
                var query = connection.prepareStatement(
                        "SELECT owner_id, heartbeat_at FROM index_build_claims WHERE index_id=? AND generation=0")) {
            query.setString(1, index);
            try (var rows = query.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("new-attempt");
                assertThat(rows.getTimestamp(2).toInstant()).isEqualTo(replacementHeartbeat);
            }
        }
    }
    @Test
    void ownershipAndGenerationAreRequiredForMutation() {
        Instant now = Instant.parse("2026-01-03T00:00:00Z");
        assertThat(claims.tryClaim("index", 0, "first", now)).isTrue();
        assertThat(claims.tryClaim("index", 0, "first", now)).isFalse();
        assertThat(claims.tryClaim("index", 1, "second", now)).isTrue();
        assertThat(claims.release("index", 1, "first")).isFalse();
        assertThat(claims.heartbeat("index", 1, "first", now.plusSeconds(2))).isFalse();
        assertThat(claims.release("index", 0, "first")).isTrue();
        assertThat(claims.release("index", 0, "first")).isFalse();
        assertThat(claims.heartbeat("index", 0, "first", now)).isFalse();
        assertThat(claims.tryClaim("index", 0, "replacement", now)).isTrue();
        assertThat(claims.release("index", 1, "second")).isTrue();
    }

    @Test
    void delayedHeartbeatDoesNotMoveClockBackAndExpiryIsStrict() {
        Instant now = Instant.parse("2026-01-03T00:00:00Z");
        assertThat(claims.tryClaim("index", 0, "attempt", now)).isTrue();
        assertThat(claims.heartbeat("index", 0, "attempt", now.plusSeconds(20))).isTrue();
        assertThat(claims.heartbeat("index", 0, "attempt", now.plusSeconds(10))).isTrue();
        assertThat(claims.expireBefore(now.plusSeconds(20))).isZero();
        assertThat(claims.expireBefore(now.plusSeconds(21))).isEqualTo(1);
        assertThat(claims.expireBefore(now.plusSeconds(21))).isZero();
    }

    @Test
    void concurrentDatabaseAcquisitionHasExactlyOneWinner() throws Exception {
        var executor = Executors.newFixedThreadPool(4);
        var ready = new CountDownLatch(4);
        var go = new CountDownLatch(1);
        List<Future<Boolean>> attempts = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                String owner = "attempt-" + i;
                attempts.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
                    return claims.tryClaim("index", 0, owner, Instant.now());
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            int winners = 0;
            for (var attempt : attempts) if (attempt.get(5, TimeUnit.SECONDS)) winners++;
            assertThat(winners).isEqualTo(1);
        } finally {
            go.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void databaseFailureIsNotReportedAsContentionOrSuccessfulCleanup() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TABLE index_build_claims");
        }
        Instant now = Instant.now();
        assertThatThrownBy(() -> claims.tryClaim("index", 0, "attempt", now)).isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> claims.heartbeat("index", 0, "attempt", now)).isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> claims.release("index", 0, "attempt")).isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> claims.expireBefore(now)).isInstanceOf(MetastoreException.class);
    }

}
