/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.streaming.maintenance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RetentionPolicyManagerTest {

    private RecordingMaintenanceSpi spi;
    private RetentionPolicyManager manager;

    @BeforeEach
    void setUp() {
        spi = new RecordingMaintenanceSpi();
        manager = new RetentionPolicyManager(spi);
    }

    @Test
    void enforceRetention_noPolicy_returnsNullResult() {
        SnapshotExpirationResult result = manager.enforceRetention("unknown_table").join();
        assertThat(result).isNull();
        assertThat(spi.expireCalls.get()).isZero();
    }

    @Test
    void enforceRetention_withPolicy_callsExpireSnapshotsAsync() {
        Duration retention = Duration.ofDays(7);
        manager.setPolicy("orders", new RetentionPolicy(retention, "created_at", RetentionAction.DELETE));

        SnapshotExpirationResult result = manager.enforceRetention("orders").join();

        assertThat(result).isNotNull();
        assertThat(result.tableName()).isEqualTo("orders");
        assertThat(result.snapshotsExpired()).isEqualTo(3);
        assertThat(spi.expireCalls.get()).isEqualTo(1);
        assertThat(spi.lastExpireTable).isEqualTo("orders");
        assertThat(spi.lastExpireDuration).isEqualTo(retention);
    }

    @Test
    void enforceRetention_isIdempotent_multipleCallsSameTable() {
        manager.setPolicy("events", new RetentionPolicy(
            Duration.ofDays(30), "event_time", RetentionAction.ARCHIVE));

        manager.enforceRetention("events").join();
        manager.enforceRetention("events").join();

        assertThat(spi.expireCalls.get()).isEqualTo(2);
    }

    @Test
    void enforceRetention_spiFailure_completesExceptionally() {
        spi.failOnExpire = true;
        manager.setPolicy("broken", new RetentionPolicy(
            Duration.ofDays(1), "ts", RetentionAction.DELETE));

        CompletableFuture<SnapshotExpirationResult> future = manager.enforceRetention("broken");

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    void enforceAllRetention_noPolicies_completesImmediately() {
        manager.enforceAllRetention().join();
        assertThat(spi.expireCalls.get()).isZero();
    }

    @Test
    void enforceAllRetention_multipleTables_enforcesAll() {
        manager.setPolicy("t1", new RetentionPolicy(
            Duration.ofDays(7), "col1", RetentionAction.DELETE));
        manager.setPolicy("t2", new RetentionPolicy(
            Duration.ofDays(14), "col2", RetentionAction.ARCHIVE));

        manager.enforceAllRetention().join();

        assertThat(spi.expireCalls.get()).isEqualTo(2);
    }

    @Test
    void removePolicy_thenEnforce_skipsTable() {
        manager.setPolicy("temp", new RetentionPolicy(
            Duration.ofDays(1), "ts", RetentionAction.DELETE));
        manager.removePolicy("temp");

        SnapshotExpirationResult result = manager.enforceRetention("temp").join();

        assertThat(result).isNull();
        assertThat(spi.expireCalls.get()).isZero();
    }

    @Test
    void getTablesWithRetentionPolicy_returnsRegisteredTables() {
        manager.setPolicy("a", new RetentionPolicy(
            Duration.ofDays(1), "ts", RetentionAction.DELETE));
        manager.setPolicy("b", new RetentionPolicy(
            Duration.ofDays(2), "ts", RetentionAction.ARCHIVE));

        assertThat(manager.getTablesWithRetentionPolicy()).containsExactlyInAnyOrder("a", "b");
    }

    /**
     * In-memory SPI that records calls and returns deterministic results.
     */
    private static class RecordingMaintenanceSpi implements TableMaintenanceSpi {

        final AtomicInteger expireCalls = new AtomicInteger();
        final ConcurrentHashMap<String, MaintenanceStatus> statuses = new ConcurrentHashMap<>();
        volatile String lastExpireTable;
        volatile Duration lastExpireDuration;
        volatile boolean failOnExpire;

        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            return CompletableFuture.completedFuture(
                new CompactionResult(tableName, 0, 0, 0L));
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(
                String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(
                new OrphanCleanupResult(tableName, 0, 0L));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(
                String tableName, Duration olderThan) {
            expireCalls.incrementAndGet();
            lastExpireTable = tableName;
            lastExpireDuration = olderThan;
            if (failOnExpire) {
                return CompletableFuture.failedFuture(
                    new RuntimeException("expiration failed"));
            }
            return CompletableFuture.completedFuture(
                new SnapshotExpirationResult(tableName, 3));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String tableName) {
            return statuses.getOrDefault(tableName, new MaintenanceStatus(
                tableName, 0, 0, 0L, 0, 0, 0L, 0,
                null, null, null));
        }
    }
}
