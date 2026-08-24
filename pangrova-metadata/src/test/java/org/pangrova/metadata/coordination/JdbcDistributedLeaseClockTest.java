/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.metadata.coordination;

import org.pangrova.metadata.store.EmbeddedDataSource;
import org.pangrova.spi.coordination.LeaseEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * H2-backed unit test for A-LEASE-01 / A-HA-01: {@link JdbcDistributedLease} must anchor every
 * lease-expiry decision on the DATABASE server clock ({@code CURRENT_TIMESTAMP}), not the caller's
 * JVM clock. Otherwise coordinator clock skew or a GC pause can make two nodes disagree about
 * whether a lease has expired, opening a double-active-leader (split-brain) window.
 *
 * <p>Runs against in-memory H2 in PostgreSQL-compatibility mode (no Docker). The full PostgreSQL
 * contract is covered by {@code JdbcDistributedLeaseIntegrationTest}. These tests exercise the
 * DB-clock timing path end-to-end (acquire / renew / query / expire) with no reliance on the
 * caller's {@code Instant.now()}.
 */
class JdbcDistributedLeaseClockTest {

    private EmbeddedDataSource dataSource;
    private JdbcDistributedLease lease;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = new EmbeddedDataSource(
                "jdbc:h2:mem:lease_clock_" + System.nanoTime()
                        + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                "sa", "");
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS distributed_lease (
                        lease_key   VARCHAR(255) PRIMARY KEY,
                        holder_id   VARCHAR(255) NOT NULL,
                        epoch       BIGINT       NOT NULL DEFAULT 0,
                        acquired_at TIMESTAMP(3) NOT NULL,
                        expires_at  TIMESTAMP(3) NOT NULL,
                        version     BIGINT       NOT NULL DEFAULT 0
                    )""");
        }
        lease = new JdbcDistributedLease(dataSource);
    }

    @AfterEach
    void tearDown() {
        dataSource.close();
    }

    @Test
    void acquireRenewAndQueryOperateAgainstDbClock() {
        assertThat(lease.tryAcquire("k", "A", 1L, Duration.ofSeconds(30))).isTrue();
        // A holds an unexpired, non-fenced lease — B (same epoch) must be rejected.
        assertThat(lease.tryAcquire("k", "B", 1L, Duration.ofSeconds(30))).isFalse();

        Optional<LeaseEntry> holder = lease.currentHolder("k");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("A");

        // Renewal validity is judged against the DB clock (expires_at >= CURRENT_TIMESTAMP).
        assertThat(lease.tryRenew("k", "A", Duration.ofSeconds(30))).isTrue();
    }

    @Test
    void expiryIsJudgedByDatabaseClockAllowingTakeover() throws InterruptedException {
        // Very short lease: expiry is evaluated against the DB's CURRENT_TIMESTAMP, not the caller.
        assertThat(lease.tryAcquire("k", "A", 1L, Duration.ofMillis(50))).isTrue();
        Thread.sleep(200);

        // Past expiry per the DB clock: no current holder, and a new node can take over.
        assertThat(lease.currentHolder("k")).isEmpty();
        assertThat(lease.tryAcquire("k", "B", 2L, Duration.ofSeconds(30))).isTrue();
        assertThat(lease.currentHolder("k").orElseThrow().holderId()).isEqualTo("B");
    }

    @Test
    void expireStaleRemovesLeasesPastDatabaseClock() throws InterruptedException {
        assertThat(lease.tryAcquire("k", "A", 1L, Duration.ofMillis(50))).isTrue();
        Thread.sleep(200);

        // expireStale compares expires_at < CURRENT_TIMESTAMP entirely inside the DB.
        assertThat(lease.expireStale()).isEqualTo(1);
        assertThat(lease.currentHolder("k")).isEmpty();
    }
}
