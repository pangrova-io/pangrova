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

import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.LeaseEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PostgreSQL test for A-LEASE-01 / A-HA-01: {@link JdbcDistributedLease} must anchor every
 * lease-expiry decision on the DATABASE server clock ({@code clock_timestamp()}), not the caller's
 * JVM clock. Otherwise coordinator clock skew or a GC pause can make two nodes disagree about
 * whether a lease has expired, opening a double-active-leader (split-brain) window.
 *
 * <p>These tests exercise the production DB-clock timing path end-to-end (acquire / renew / query /
 * takeover) with no reliance on the caller's {@code Instant.now()}.
 */
class JdbcDistributedLeaseClockTest {

    private JdbcDistributedLease lease;

    @BeforeEach
    void setUp() throws Exception {
        DataSource dataSource = SharedPostgresContainer.migratedDataSource("lease_clock");
        lease = new JdbcDistributedLease(dataSource);
    }

    @Test
    void acquireRenewAndQueryOperateAgainstDbClock() {
        LeaseEntry acquired = lease.tryAcquire(
                "k", "A", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease.tryAcquire("k", "B", Duration.ofSeconds(30))).isEmpty();

        Optional<LeaseEntry> holder = lease.currentHolder("k");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("A");

        // Renewal validity is judged against the DB clock (expires_at > clock_timestamp()).
        assertThat(lease.tryRenew(
                "k", "A", acquired.epoch(), Duration.ofSeconds(30))).isTrue();
    }

    @Test
    void expiryIsJudgedByDatabaseClockAllowingTakeover() throws InterruptedException {
        // Very short lease: expiry is evaluated against the DB's clock_timestamp(), not the caller.
        LeaseEntry first = lease.tryAcquire(
                "k", "A", Duration.ofMillis(50)).orElseThrow();
        Thread.sleep(200);

        // Past expiry per the DB clock: no current holder, and a new node can take over.
        assertThat(lease.currentHolder("k")).isEmpty();
        LeaseEntry second = lease.tryAcquire(
                "k", "B", Duration.ofSeconds(30)).orElseThrow();
        assertThat(second.holderId()).isEqualTo("B");
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void staleEpochCannotRenewOrReleaseReplacement() throws InterruptedException {
        LeaseEntry first = lease.tryAcquire(
                "k", "A", Duration.ofMillis(50)).orElseThrow();
        Thread.sleep(200);
        LeaseEntry second = lease.tryAcquire(
                "k", "B", Duration.ofSeconds(30)).orElseThrow();

        assertThat(lease.tryRenew(
                "k", "A", first.epoch(), Duration.ofSeconds(30))).isFalse();
        assertThat(lease.release("k", "A", first.epoch())).isFalse();
        assertThat(lease.currentHolder("k")).contains(second);
    }
}
