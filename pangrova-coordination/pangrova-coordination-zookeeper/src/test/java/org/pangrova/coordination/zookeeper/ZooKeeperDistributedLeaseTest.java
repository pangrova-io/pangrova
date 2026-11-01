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
package org.pangrova.coordination.zookeeper;

import org.pangrova.spi.coordination.LeaseEntry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-HA-01: verifies {@link ZooKeeperDistributedLease} against a REAL ZooKeeper, started in-JVM via
 * {@link ZooKeeperServer} + {@link ServerCnxnFactory} (the mechanism {@code curator-test}'s
 * TestingServer wraps). No mocks — the lease runs against a genuine ZK data tree, so the version-CAS
 * mutual exclusion, epoch fencing, server-clock expiry, and handoff semantics are exercised for real.
 */
class ZooKeeperDistributedLeaseTest {

    private ServerCnxnFactory cnxnFactory;
    private ZooKeeperServer zkServer;
    private CuratorFramework client;
    private ZooKeeperDistributedLease lease;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");
        File dataDir = Files.createTempDirectory("zk-lease-test").toFile();
        zkServer = new ZooKeeperServer(dataDir, dataDir, 2000);
        cnxnFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
        cnxnFactory.startup(zkServer);
        int port = cnxnFactory.getLocalPort();
        this.port = port;
        client = CuratorFrameworkFactory.newClient("127.0.0.1:" + port, new RetryOneTime(100));
        client.start();
        assertThat(client.blockUntilConnected(10, TimeUnit.SECONDS)).isTrue();
        lease = new ZooKeeperDistributedLease(client, "/pangrova/leases");
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    @Test
    void acquireGivesMutualExclusionAndReentrancy() {
        LeaseEntry first = lease.tryAcquire(
                "ha-leader", "A", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease.tryAcquire("ha-leader", "B", Duration.ofSeconds(30)))
                .as("a second holder must be excluded while A's lease is active").isEmpty();
        assertThat(lease.tryAcquire("ha-leader", "A", Duration.ofSeconds(30)))
                .contains(first);
        Optional<LeaseEntry> holder = lease.currentHolder("ha-leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("A");
        assertThat(holder.get().epoch()).isEqualTo(1);
    }

    @Test
    void renewOnlyByOwnerThenReleaseHandsOff() {
        LeaseEntry held = lease.tryAcquire(
                "k", "A", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease.tryRenew("k", "A", held.epoch(), Duration.ofSeconds(30))).isTrue();
        assertThat(lease.tryRenew("k", "B", held.epoch(), Duration.ofSeconds(30)))
                .as("a non-owner cannot renew").isFalse();
        assertThat(lease.release("k", "B", held.epoch()))
                .as("a non-owner cannot release").isFalse();
        assertThat(lease.release("k", "A", held.epoch())).isTrue();
        LeaseEntry next = lease.tryAcquire(
                "k", "B", Duration.ofSeconds(30)).orElseThrow();
        assertThat(next.epoch()).isEqualTo(held.epoch() + 1);
        assertThat(lease.tryRenew("k", "A", held.epoch(), Duration.ofSeconds(30)))
                .as("the released token is permanently fenced").isFalse();
    }

    @Test
    void activeHolderCannotBePreemptedByAContender() {
        LeaseEntry held = lease.tryAcquire(
                "k", "old-leader", Duration.ofSeconds(30)).orElseThrow();
        assertThat(lease.tryAcquire("k", "new-leader", Duration.ofSeconds(30)))
                .as("callers cannot invent an epoch to preempt a live holder").isEmpty();
        assertThat(lease.currentHolder("k")).contains(held);
    }

    @Test
    void expiredLeaseIsTakenOverUsingServerClock() throws Exception {
        LeaseEntry first = lease.tryAcquire(
                "k", "A", Duration.ofMillis(300)).orElseThrow();
        Thread.sleep(700); // exceed the TTL; expiry is judged by the ZK server clock
        assertThat(lease.currentHolder("k")).as("an expired lease has no active holder").isEmpty();
        LeaseEntry next = lease.tryAcquire(
                "k", "B", Duration.ofSeconds(30)).orElseThrow();
        assertThat(next.holderId()).isEqualTo("B");
        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void expiryRetainsEpochHighWatermark() throws Exception {
        LeaseEntry first = lease.tryAcquire(
                "k-short", "A", Duration.ofMillis(200)).orElseThrow();
        assertThat(lease.tryAcquire("k-long", "A", Duration.ofSeconds(30))).isPresent();
        Thread.sleep(600);
        LeaseEntry next = lease.tryAcquire(
                "k-short", "B", Duration.ofSeconds(30)).orElseThrow();
        assertThat(next.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(lease.currentHolder("k-long")).as("the active lease survives").isPresent();
    }

    @Test
    void compositeLeaseKeysHaveCollisionFreeZooKeeperPaths() {
        LeaseEntry slashKey = lease.tryAcquire(
                "artifact/a_b", "A", Duration.ofSeconds(30)).orElseThrow();
        LeaseEntry underscoreKey = lease.tryAcquire(
                "artifact_a_b", "B", Duration.ofSeconds(30)).orElseThrow();

        assertThat(slashKey.epoch()).isEqualTo(1L);
        assertThat(underscoreKey.epoch()).isEqualTo(1L);
        assertThat(lease.currentHolder("artifact/a_b").orElseThrow().holderId())
                .isEqualTo("A");
        assertThat(lease.currentHolder("artifact_a_b").orElseThrow().holderId())
                .isEqualTo("B");
    }

    @Test
    void providerBuildsWorkingLeaseFromConnectString() {
        // A-HA-01 wiring: the provider builds + owns a Curator client from a connect string and
        // exposes a working quorum lease — the exact path the coordinator bootstrap uses to select
        // the ZooKeeper backend.
        try (ZooKeeperLeaseProvider provider =
                     new ZooKeeperLeaseProvider("127.0.0.1:" + port, "/pangrova/provider-leases")) {
            org.pangrova.persistence.api.coordination.DistributedLease l = provider.lease();
            assertThat(l.tryAcquire("k", "A", Duration.ofSeconds(30))).isPresent();
            assertThat(l.tryAcquire("k", "B", Duration.ofSeconds(30)))
                    .as("provider lease enforces mutual exclusion").isEmpty();
            assertThat(l.currentHolder("k").orElseThrow().holderId()).isEqualTo("A");
        }
    }
}
