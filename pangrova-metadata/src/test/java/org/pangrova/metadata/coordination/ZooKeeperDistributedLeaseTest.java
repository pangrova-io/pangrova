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
package org.pangrova.metadata.coordination;

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
        assertThat(lease.tryAcquire("ha-leader", "A", 1, Duration.ofSeconds(30))).isTrue();
        assertThat(lease.tryAcquire("ha-leader", "B", 1, Duration.ofSeconds(30)))
                .as("a second holder must be excluded while A's lease is active").isFalse();
        assertThat(lease.tryAcquire("ha-leader", "A", 1, Duration.ofSeconds(30)))
                .as("re-entrant acquire by the current holder succeeds").isTrue();
        Optional<LeaseEntry> holder = lease.currentHolder("ha-leader");
        assertThat(holder).isPresent();
        assertThat(holder.get().holderId()).isEqualTo("A");
        assertThat(holder.get().epoch()).isEqualTo(1);
    }

    @Test
    void renewOnlyByOwnerThenReleaseHandsOff() {
        assertThat(lease.tryAcquire("k", "A", 1, Duration.ofSeconds(30))).isTrue();
        assertThat(lease.tryRenew("k", "A", Duration.ofSeconds(30))).isTrue();
        assertThat(lease.tryRenew("k", "B", Duration.ofSeconds(30)))
                .as("a non-owner cannot renew").isFalse();
        assertThat(lease.release("k", "B")).as("a non-owner cannot release").isFalse();
        assertThat(lease.release("k", "A")).isTrue();
        assertThat(lease.tryAcquire("k", "B", 1, Duration.ofSeconds(30)))
                .as("after release the lease is free to acquire").isTrue();
    }

    @Test
    void higherEpochFencesStaleHolder() {
        assertThat(lease.tryAcquire("k", "old-leader", 1, Duration.ofSeconds(30))).isTrue();
        assertThat(lease.tryAcquire("k", "new-leader", 2, Duration.ofSeconds(30)))
                .as("a strictly higher epoch fences the stale holder").isTrue();
        assertThat(lease.currentHolder("k").orElseThrow().holderId()).isEqualTo("new-leader");
        assertThat(lease.tryAcquire("k", "old-leader", 1, Duration.ofSeconds(30)))
                .as("a lower epoch cannot reclaim after being fenced").isFalse();
    }

    @Test
    void expiredLeaseIsTakenOverUsingServerClock() throws Exception {
        assertThat(lease.tryAcquire("k", "A", 1, Duration.ofMillis(300))).isTrue();
        Thread.sleep(700); // exceed the TTL; expiry is judged by the ZK server clock
        assertThat(lease.currentHolder("k")).as("an expired lease has no active holder").isEmpty();
        assertThat(lease.tryAcquire("k", "B", 1, Duration.ofSeconds(30)))
                .as("an expired lease can be taken over by a new holder").isTrue();
        assertThat(lease.currentHolder("k").orElseThrow().holderId()).isEqualTo("B");
    }

    @Test
    void expireStaleReapsOnlyLapsedLeases() throws Exception {
        assertThat(lease.tryAcquire("k-short", "A", 1, Duration.ofMillis(200))).isTrue();
        assertThat(lease.tryAcquire("k-long", "A", 1, Duration.ofSeconds(30))).isTrue();
        Thread.sleep(600);
        int removed = lease.expireStale();
        assertThat(removed).as("only the lapsed lease is reaped").isEqualTo(1);
        assertThat(lease.currentHolder("k-long")).as("the active lease survives").isPresent();
    }

    @Test
    void providerBuildsWorkingLeaseFromConnectString() {
        // A-HA-01 wiring: the provider builds + owns a Curator client from a connect string and
        // exposes a working quorum lease — the exact path the coordinator bootstrap uses to select
        // the ZooKeeper backend.
        try (ZooKeeperLeaseProvider provider =
                     new ZooKeeperLeaseProvider("127.0.0.1:" + port, "/pangrova/provider-leases")) {
            org.pangrova.spi.coordination.DistributedLease l = provider.lease();
            assertThat(l.tryAcquire("k", "A", 1, Duration.ofSeconds(30))).isTrue();
            assertThat(l.tryAcquire("k", "B", 1, Duration.ofSeconds(30)))
                    .as("provider lease enforces mutual exclusion").isFalse();
            assertThat(l.currentHolder("k").orElseThrow().holderId()).isEqualTo("A");
        }
    }
}
