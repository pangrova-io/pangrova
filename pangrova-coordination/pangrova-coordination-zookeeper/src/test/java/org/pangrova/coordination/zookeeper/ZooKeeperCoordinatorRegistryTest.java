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

import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-HA-01: verifies {@link ZooKeeperCoordinatorRegistry} against a REAL in-JVM ZooKeeper (no mocks),
 * covering registration/upsert, heartbeat liveness, role transition + leader discovery, staleness
 * filtering, unregister, and stale expiry.
 */
class ZooKeeperCoordinatorRegistryTest {

    private ServerCnxnFactory cnxnFactory;
    private ZooKeeperServer zkServer;
    private CuratorFramework client;
    private ZooKeeperCoordinatorRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("zookeeper.admin.enableServer", "false");
        File dataDir = Files.createTempDirectory("zk-registry-test").toFile();
        zkServer = new ZooKeeperServer(dataDir, dataDir, 2000);
        cnxnFactory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
        cnxnFactory.startup(zkServer);
        client = CuratorFrameworkFactory.newClient("127.0.0.1:" + cnxnFactory.getLocalPort(), new RetryOneTime(100));
        client.start();
        assertThat(client.blockUntilConnected(10, TimeUnit.SECONDS)).isTrue();
        registry = new ZooKeeperCoordinatorRegistry(client, "/pangrova/registry");
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

    private static CoordinatorSnapshot snap(String id, String role, long heartbeatMs) {
        return new CoordinatorSnapshot(id, "host-" + id, 8080, role, 1L, heartbeatMs, heartbeatMs, 0L, "instance-" + id);
    }

    @Test
    void reregisterUpdatesMembershipAndRetainsTheLastReportedWatermark() {
        long now = System.currentTimeMillis();
        registry.register(new CoordinatorSnapshot("self", "old-host", 8080, "STANDBY", 1, now, now));
        registry.updateLastProcessedInvalidationId("self", 181);
        registry.register(new CoordinatorSnapshot("self", "new-host", 9090, "STANDBY", 2, now + 1, now + 1));
        var current = registry.getCoordinator("self").orElseThrow();
        assertThat(current.host()).isEqualTo("new-host");
        assertThat(current.lastProcessedInvalidationId()).isEqualTo(181);
    }

    @Test
    void registerAndLookupRoundTrip() {
        registry.register(snap("A", "STANDBY", System.currentTimeMillis()));
        CoordinatorSnapshot got = registry.getCoordinator("A").orElseThrow();
        assertThat(got.coordinatorId()).isEqualTo("A");
        assertThat(got.role()).isEqualTo("STANDBY");
        assertThat(got.host()).isEqualTo("host-A");
        assertThat(got.httpPort()).isEqualTo(8080);
    }

    @Test
    void registerUpsertsLatestSnapshot() {
        registry.register(snap("A", "STANDBY", System.currentTimeMillis()));
        registry.register(snap("A", "LEADER", System.currentTimeMillis()));
        assertThat(registry.getCoordinator("A").orElseThrow().role()).isEqualTo("LEADER");
    }

    @Test
    void heartbeatRefreshesLiveness() {
        registry.register(snap("A", "STANDBY", System.currentTimeMillis() - 60_000)); // stale
        assertThat(registry.getAllCoordinators(Duration.ofSeconds(5))).isEmpty();
        registry.heartbeat("A");
        assertThat(registry.getAllCoordinators(Duration.ofSeconds(5)))
                .singleElement().satisfies(snapshot -> {
                    assertThat(snapshot.coordinatorId()).isEqualTo("A");
                    assertThat(snapshot.incarnation()).isEqualTo("instance-A");
                });
    }

    @Test
    void updateRoleThenGetLeaderFindsIt() {
        registry.register(snap("A", "STANDBY", System.currentTimeMillis()));
        assertThat(registry.getLeader()).as("no leader while A is STANDBY").isEmpty();
        registry.updateRole("A", CoordinatorRole.LEADER);
        CoordinatorSnapshot leader = registry.getLeader().orElseThrow();
        assertThat(leader.coordinatorId()).isEqualTo("A");
        assertThat(leader.incarnation()).isEqualTo("instance-A");
    }

    @Test
    void getLeaderIgnoresStaleLeader() {
        registry.register(snap("A", "LEADER", System.currentTimeMillis() - 60_000)); // stale heartbeat
        assertThat(registry.getLeader()).as("a stale LEADER is not an active leader").isEmpty();
    }

    @Test
    void getAllCoordinatorsFiltersStale() {
        registry.register(snap("live", "STANDBY", System.currentTimeMillis()));
        registry.register(snap("dead", "STANDBY", System.currentTimeMillis() - 60_000));
        assertThat(registry.getAllCoordinators(Duration.ofSeconds(5)))
                .extracting(CoordinatorSnapshot::coordinatorId).containsExactly("live");
    }

    @Test
    void unregisterRemovesCoordinator() {
        registry.register(snap("A", "STANDBY", System.currentTimeMillis()));
        registry.unregister("A");
        assertThat(registry.getCoordinator("A")).isEmpty();
        registry.unregister("A"); // idempotent — must not throw
    }

    @Test
    void expireStaleReapsOnlyStaleCoordinators() {
        registry.register(snap("live", "STANDBY", System.currentTimeMillis()));
        registry.register(snap("dead", "STANDBY", System.currentTimeMillis() - 60_000));
        assertThat(registry.expireStale(Duration.ofSeconds(5))).isEqualTo(1);
        assertThat(registry.getCoordinator("dead")).isEmpty();
        assertThat(registry.getCoordinator("live")).isPresent();
    }
}
