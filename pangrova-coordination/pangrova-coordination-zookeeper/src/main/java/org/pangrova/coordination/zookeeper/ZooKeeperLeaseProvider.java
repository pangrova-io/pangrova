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

import org.pangrova.persistence.api.coordination.CoordinationProvider;
import org.pangrova.persistence.api.coordination.CoordinatorRegistry;
import org.pangrova.persistence.api.coordination.DistributedLease;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Builds and owns the {@link CuratorFramework} client backing a {@link ZooKeeperDistributedLease}
 * (A-HA-01). Encapsulating Curator here lets callers (e.g. the coordinator bootstrap) depend only on
 * the {@link CoordinationProvider} contract rather than on Curator/ZooKeeper types directly, and gives a
 * single {@link #close()} hook to release the ZooKeeper connection on coordinator shutdown.
 *
 * <p>The constructor starts the client and blocks until the ensemble is reachable (or fails fast on
 * timeout), so a returned provider is ready to serve lease operations against a live quorum.</p>
 */
public final class ZooKeeperLeaseProvider implements CoordinationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaseProvider.class);

    /** Default ZooKeeper session timeout (seconds) used by the two-argument constructor. */
    private static final long DEFAULT_SESSION_TIMEOUT_SECONDS = 15;
    /** Default initial-connection timeout (seconds) used by the two-argument constructor. */
    private static final long DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final Duration DEFAULT_SESSION_TIMEOUT =
            Duration.ofSeconds(DEFAULT_SESSION_TIMEOUT_SECONDS);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT =
            Duration.ofSeconds(DEFAULT_CONNECTION_TIMEOUT_SECONDS);

    private final CuratorFramework client;
    private final ZooKeeperDistributedLease lease;
    private final ZooKeeperCoordinatorRegistry registry;

    public ZooKeeperLeaseProvider(String connectString, String basePath) {
        this(connectString, basePath, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * @param connectString     ZooKeeper connect string (host:port,...)
     * @param basePath          parent znode under which lease nodes are stored
     * @param sessionTimeout    ZooKeeper session timeout
     * @param connectionTimeout initial-connection timeout (also the blockUntilConnected budget)
     */
    public ZooKeeperLeaseProvider(String connectString, String basePath,
                                  Duration sessionTimeout, Duration connectionTimeout) {
        Objects.requireNonNull(connectString, "connectString must not be null");
        Objects.requireNonNull(basePath, "basePath must not be null");
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs((int) sessionTimeout.toMillis())
                .connectionTimeoutMs((int) connectionTimeout.toMillis())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.client.start();
        try {
            if (!client.blockUntilConnected((int) connectionTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                client.close();
                throw new IllegalStateException(
                        "Timed out connecting to ZooKeeper for distributed lease: " + connectString);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            client.close();
            throw new IllegalStateException("Interrupted connecting to ZooKeeper: " + connectString, e);
        }
        this.lease = new ZooKeeperDistributedLease(client, basePath);
        this.registry = new ZooKeeperCoordinatorRegistry(client, siblingPath(basePath, "coordinators"));
        LOG.info("event=distributed_lease_configured backend=zookeeper connectString={} basePath={}",
                connectString, basePath);
    }

    /** The quorum-backed lease. Returned as the API type so callers stay decoupled from Curator. */
    @Override
    public DistributedLease lease() {
        return lease;
    }

    /** The quorum-backed coordinator registry, sharing this provider's single ZooKeeper client. */
    @Override
    public CoordinatorRegistry registry() {
        return registry;
    }

    /** Resolves a sibling znode of {@code basePath}, preserving its parent path. */
    private static String siblingPath(String basePath, String sibling) {
        int slash = basePath.lastIndexOf('/');
        String parent = slash > 0 ? basePath.substring(0, slash) : "";
        return parent + "/" + sibling;
    }

    @Override
    public void close() {
        client.close();
        LOG.info("event=zk_lease_provider_closed");
    }
}
