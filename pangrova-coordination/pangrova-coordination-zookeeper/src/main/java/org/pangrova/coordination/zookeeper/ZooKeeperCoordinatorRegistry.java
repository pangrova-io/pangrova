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

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * ZooKeeper-backed {@link CoordinatorRegistry} (A-HA-01): replicates coordinator membership across a
 * ZooKeeper quorum, so leader discovery and topology survive loss of a minority of nodes — completing
 * the coordination-state migration alongside {@link ZooKeeperDistributedLease} (which handles the
 * leader-election lease).
 *
 * <p>Each coordinator is a persistent znode at {@code <basePath>/<coordinatorId>} whose data encodes
 * the {@link CoordinatorSnapshot}. Single-field mutations (heartbeat, role, invalidation id) use a
 * read-modify-write with ZooKeeper version CAS + bounded retry so concurrent updates don't clobber
 * each other. Staleness mirrors {@code JdbcCoordinatorRegistry}: a coordinator is active while
 * {@code now - lastHeartbeatMs < threshold}.</p>
 *
 * <p>Backend/payload failures and exhausted mutation contention propagate. Only explicit missing
 * records are absence/no-op; an incomplete listing is never returned as a complete topology.
 * The supplied client must already be started and remains owned by its caller.</p>
 */
public final class ZooKeeperCoordinatorRegistry implements CoordinatorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCoordinatorRegistry.class);
    private static final int MAX_CAS_RETRIES = 3;
    /** Default staleness bound for {@link #getLeader()} (which takes no threshold argument): 30 seconds. */
    private static final long DEFAULT_LEADER_STALE_MS = 30_000L;

    private final CuratorFramework client;
    private final String basePath;

    public ZooKeeperCoordinatorRegistry(CuratorFramework client, String basePath) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.basePath = normalizeBase(Objects.requireNonNull(basePath, "basePath must not be null"));
        ensureBase();
    }

    @Override
    public void register(CoordinatorSnapshot coordinator) {
        Objects.requireNonNull(coordinator, "coordinator must not be null");
        String path = coordinatorPath(coordinator.coordinatorId());
        byte[] data = serialize(coordinator);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                try {
                    client.create().creatingParentsIfNeeded().forPath(path, data);
                } catch (KeeperException.NodeExistsException exists) {
                    Stat stat = new Stat();
                    CoordinatorSnapshot current = parse(client.getData().storingStatIn(stat).forPath(path));
                    byte[] registration = serialize(coordinator.withInvalidationProgress(
                            current.lastProcessedInvalidationId()));
                    client.setData().withVersion(stat.getVersion()).forPath(path, registration);
                }
                LOG.info("event=coordinator_registered coordinatorId={} role={} epoch={}",
                        coordinator.coordinatorId(), coordinator.role(), coordinator.epoch());
                return;
            } catch (KeeperException.BadVersionException | KeeperException.NoNodeException concurrent) {
                // Another process updated the watermark or removed the registration. Read the new version.
            } catch (Exception e) {
                throw ZooKeeperFailures.storageFailure("register", coordinator.coordinatorId(), e);
            }
        }
        throw ZooKeeperFailures.storageFailure("register contention exhausted", coordinator.coordinatorId(),
                new java.util.ConcurrentModificationException("CAS attempts=" + MAX_CAS_RETRIES));
    }

    @Override
    public void heartbeat(String coordinatorId) {
        long now = System.currentTimeMillis();
        mutate(coordinatorId, s -> new CoordinatorSnapshot(s.coordinatorId(), s.host(), s.httpPort(),
                s.role(), s.epoch(), now, s.registeredAtMs(), s.lastProcessedInvalidationId()));
    }

    @Override
    public void updateRole(String coordinatorId, CoordinatorRole role) {
        Objects.requireNonNull(role, "role must not be null");
        mutate(coordinatorId, s -> new CoordinatorSnapshot(s.coordinatorId(), s.host(), s.httpPort(),
                role.name(), s.epoch(), s.lastHeartbeatMs(), s.registeredAtMs(),
                s.lastProcessedInvalidationId()));
        LOG.info("event=coordinator_role_updated coordinatorId={} role={}", coordinatorId, role);
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long lastProcessedId) {
        mutate(coordinatorId, snapshot -> snapshot.withInvalidationProgress(lastProcessedId));
    }

    @Override
    public void unregister(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try {
            client.delete().forPath(coordinatorPath(coordinatorId));
            LOG.info("event=coordinator_unregistered coordinatorId={}", coordinatorId);
        } catch (KeeperException.NoNodeException absent) {
            // idempotent no-op
        } catch (Exception e) {
            LOG.error("event=coordinator_unregister_error coordinatorId={} error={}",
                    coordinatorId, e.toString(), e);
            throw ZooKeeperFailures.storageFailure("unregister", coordinatorId, e);
        }
    }

    @Override
    public Optional<CoordinatorSnapshot> getLeader() {
        long now = System.currentTimeMillis();
        for (CoordinatorSnapshot s : readAll()) {
            if ("LEADER".equals(s.role()) && now - s.lastHeartbeatMs() < DEFAULT_LEADER_STALE_MS) {
                return Optional.of(s);
            }
        }
        return Optional.empty();
    }

    @Override
    public List<CoordinatorSnapshot> getAllCoordinators(Duration staleThreshold) {
        Objects.requireNonNull(staleThreshold, "staleThreshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = staleThreshold.toMillis();
        List<CoordinatorSnapshot> active = new ArrayList<>();
        for (CoordinatorSnapshot s : readAll()) {
            if (now - s.lastHeartbeatMs() < thresholdMs) {
                active.add(s);
            }
        }
        return List.copyOf(active);
    }

    @Override
    public Optional<CoordinatorSnapshot> getCoordinator(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try {
            return Optional.of(parse(client.getData().forPath(coordinatorPath(coordinatorId))));
        } catch (KeeperException.NoNodeException absent) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.error("event=coordinator_lookup_error coordinatorId={} error={}",
                    coordinatorId, e.toString(), e);
            throw ZooKeeperFailures.storageFailure("lookup", coordinatorId, e);
        }
    }

    @Override
    public int expireStale(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        long now = System.currentTimeMillis();
        long thresholdMs = threshold.toMillis();
        int removed = 0;
        try {
            for (String child : client.getChildren().forPath(basePath)) {
                if (expireIfStale(basePath + "/" + child, now, thresholdMs)) {
                    removed++;
                }
            }
        } catch (KeeperException.NoNodeException noBase) {
            throw ZooKeeperFailures.storageFailure("missing registry root", basePath, noBase);
        } catch (Exception e) {
            LOG.error("event=coordinator_expire_stale_error error={}", e.toString(), e);
            throw ZooKeeperFailures.storageFailure("expire", basePath, e);
        }
        if (removed > 0) {
            LOG.info("event=coordinator_stale_expired count={}", removed);
        }
        return removed;
    }

    /**
     * Deletes one coordinator record when its last heartbeat is older than the threshold.
     *
     * <p>Extracted from {@link #expireStale(Duration)} to keep that loop within the
     * project indent-depth budget. Concurrent heartbeats and deletions are benign:
     * a version mismatch or a vanished node both mean another actor already handled
     * the record, so the node is left alone and reported as not expired.</p>
     *
     * @return {@code true} only when this call actually deleted the node
     */
    private boolean expireIfStale(String path, long now, long thresholdMs) throws Exception {
        try {
            Stat stat = new Stat();
            CoordinatorSnapshot snapshot = parse(client.getData().storingStatIn(stat).forPath(path));
            if (now - snapshot.lastHeartbeatMs() < thresholdMs) {
                return false;
            }
            client.delete().withVersion(stat.getVersion()).forPath(path);
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException concurrent) {
            // heartbeated, or removed by another coordinator, between read and delete
            return false;
        }
    }

    /** Read-modify-write a single coordinator record with version CAS + bounded retry. */
    private void mutate(String coordinatorId, UnaryOperator<CoordinatorSnapshot> op) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        String path = coordinatorPath(coordinatorId);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                Stat stat = new Stat();
                CoordinatorSnapshot current = parse(client.getData().storingStatIn(stat).forPath(path));
                client.setData().withVersion(stat.getVersion()).forPath(path, serialize(op.apply(current)));
                return;
            } catch (KeeperException.NoNodeException absent) {
                return; // not registered — implementation-defined no-op (mirror: silently ignore)
            } catch (KeeperException.BadVersionException race) {
                // concurrent update — retry
            } catch (Exception e) {
                LOG.error("event=coordinator_mutate_error coordinatorId={} error={}",
                        coordinatorId, e.toString(), e);
                throw ZooKeeperFailures.storageFailure("mutate", coordinatorId, e);
            }
        }
        throw ZooKeeperFailures.storageFailure("mutate contention exhausted", coordinatorId,
                new java.util.ConcurrentModificationException("CAS attempts=" + MAX_CAS_RETRIES));
    }

    private List<CoordinatorSnapshot> readAll() {
        List<CoordinatorSnapshot> all = new ArrayList<>();
        try {
            for (String child : client.getChildren().forPath(basePath)) {
                try {
                    all.add(parse(client.getData().forPath(basePath + "/" + child)));
                } catch (KeeperException.NoNodeException vanished) {
                    // removed concurrently
                }
            }
        } catch (KeeperException.NoNodeException noBase) {
            throw ZooKeeperFailures.storageFailure("missing registry root", basePath, noBase);
        } catch (Exception e) {
            LOG.error("event=coordinator_read_all_error error={}", e.toString(), e);
            throw ZooKeeperFailures.storageFailure("list", basePath, e);
        }
        return all;
    }

    private void ensureBase() {
        try {
            if (client.checkExists().forPath(basePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(basePath);
            }
        } catch (KeeperException.NodeExistsException concurrent) {
            // created concurrently
        } catch (Exception e) {
            throw ZooKeeperFailures.storageFailure("initialize registry", basePath, e);
        }
    }

    private String coordinatorPath(String coordinatorId) {
        return basePath + "/" + coordinatorId.replace('/', '_');
    }

    private static String normalizeBase(String base) {
        String b = base.startsWith("/") ? base : "/" + base;
        while (b.length() > 1 && b.endsWith("/")) {
            b = b.substring(0, b.length() - 1);
        }
        return b;
    }

    // Wire format (newline-delimited): coordinatorId, host, httpPort, role, epoch,
    // lastHeartbeatMs, registeredAtMs, lastProcessedInvalidationId
    private static byte[] serialize(CoordinatorSnapshot s) {
        String data = String.join("\n",
                s.coordinatorId(), s.host(), Integer.toString(s.httpPort()), s.role(),
                Long.toString(s.epoch()), Long.toString(s.lastHeartbeatMs()),
                Long.toString(s.registeredAtMs()), Long.toString(s.lastProcessedInvalidationId()));
        return data.getBytes(StandardCharsets.UTF_8);
    }

    private static CoordinatorSnapshot parse(byte[] data) {
        String[] p = new String(data, StandardCharsets.UTF_8).split("\n", -1);
        if (p.length < 8) {
            throw new IllegalStateException("Corrupt coordinator registry payload");
        }
        return new CoordinatorSnapshot(p[0], p[1], Integer.parseInt(p[2]), p[3],
                Long.parseLong(p[4]), Long.parseLong(p[5]), Long.parseLong(p[6]), Long.parseLong(p[7]));
    }
}
