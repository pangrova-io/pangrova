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

import org.pangrova.spi.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * ZooKeeper-backed {@link DistributedLease} (A-HA-01): removes the single-JDBC coordination SPOF by
 * replicating lease state across a ZooKeeper quorum. When deployed against a 3- or 5-node ensemble,
 * loss of a minority of nodes does not lose coordination — unlike a single database.
 *
 * <p>Semantics mirror {@link JdbcDistributedLease}: time-bounded (TTL) leases with epoch fencing and
 * re-entrant acquisition. Each lease is a persistent znode at {@code <basePath>/<leaseKey>} whose
 * data encodes {@code (epoch, acquiredAtMs, expiresAtMs, holderId)}.</p>
 *
 * <p><b>Mutual exclusion</b> is provided by ZooKeeper's per-znode version: acquire/renew/release
 * mutate the node with {@code withVersion(expected)}, so exactly one of two racing writers succeeds
 * (the loser sees {@link KeeperException.BadVersionException}). The quorum serializes the writes.</p>
 *
 * <p><b>Authoritative clock (A-LEASE-01):</b> every expiry decision anchors on the ZooKeeper leader's
 * clock — sampled via the {@code mtime} the server stamps on a write to a dedicated {@code _clock}
 * znode — rather than each coordinator's local JVM clock. This prevents coordinator clock skew or GC
 * pauses from disagreeing on expiry and opening a double-active-leader (split-brain) window.</p>
 *
 * <p>All operations are <b>fail-closed</b>: any ZooKeeper error results in "not acquired / not
 * renewed / no holder" rather than a false grant. The supplied {@link CuratorFramework} must already
 * be started. Implementations are thread-safe (ZooKeeper serializes at the quorum).</p>
 */
public final class ZooKeeperDistributedLease implements DistributedLease {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperDistributedLease.class);
    private static final byte[] EMPTY = new byte[0];
    private static final String CLOCK_NODE = "_clock";

    private final CuratorFramework client;
    private final String basePath;
    private final String clockPath;

    /**
     * @param client   a STARTED CuratorFramework connected to the ZooKeeper ensemble
     * @param basePath the parent znode under which lease nodes live (e.g. {@code /pangrova/leases})
     */
    public ZooKeeperDistributedLease(CuratorFramework client, String basePath) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.basePath = normalizeBase(Objects.requireNonNull(basePath, "basePath must not be null"));
        this.clockPath = this.basePath + "/" + CLOCK_NODE;
        ensureClockNode();
    }

    @Override
    public boolean tryAcquire(String leaseKey, String holderId, long epoch, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        String path = leasePath(leaseKey);
        try {
            long now = serverNowMillis();
            long expiresAt = now + duration.toMillis();
            Stat stat = new Stat();
            byte[] data;
            try {
                data = client.getData().storingStatIn(stat).forPath(path);
            } catch (KeeperException.NoNodeException absent) {
                return createLease(path, leaseKey, holderId, epoch, now, expiresAt);
            }
            LeaseData existing = LeaseData.parse(data);
            // Acquire is allowed when the current lease is expired, held by us (re-entrant), or
            // fenced by a strictly higher epoch. Otherwise an active foreign holder blocks us.
            boolean canTake = existing.expiresAtMs() <= now
                    || existing.holderId().equals(holderId)
                    || existing.epoch() < epoch;
            if (!canTake) {
                return false;
            }
            try {
                client.setData().withVersion(stat.getVersion())
                        .forPath(path, LeaseData.serialize(epoch, now, expiresAt, holderId));
                LOG.info("event=zk_lease_acquired leaseKey={} holderId={} epoch={} expiresAt={}",
                        leaseKey, holderId, epoch, Instant.ofEpochMilli(expiresAt));
                return true;
            } catch (KeeperException.BadVersionException race) {
                // A concurrent writer changed the node first — lost the race.
                return false;
            }
        } catch (Exception e) {
            LOG.warn("event=zk_lease_acquire_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString());
            return false;
        }
    }

    private boolean createLease(String path, String leaseKey, String holderId,
                                long epoch, long now, long expiresAt) {
        try {
            client.create().creatingParentsIfNeeded()
                    .forPath(path, LeaseData.serialize(epoch, now, expiresAt, holderId));
            LOG.info("event=zk_lease_acquired leaseKey={} holderId={} epoch={} expiresAt={} note=created",
                    leaseKey, holderId, epoch, Instant.ofEpochMilli(expiresAt));
            return true;
        } catch (KeeperException.NodeExistsException race) {
            // A concurrent acquirer created it first.
            return false;
        } catch (Exception e) {
            LOG.warn("event=zk_lease_create_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString());
            return false;
        }
    }

    @Override
    public boolean tryRenew(String leaseKey, String holderId, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        String path = leasePath(leaseKey);
        try {
            long now = serverNowMillis();
            long expiresAt = now + duration.toMillis();
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(path);
            LeaseData existing = LeaseData.parse(data);
            // Renew only if we still own it AND it has not already lapsed (a lapsed lease must be
            // re-acquired, not renewed, so a new holder is not silently overwritten).
            if (!existing.holderId().equals(holderId) || existing.expiresAtMs() <= now) {
                return false;
            }
            try {
                client.setData().withVersion(stat.getVersion())
                        .forPath(path, LeaseData.serialize(existing.epoch(), existing.acquiredAtMs(),
                                expiresAt, holderId));
                LOG.debug("event=zk_lease_renewed leaseKey={} holderId={} expiresAt={}",
                        leaseKey, holderId, Instant.ofEpochMilli(expiresAt));
                return true;
            } catch (KeeperException.BadVersionException race) {
                return false;
            }
        } catch (KeeperException.NoNodeException absent) {
            return false;
        } catch (Exception e) {
            LOG.warn("event=zk_lease_renew_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString());
            return false;
        }
    }

    @Override
    public boolean release(String leaseKey, String holderId) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        String path = leasePath(leaseKey);
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(path);
            LeaseData existing = LeaseData.parse(data);
            if (!existing.holderId().equals(holderId)) {
                return false;
            }
            try {
                client.delete().withVersion(stat.getVersion()).forPath(path);
                LOG.info("event=zk_lease_released leaseKey={} holderId={}", leaseKey, holderId);
                return true;
            } catch (KeeperException.BadVersionException | KeeperException.NoNodeException race) {
                return false;
            }
        } catch (KeeperException.NoNodeException absent) {
            return false;
        } catch (Exception e) {
            LOG.error("event=zk_lease_release_error leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString(), e);
            return false;
        }
    }

    @Override
    public Optional<LeaseEntry> currentHolder(String leaseKey) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        String path = leasePath(leaseKey);
        try {
            long now = serverNowMillis();
            byte[] data = client.getData().forPath(path);
            LeaseData d = LeaseData.parse(data);
            if (d.expiresAtMs() > now) {
                return Optional.of(new LeaseEntry(leaseKey, d.holderId(), d.epoch(),
                        Instant.ofEpochMilli(d.acquiredAtMs()), Instant.ofEpochMilli(d.expiresAtMs())));
            }
            return Optional.empty();
        } catch (KeeperException.NoNodeException absent) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.error("event=zk_lease_query_error leaseKey={} error={}", leaseKey, e.toString(), e);
            return Optional.empty();
        }
    }

    @Override
    public int expireStale() {
        int removed = 0;
        try {
            long now = serverNowMillis();
            List<String> children = client.getChildren().forPath(basePath);
            for (String child : children) {
                if (CLOCK_NODE.equals(child)) {
                    continue;
                }
                if (expireIfElapsed(basePath + "/" + child, now)) {
                    removed++;
                }
            }
        } catch (KeeperException.NoNodeException noBase) {
            return 0;
        } catch (Exception e) {
            LOG.error("event=zk_lease_expire_stale_error error={}", e.toString(), e);
            return 0;
        }
        if (removed > 0) {
            LOG.info("event=zk_lease_stale_expired count={}", removed);
        }
        return removed;
    }

    /**
     * Deletes one lease znode when its TTL has elapsed against the authoritative clock.
     *
     * <p>Extracted from {@link #expireStale()} to keep that loop within the project
     * indent-depth budget. Concurrent renewal and deletion are both benign: a version
     * mismatch means the holder renewed the lease, and a vanished node means another
     * coordinator already expired it, so the znode is left alone and reported as not
     * expired.</p>
     *
     * @return {@code true} only when this call actually deleted the znode
     */
    private boolean expireIfElapsed(String path, long now) throws Exception {
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(path);
            if (LeaseData.parse(data).expiresAtMs() > now) {
                return false;
            }
            client.delete().withVersion(stat.getVersion()).forPath(path);
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException concurrent) {
            return false;
        }
    }

    /**
     * Samples the ZooKeeper leader's clock (A-LEASE-01): a write to the dedicated {@code _clock}
     * znode is stamped by the server with an {@code mtime}, giving a single authoritative time source
     * shared by every coordinator, independent of local JVM clocks.
     */
    private long serverNowMillis() throws Exception {
        Stat stat = client.setData().forPath(clockPath, EMPTY);
        return stat.getMtime();
    }

    private void ensureClockNode() {
        try {
            if (client.checkExists().forPath(clockPath) == null) {
                client.create().creatingParentsIfNeeded().forPath(clockPath, EMPTY);
            }
        } catch (KeeperException.NodeExistsException concurrent) {
            // Created by another coordinator — fine.
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to initialize ZooKeeper lease clock node at " + clockPath, e);
        }
    }

    private String leasePath(String leaseKey) {
        // znode names cannot contain '/'; encode it so composite keys map to a single child node.
        return basePath + "/" + leaseKey.replace('/', '_');
    }

    private static String normalizeBase(String base) {
        String b = base.startsWith("/") ? base : "/" + base;
        while (b.length() > 1 && b.endsWith("/")) {
            b = b.substring(0, b.length() - 1);
        }
        return b;
    }

    /**
     * Wire format of a lease znode: {@code epoch\nacquiredAtMs\nexpiresAtMs\nholderId}. The holderId
     * is last (captured as the remainder) so it may safely contain any character except the reserved
     * newline separators used by the three leading numeric fields.
     */
    private record LeaseData(long epoch, long acquiredAtMs, long expiresAtMs, String holderId) {

        static byte[] serialize(long epoch, long acquiredAtMs, long expiresAtMs, String holderId) {
            String s = epoch + "\n" + acquiredAtMs + "\n" + expiresAtMs + "\n" + holderId;
            return s.getBytes(StandardCharsets.UTF_8);
        }

        static LeaseData parse(byte[] data) {
            String[] parts = new String(data, StandardCharsets.UTF_8).split("\n", 4);
            if (parts.length < 4) {
                throw new IllegalStateException("Corrupt lease znode payload");
            }
            return new LeaseData(Long.parseLong(parts[0]), Long.parseLong(parts[1]),
                    Long.parseLong(parts[2]), parts[3]);
        }
    }
}
