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

import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * ZooKeeper-backed {@link DistributedLease} (A-HA-01): removes the single-JDBC coordination SPOF by
 * replicating lease state across a ZooKeeper quorum. When deployed against a 3- or 5-node ensemble,
 * loss of a minority of nodes does not lose coordination — unlike a single database.
 *
 * <p>Semantics mirror {@link JdbcDistributedLease}: time-bounded leases with backend-issued fencing
 * epochs. Each lease is a persistent znode at {@code <basePath>/<leaseKey>} whose
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
 * <p>Explicit absence and version contention return empty/false. Backend, clock and payload failures
 * propagate; a failed mutation may have committed, so no success or absence can be inferred.
 * The supplied client must already be started; the caller owns its lifetime.</p>
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
    public Optional<LeaseEntry> tryAcquire(
            String leaseKey, String holderId, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        String path = leasePath(leaseKey);
        try {
            long now = serverNowMillis();
            long expiresAt = Math.addExact(now, duration.toMillis());
            Stat stat = new Stat();
            byte[] data;
            try {
                data = client.getData().storingStatIn(stat).forPath(path);
            } catch (KeeperException.NoNodeException absent) {
                return createLease(path, leaseKey, holderId, now, expiresAt);
            }
            LeaseData existing = LeaseData.parse(data);
            if (existing.expiresAtMs() > now) {
                return existing.holderId().equals(holderId)
                        ? Optional.of(toEntry(leaseKey, existing)) : Optional.empty();
            }
            long nextEpoch = Math.incrementExact(existing.epoch());
            try {
                client.setData().withVersion(stat.getVersion())
                        .forPath(path, LeaseData.serialize(
                                nextEpoch, now, expiresAt, holderId));
                LOG.info("event=zk_lease_acquired leaseKey={} holderId={} epoch={} expiresAt={}",
                        leaseKey, holderId, nextEpoch, Instant.ofEpochMilli(expiresAt));
                return Optional.of(new LeaseEntry(leaseKey, holderId, nextEpoch,
                        Instant.ofEpochMilli(now), Instant.ofEpochMilli(expiresAt)));
            } catch (KeeperException.BadVersionException race) {
                return Optional.empty();
            }
        } catch (Exception e) {
            LOG.warn("event=zk_lease_acquire_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString());
            throw ZooKeeperFailures.storageFailure("acquire", leaseKey, e);
        }
    }

    private Optional<LeaseEntry> createLease(
            String path, String leaseKey, String holderId, long now, long expiresAt) {
        try {
            client.create().creatingParentsIfNeeded()
                    .forPath(path, LeaseData.serialize(1, now, expiresAt, holderId));
            LOG.info("event=zk_lease_acquired leaseKey={} holderId={} epoch={} expiresAt={} note=created",
                    leaseKey, holderId, 1, Instant.ofEpochMilli(expiresAt));
            return Optional.of(new LeaseEntry(leaseKey, holderId, 1,
                    Instant.ofEpochMilli(now), Instant.ofEpochMilli(expiresAt)));
        } catch (KeeperException.NodeExistsException race) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("event=zk_lease_create_failed leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString());
            throw ZooKeeperFailures.storageFailure("create", leaseKey, e);
        }
    }

    @Override
    public boolean tryRenew(
            String leaseKey, String holderId, long epoch, Duration duration) {
        requireRequest(leaseKey, holderId, duration);
        requireEpoch(epoch);
        String path = leasePath(leaseKey);
        try {
            long now = serverNowMillis();
            long expiresAt = Math.addExact(now, duration.toMillis());
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(path);
            LeaseData existing = LeaseData.parse(data);
            // Renew only if we still own it AND it has not already lapsed (a lapsed lease must be
            // re-acquired, not renewed, so a new holder is not silently overwritten).
            if (!existing.holderId().equals(holderId) || existing.epoch() != epoch
                    || existing.expiresAtMs() <= now) {
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
            throw ZooKeeperFailures.storageFailure("renew", leaseKey, e);
        }
    }

    @Override
    public boolean release(String leaseKey, String holderId, long epoch) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        requireEpoch(epoch);
        String path = leasePath(leaseKey);
        try {
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(path);
            LeaseData existing = LeaseData.parse(data);
            long now = serverNowMillis();
            if (!existing.holderId().equals(holderId) || existing.epoch() != epoch
                    || existing.expiresAtMs() <= now) {
                return false;
            }
            try {
                client.setData().withVersion(stat.getVersion()).forPath(path,
                        LeaseData.serialize(existing.epoch(), existing.acquiredAtMs(),
                                now, holderId));
                LOG.info("event=zk_lease_released leaseKey={} holderId={} epoch={}",
                        leaseKey, holderId, epoch);
                return true;
            } catch (KeeperException.BadVersionException race) {
                return false;
            }
        } catch (KeeperException.NoNodeException absent) {
            return false;
        } catch (Exception e) {
            LOG.error("event=zk_lease_release_error leaseKey={} holderId={} error={}",
                    leaseKey, holderId, e.toString(), e);
            throw ZooKeeperFailures.storageFailure("release", leaseKey, e);
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
            throw ZooKeeperFailures.storageFailure("query", leaseKey, e);
        }
    }

    /**
     * Samples the ZooKeeper leader's clock (A-LEASE-01): a write to the dedicated {@code _clock}
     * znode is stamped by the server with an {@code mtime}, giving a single authoritative time source
     * shared by every coordinator, independent of local JVM clocks.
     */
    private long serverNowMillis() {
        try {
            return client.setData().forPath(clockPath, EMPTY).getMtime();
        } catch (Exception failure) {
            throw ZooKeeperFailures.storageFailure("clock", clockPath, failure);
        }
    }

    private void ensureClockNode() {
        try {
            if (client.checkExists().forPath(clockPath) == null) {
                client.create().creatingParentsIfNeeded().forPath(clockPath, EMPTY);
            }
        } catch (KeeperException.NodeExistsException concurrent) {
            // Created by another coordinator — fine.
        } catch (Exception e) {
            throw ZooKeeperFailures.storageFailure("initialize clock", clockPath, e);
        }
    }

    private String leasePath(String leaseKey) {
        // A reversible, collision-free path segment is mandatory: replacing '/'
        // with '_' aliases distinct logical resources (for example a/b and a_b),
        // which can make unrelated jobs share one fence. URL-safe Base64 contains
        // no ZooKeeper path separator and preserves the full UTF-8 key.
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(leaseKey.getBytes(StandardCharsets.UTF_8));
        return basePath + "/k-" + encoded;
    }

    private static String normalizeBase(String base) {
        String b = base.startsWith("/") ? base : "/" + base;
        while (b.length() > 1 && b.endsWith("/")) {
            b = b.substring(0, b.length() - 1);
        }
        return b;
    }

    private static LeaseEntry toEntry(String leaseKey, LeaseData data) {
        return new LeaseEntry(leaseKey, data.holderId(), data.epoch(),
                Instant.ofEpochMilli(data.acquiredAtMs()),
                Instant.ofEpochMilli(data.expiresAtMs()));
    }

    private static void requireRequest(
            String leaseKey, String holderId, Duration duration) {
        Objects.requireNonNull(leaseKey, "leaseKey must not be null");
        Objects.requireNonNull(holderId, "holderId must not be null");
        Objects.requireNonNull(duration, "duration must not be null");
        if (leaseKey.isBlank() || holderId.isBlank() || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(
                    "leaseKey and holderId must be non-blank and duration must be positive");
        }
    }

    private static void requireEpoch(long epoch) {
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive");
        }
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
