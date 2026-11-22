package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.pangrova.client.exception.RemoteShuffleClientException;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.protocol.shuffle.codec.CanonicalBatchChunks;
import org.pangrova.shuffle.pangrova.client.security.RemoteShuffleCredentials;
import org.pangrova.shuffle.pangrova.client.security.ShuffleClientTlsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Connection pool for {@link PangrovaShuffleClient} instances, keyed by remote shuffle server
 * endpoint.
 *
 * <p>The lookup cache has independently configured idle retention and size. Eviction retires an
 * owner; an active RPC lease keeps its channel alive. Worker-wide admission counts creating, cached
 * and retired channels until their actual termination. These are ManagedChannel credits, not socket
 * limits.
 *
 * <p>When TLS is enabled, the pool configures proper certificate validation using the provided
 * trust store. Mutual TLS (mTLS) is supported when a key store is also configured. The trust-all
 * pattern is explicitly avoided — all server certificates must be validated against the configured
 * trust store.
 *
 * <p>Concurrency strategy: Caffeine cache handles thread-safe access internally. Eviction callbacks
 * run outside any cache lock, so channel shutdown (I/O) never holds a lock.
 */
@ThreadSafe
public final class RemoteShuffleClientPool implements Closeable, org.pangrova.shuffle.nativewire.NativeShuffleReaders {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleClientPool.class);

    static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 5;
    static final int DEFAULT_MAX_POOL_SIZE = 100;
    static final int MAX_INBOUND_MESSAGE_BYTES = CanonicalBatchChunks.MAX_CHUNK_BYTES * 2;

    private final Cache<RemoteShuffleChannelAdmission.Endpoint, PangrovaShuffleClient> clients;
    private final long writeTimeoutMs;
    private final long readTimeoutMs;
    private final boolean tlsEnabled;
    private final ChannelCredentials tlsCredentials;
    private final RemoteShuffleChannelAdmission channelAdmission;
    private final Object admission = new Object();

    @GuardedBy("admission")
    private final Map<PangrovaShuffleClient, RemoteShuffleClientOwner> owners = new HashMap<>();

    @GuardedBy("admission")
    private final Set<RemoteShuffleClientOwner> constructionOwners = new HashSet<>();

    @GuardedBy("admission")
    private boolean closed;

    @GuardedBy("admission")
    private int creating;

    private final ConcurrentLinkedQueue<Throwable> evictionFailures = new ConcurrentLinkedQueue<>();

    /**
     * Pool tuning options: eviction timing, capacity, and TLS configuration.
     *
     * @param idleTimeout idle duration before a client is evicted
     * @param timeUnit unit for {@code idleTimeout}
     * @param maxCachedChannels maximum number of lookup-cache entries, not owner credits
     * @param tlsEnabled whether TLS is enabled for new channels
     * @param tlsConfig TLS trust/key store configuration (required when TLS is enabled)
     */
    public record PoolOptions(
            long idleTimeout,
            TimeUnit timeUnit,
            int maxCachedChannels,
            boolean tlsEnabled,
            ShuffleClientTlsConfig tlsConfig,
            PemTlsConfig pemTlsConfig) {
        public PoolOptions {
            checkArgument(
                    idleTimeout > 0 && maxCachedChannels > 0, "Cache retention must be positive");
            checkNotNull(timeUnit, "timeUnit");
        }

        /** Returns bounded channel retention with the default transport configuration. */
        public static PoolOptions defaults() {
            return new PoolOptions(
                    DEFAULT_IDLE_TIMEOUT_MINUTES,
                    TimeUnit.MINUTES,
                    DEFAULT_MAX_POOL_SIZE,
                    false,
                    null,
                    null);
        }

        /** Returns bounded retention using the supplied PEM mutual TLS identity. */
        public static PoolOptions mutualTls(PemTlsConfig pem) {
            return new PoolOptions(
                    DEFAULT_IDLE_TIMEOUT_MINUTES,
                    TimeUnit.MINUTES,
                    DEFAULT_MAX_POOL_SIZE,
                    true,
                    null,
                    checkNotNull(pem, "pem"));
        }
    }

    /** PEM certificate material shared with the Native Worker transport policy. */
    public record PemTlsConfig(Path certificateChain, Path privateKey, Path rootCa) {
        public PemTlsConfig {
            checkNotNull(certificateChain, "certificateChain must not be null");
            checkNotNull(privateKey, "privateKey must not be null");
            checkNotNull(rootCa, "rootCa must not be null");
        }
    }

    /** Configures actual cache retention and transport security for a deployment. */
    public RemoteShuffleClientPool(
            long writeTimeoutMs,
            long readTimeoutMs,
            PoolOptions options,
            RemoteShuffleChannelAdmission channelAdmission) {
        checkArgument(writeTimeoutMs > 0, "writeTimeoutMs must be positive");
        checkArgument(readTimeoutMs > 0, "readTimeoutMs must be positive");
        this.channelAdmission = checkNotNull(channelAdmission, "channelAdmission");
        if (options.tlsEnabled() && options.tlsConfig() == null && options.pemTlsConfig() == null) {
            throw new IllegalArgumentException(
                    "TLS key and trust material is required when TLS is enabled");
        }
        this.writeTimeoutMs = writeTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.tlsEnabled = options.tlsEnabled();
        this.tlsCredentials = buildCredentials(options);
        this.clients =
                Caffeine.newBuilder()
                        .expireAfterAccess(options.idleTimeout(), options.timeUnit())
                        .maximumSize(options.maxCachedChannels())
                        .removalListener(this::onRemoval)
                        .build();
    }

    private static ChannelCredentials buildCredentials(PoolOptions options) {
        if (!options.tlsEnabled()) {
            return null;
        }
        var pem = options.pemTlsConfig();
        return pem == null
                ? RemoteShuffleCredentials.fromStores(options.tlsConfig())
                : RemoteShuffleCredentials.fromPem(
                        pem.rootCa(), pem.certificateChain(), pem.privateKey());
    }

    /**
     * Removal listener — called by Caffeine outside any cache lock. Retires the evicted client; its
     * channel is joined after its last borrower returns.
     */
    private void onRemoval(
            RemoteShuffleChannelAdmission.Endpoint key,
            PangrovaShuffleClient client,
            RemovalCause cause) {
        if (client == null) {
            return;
        }
        final RemoteShuffleClientOwner owner;
        synchronized (admission) {
            owner = owners.get(client);
            if (owner == null) {
                return;
            }
            owner.retired = true;
            if (owner.borrowers != 0) {
                return;
            }
        }
        try {
            drainOwner(owner);
        } catch (RuntimeException | Error failure) {
            evictionFailures.add(failure);
            LOG.error(
                    "event=remote_shuffle_client_evict_failed key={} cause={}",
                    key,
                    cause,
                    failure);
            return;
        }
        LOG.info("event=remote_shuffle_client_evict key={} cause={}", key, cause);
    }

    /** Creates the reader only after validating the wire receipt and the exact consumer owner. */
    @Override
    public RemoteShuffleExchangeSource open(org.pangrova.protocol.v2.ExternalShuffleInput input,
            org.pangrova.shuffle.api.identity.ExchangeTaskAttempt consumer) {
        var admittedOwner = input.getExecutionOwner();
        var consumerOwner = consumer.executionOwner();
        if (!org.pangrova.protocol.task.CanonicalTaskRequests.validExternalInput(input)
                || !admittedOwner.getOwnerId().equals(consumerOwner.ownerId())
                || !admittedOwner.getOwnerIncarnation().equals(consumerOwner.ownerIncarnation())
                || admittedOwner.getFencingToken() != consumerOwner.fencingToken()) {
            throw new IllegalArgumentException("Invalid external shuffle read binding or execution owner");
        }
        var checksum = input.getExpectedProducerSetChecksum();
        var published = input.getPublication();
        var context = new org.pangrova.shuffle.api.identity.ExchangeChannelContext(
                consumer, input.getOutputPartitionId(), input.getDeadlineEpochMs());
        var read = new PangrovaPartitionRead(input.getShuffleId(), context, input.getPlanEpoch(),
                input.getExpectedProducerPartitions(), checksum.getDigest().toByteArray(),
                new org.pangrova.shuffle.api.ShuffleCommitReceipt.Partition(published.getLastSequence(),
                        published.getTotalBytes(), published.getTotalRows(),
                        java.util.HexFormat.of().formatHex(published.getManifestSha256().toByteArray())));
        var grant = input.getReadCapability();
        var access = new org.pangrova.shuffle.api.ShuffleAccess(grant.getToken().toByteArray(),
                grant.getExpiresAtEpochMs());
        var endpoint = input.getEndpointsList().stream().filter(candidate ->
                candidate.getPartitionStart() <= input.getOutputPartitionId()
                        && input.getOutputPartitionId() < candidate.getPartitionEnd()).findFirst().orElseThrow();
        return new RemoteShuffleExchangeSource(read, access,
                borrow(new ShuffleServiceAddress(endpoint.getHost(), endpoint.getPort(), endpoint.getTlsIdentity())));
    }

    /**
     * Borrows a channel owner for the given server endpoint. Creates a new client if none exists.
     * Accessing a client resets its idle timer.
     *
     * @param endpoint remote shuffle server endpoint and expected TLS identity
     * @return a lease kept until the actual producer/consumer RPCs have drained
     */
    public RemoteShuffleClientLease borrow(ShuffleServerEndpoint endpoint) {
        checkNotNull(endpoint, "endpoint must not be null");
        return borrow(endpoint.host(), endpoint.port(), endpoint.tlsIdentity());
    }

    /** Returns a client for the explicit producer-attempt commit address. */
    public RemoteShuffleClientLease borrow(ShuffleServiceAddress address) {
        checkNotNull(address, "address must not be null");
        return borrow(address.host(), address.port(), address.tlsIdentity());
    }

    private RemoteShuffleClientLease borrow(String host, int port, String tlsIdentity) {
        if (tlsEnabled != !tlsIdentity.isBlank()) {
            throw new IllegalArgumentException(
                    "Shuffle endpoint TLS identity does not match client TLS policy");
        }
        var endpoint = new RemoteShuffleChannelAdmission.Endpoint(host, port, tlsIdentity);
        cleanUp();
        while (true) {
            final RemoteShuffleClientOwner candidate;
            synchronized (admission) {
                if (closed) {
                    throw new IllegalStateException("Remote shuffle client pool is closed");
                }
                channelAdmission.requireOpen();
                var existing = clients.getIfPresent(endpoint);
                var owner = owners.get(existing);
                if (owner != null && !owner.retired) {
                    return lease(owner);
                }
                candidate = beginCreation(endpoint);
            }
            try {
                createCandidate(candidate, endpoint);
            } catch (RuntimeException | Error failure) {
                abandonCreation(candidate);
                try {
                    drainOwner(candidate);
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            try {
                var admitted = publishCreation(candidate, endpoint);
                if (admitted != null) {
                    return admitted;
                }
            } catch (RuntimeException | Error failure) {
                try {
                    drainOwner(candidate);
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            drainOwner(candidate);
        }
    }

    @GuardedBy("admission")
    private RemoteShuffleClientOwner beginCreation(
            RemoteShuffleChannelAdmission.Endpoint endpoint) {
        var reservation = channelAdmission.reserve(endpoint);
        RemoteShuffleClientOwner candidate = null;
        try {
            candidate = new RemoteShuffleClientOwner(reservation);
            constructionOwners.add(candidate);
            creating++;
            return candidate;
        } catch (RuntimeException | Error failure) {
            if (candidate != null) {
                constructionOwners.remove(candidate);
            }
            reservation.constructionExited();
            reservation.releaseTerminated();
            throw failure;
        }
    }

    private void abandonCreation(RemoteShuffleClientOwner candidate) {
        synchronized (admission) {
            candidate.retired = true;
            creating--;
            admission.notifyAll();
        }
    }

    private RemoteShuffleClientLease publishCreation(
            RemoteShuffleClientOwner candidate, RemoteShuffleChannelAdmission.Endpoint endpoint) {
        synchronized (admission) {
            try {
                owners.put(candidate.client, candidate);
                if (!closed && clients.asMap().putIfAbsent(endpoint, candidate.client) == null) {
                    return lease(candidate);
                }
                candidate.retired = true;
                return null;
            } catch (RuntimeException | Error failure) {
                // The construction ledger remains authoritative if publication
                // allocation fails. Retirement must precede any cleanup retry.
                candidate.retired = true;
                try {
                    clients.asMap().remove(endpoint, candidate.client);
                } catch (RuntimeException | Error removalFailure) {
                    if (removalFailure != failure) {
                        failure.addSuppressed(removalFailure);
                    }
                }
                throw failure;
            } finally {
                if (owners.get(candidate.client) == candidate) {
                    constructionOwners.remove(candidate);
                }
                creating--;
                admission.notifyAll();
            }
        }
    }

    private void createCandidate(
            RemoteShuffleClientOwner candidate, RemoteShuffleChannelAdmission.Endpoint endpoint) {
        try {
            ManagedChannel channel;
            if (tlsEnabled) {
                channel =
                        Grpc.newChannelBuilder(
                                        endpoint.host() + ":" + endpoint.port(), tlsCredentials)
                                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                                .overrideAuthority(endpoint.tlsIdentity())
                                .build();
            } else {
                channel =
                        ManagedChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                                .usePlaintext()
                                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                                .build();
            }
            candidate.reservation.attach(channel);
            candidate.client = new PangrovaShuffleClient(channel, writeTimeoutMs, readTimeoutMs);
            LOG.info(
                    "event=remote_shuffle_client_created host={} port={} tls={}",
                    endpoint.host(),
                    endpoint.port(),
                    tlsEnabled);
        } finally {
            candidate.reservation.constructionExited();
        }
    }

    private RemoteShuffleClientLease lease(RemoteShuffleClientOwner owner) {
        var result = new RemoteShuffleClientLease(this, owner.client);
        owner.borrowers++;
        return result;
    }

    void returnLease(PangrovaShuffleClient client, boolean firstReturn) {
        final RemoteShuffleClientOwner owner;
        synchronized (admission) {
            owner = owners.get(client);
            if (owner == null) {
                if (!client.isTerminated()) {
                    throw new IllegalStateException("Untracked live shuffle client");
                }
                return;
            }
            if (firstReturn) {
                owner.borrowers--;
            }
            if (owner.borrowers < 0) {
                throw new IllegalStateException("Negative shuffle client borrower count");
            }
            if (!owner.retired || owner.borrowers != 0) {
                return;
            }
        }
        drainOwner(owner);
    }

    private void drainOwner(RemoteShuffleClientOwner owner) {
        owner.closeChannel();
        synchronized (admission) {
            owners.remove(owner.client, owner);
            constructionOwners.remove(owner);
        }
    }

    /** Live and creating channels across every pool sharing this Worker admission budget. */
    public int workerOwnedChannelCount() {
        return channelAdmission.snapshot().ownedAndCreatingChannels();
    }

    /**
     * Returns this pool's exact owned and creating channel count, including retired owners whose
     * actual join has not yet succeeded.
     */
    public int activeConnectionCount() {
        synchronized (admission) {
            return owners.size() + constructionOwners.size();
        }
    }

    /**
     * Forces pending eviction maintenance to run synchronously. Useful in tests to ensure eviction
     * callbacks have completed.
     */
    public void cleanUp() {
        clients.cleanUp();
        reapOwners(false);
    }

    @Override
    public void close() {
        synchronized (admission) {
            closed = true;
        }
        awaitCreators();
        // Retire ownership before invalidating the lookup cache. An active
        // lease is not closed by eviction or by a failed pool shutdown.
        reapOwners(true);
        clients.invalidateAll();
        clients.cleanUp();
        LOG.info("event=remote_shuffle_client_pool_closed");
    }

    private void awaitCreators() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        synchronized (admission) {
            while (creating != 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("Shuffle client creation has not exited");
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(admission, remaining);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new RemoteShuffleClientException(
                            "Shuffle pool admission drain interrupted", failure);
                }
            }
        }
    }

    private void reapOwners(boolean all) {
        final List<RemoteShuffleClientOwner> snapshot;
        synchronized (admission) {
            var allOwners = new HashSet<>(owners.values());
            allOwners.addAll(constructionOwners);
            snapshot = List.copyOf(allOwners);
            for (var owner : snapshot) {
                if ((!constructionOwners.contains(owner) || owner.retired)
                        && owner.reservation.hasConstructionExited()
                        && (all
                                || owner.client == null
                                || !clients.asMap().containsValue(owner.client))) {
                    owner.retired = true;
                }
            }
        }
        Throwable failure = takeEvictionFailures();
        for (var owner : snapshot) {
            try {
                reapOwner(owner, all);
            } catch (RuntimeException | Error cleanup) {
                failure = combine(failure, cleanup);
            }
        }
        failure = combine(failure, takeEvictionFailures());
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private Throwable takeEvictionFailures() {
        Throwable failure = null;
        Throwable observed;
        while ((observed = evictionFailures.poll()) != null) failure = combine(failure, observed);
        return failure;
    }

    private void reapOwner(RemoteShuffleClientOwner owner, boolean all) {
        synchronized (admission) {
            if (!owner.reservation.hasConstructionExited()) {
                return;
            }
            if (!owner.retired) {
                return;
            }
            if (owner.borrowers != 0) {
                if (all) {
                    throw new IllegalStateException(
                            "Shuffle client still has active producer/reader leases");
                }
                return;
            }
        }
        drainOwner(owner);
    }

    private static Throwable combine(Throwable first, Throwable next) {
        if (next == null) {
            return first;
        }
        if (first == null) {
            return next;
        }
        if (first != next) {
            first.addSuppressed(next);
        }
        return first;
    }
}
