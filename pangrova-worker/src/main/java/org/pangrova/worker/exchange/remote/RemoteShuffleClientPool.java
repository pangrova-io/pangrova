package org.pangrova.worker.exchange.remote;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleServiceAddress;
import org.pangrova.worker.exchange.CanonicalBatchChunks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.TlsChannelCredentials;

/**
 * Connection pool for {@link RemoteShuffleClient} instances, keyed by remote shuffle server endpoint.
 *
 * <p>Backed by Caffeine cache with idle timeout (5 minutes) and maximum size (100).
 * Connections idle longer than the timeout are automatically evicted and closed.
 * gRPC channels are inherently multiplexed (HTTP/2), so a single channel
 * per server is sufficient for high throughput.</p>
 *
 * <p>When TLS is enabled, the pool configures proper certificate validation using
 * the provided trust store. Mutual TLS (mTLS) is supported when a key store is also
 * configured. The trust-all pattern is explicitly avoided — all server certificates
 * must be validated against the configured trust store.</p>
 *
 * <p>Concurrency strategy: Caffeine cache handles thread-safe access internally.
 * Eviction callbacks run outside any cache lock, so channel shutdown (I/O) never
 * holds a lock.</p>
 */
@ThreadSafe
public final class RemoteShuffleClientPool implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleClientPool.class);

    static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 5;
    static final int DEFAULT_MAX_POOL_SIZE = 100;
    static final int MAX_INBOUND_MESSAGE_BYTES =
            CanonicalBatchChunks.MAX_CHUNK_BYTES * 2;

    private final Cache<String, RemoteShuffleClient> clients;
    private final long writeTimeoutMs;
    private final long readTimeoutMs;
    private final boolean tlsEnabled;
    private final ChannelCredentials tlsCredentials;

    /**
     * Pool tuning options: eviction timing, capacity, and TLS configuration.
     *
     * @param idleTimeout idle duration before a client is evicted
     * @param timeUnit    unit for {@code idleTimeout}
     * @param maxPoolSize maximum number of pooled clients
     * @param tlsEnabled  whether TLS is enabled for new channels
     * @param tlsConfig   TLS trust/key store configuration (required when TLS is enabled)
     */
    public record PoolOptions(long idleTimeout, TimeUnit timeUnit, int maxPoolSize,
                              boolean tlsEnabled, ShuffleClientTlsConfig tlsConfig,
                              PemTlsConfig pemTlsConfig) {}

    /** PEM certificate material shared with the Native Worker transport policy. */
    public record PemTlsConfig(Path certificateChain, Path privateKey, Path rootCa) {
        public PemTlsConfig {
            checkNotNull(certificateChain, "certificateChain must not be null");
            checkNotNull(privateKey, "privateKey must not be null");
            checkNotNull(rootCa, "rootCa must not be null");
        }
    }

    public RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs, int maxConnectionsPerServer) {
        this(writeTimeoutMs, readTimeoutMs, maxConnectionsPerServer,
                new PoolOptions(DEFAULT_IDLE_TIMEOUT_MINUTES, TimeUnit.MINUTES,
                        DEFAULT_MAX_POOL_SIZE, false, null, null));
    }

    public RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs,
                                   int maxConnectionsPerServer, boolean tlsEnabled) {
        this(writeTimeoutMs, readTimeoutMs, maxConnectionsPerServer,
                new PoolOptions(DEFAULT_IDLE_TIMEOUT_MINUTES, TimeUnit.MINUTES,
                        DEFAULT_MAX_POOL_SIZE, tlsEnabled, null, null));
    }

    public RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs,
                                   int maxConnectionsPerServer,
                                   long idleTimeout, TimeUnit timeUnit, int maxPoolSize,
                                   boolean tlsEnabled) {
        this(writeTimeoutMs, readTimeoutMs, maxConnectionsPerServer,
                new PoolOptions(idleTimeout, timeUnit, maxPoolSize, tlsEnabled, null, null));
    }

    /** Creates a TLS-enabled pool using the provided trust/key store configuration. */
    public RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs,
                                   int maxConnectionsPerServer,
                                   ShuffleClientTlsConfig tlsConfig) {
        this(writeTimeoutMs, readTimeoutMs, maxConnectionsPerServer,
                new PoolOptions(DEFAULT_IDLE_TIMEOUT_MINUTES, TimeUnit.MINUTES,
                        DEFAULT_MAX_POOL_SIZE, true,
                        checkNotNull(tlsConfig, "tlsConfig must not be null"), null));
    }

    /** Creates a mutual-TLS pool from the Worker's canonical PEM transport material. */
    public RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs,
                                   int maxConnectionsPerServer,
                                   PemTlsConfig pemTlsConfig) {
        this(writeTimeoutMs, readTimeoutMs, maxConnectionsPerServer,
                new PoolOptions(DEFAULT_IDLE_TIMEOUT_MINUTES, TimeUnit.MINUTES,
                        DEFAULT_MAX_POOL_SIZE, true, null,
                        checkNotNull(pemTlsConfig, "pemTlsConfig must not be null")));
    }

    private RemoteShuffleClientPool(long writeTimeoutMs, long readTimeoutMs,
                                    int maxConnectionsPerServer, PoolOptions options) {
        checkArgument(writeTimeoutMs > 0, "writeTimeoutMs must be positive");
        checkArgument(readTimeoutMs > 0, "readTimeoutMs must be positive");
        checkArgument(maxConnectionsPerServer > 0, "maxConnectionsPerServer must be positive");
        if (options.tlsEnabled()
                && options.tlsConfig() == null && options.pemTlsConfig() == null) {
            throw new IllegalArgumentException(
                    "TLS key and trust material is required when TLS is enabled");
        }
        this.writeTimeoutMs = writeTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.tlsEnabled = options.tlsEnabled();
        this.tlsCredentials = buildCredentials(options);
        this.clients = Caffeine.newBuilder()
                .expireAfterAccess(options.idleTimeout(), options.timeUnit())
                .maximumSize(options.maxPoolSize())
                .removalListener(this::onRemoval)
                .build();
    }

    /**
     * Removal listener — called by Caffeine outside any cache lock.
     * Closes the evicted shuffle client and its gRPC channel.
     */
    private void onRemoval(String key, RemoteShuffleClient client, RemovalCause cause) {
        if (client == null) {
            return;
        }
        client.close();
        LOG.info("event=remote_shuffle_client_evict key={} cause={}", key, cause);
    }

    /**
     * Returns a {@link RemoteShuffleClient} for the given server endpoint.
     * Creates a new client if none exists. Accessing a client resets its idle timer.
     *
     * @param endpoint remote shuffle server endpoint and expected TLS identity
     * @return a client connected to the specified endpoint
     */
    public RemoteShuffleClient getOrCreateClient(ShuffleServerEndpoint endpoint) {
        checkNotNull(endpoint, "endpoint must not be null");
        return getOrCreateClient(endpoint.host(), endpoint.port(), endpoint.tlsIdentity());
    }

    /** Returns a client for the explicit producer-attempt commit address. */
    public RemoteShuffleClient getOrCreateClient(ShuffleServiceAddress address) {
        checkNotNull(address, "address must not be null");
        return getOrCreateClient(address.host(), address.port(), address.tlsIdentity());
    }

    private RemoteShuffleClient getOrCreateClient(
            String host, int port, String tlsIdentity) {
        if (tlsEnabled != !tlsIdentity.isBlank()) {
            throw new IllegalArgumentException(
                    "Shuffle endpoint TLS identity does not match client TLS policy");
        }
        String key = host + ":" + port + ":" + tlsIdentity;
        return clients.get(key, k -> {
            ManagedChannel channel;
            if (tlsEnabled) {
                channel = Grpc.newChannelBuilder(host + ":" + port, tlsCredentials)
                        .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                        .overrideAuthority(tlsIdentity)
                        .build();
                LOG.info("event=remote_shuffle_client_created host={} port={} tls=true mtls={}",
                        host, port, true);
            } else {
                channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
                        .build();
                LOG.info("event=remote_shuffle_client_created host={} port={} tls=false", host, port);
            }
            return new RemoteShuffleClient(channel, writeTimeoutMs, readTimeoutMs);
        });
    }

    private static ChannelCredentials buildTlsCredentials(ShuffleClientTlsConfig config) {
        try {
            TlsChannelCredentials.Builder builder = TlsChannelCredentials.newBuilder();

            // Configure trust store for server certificate validation
            KeyStore trustStore = loadKeyStore(config.trustStorePath(), config.trustStorePassword());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            builder.trustManager(tmf.getTrustManagers());

            // Configure key store for mutual TLS (client certificate)
            if (config.mtlsEnabled()) {
                KeyStore keyStore = loadKeyStore(config.keyStorePath(), config.keyStorePassword());
                char[] keyPassword = config.keyStorePassword() != null
                        ? config.keyStorePassword().toCharArray() : null;
                try {
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                            KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(keyStore, keyPassword);
                    builder.keyManager(kmf.getKeyManagers());
                } finally {
                    if (keyPassword != null) {
                        Arrays.fill(keyPassword, '\0');
                    }
                }
            }

            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to build TLS credentials for shuffle client. "
                            + "trustStore=" + config.trustStorePath()
                            + " keyStore=" + config.keyStorePath(), e);
        }
    }

    private static ChannelCredentials buildCredentials(PoolOptions options) {
        if (!options.tlsEnabled()) {
            return null;
        }
        if (options.pemTlsConfig() != null) {
            PemTlsConfig pem = options.pemTlsConfig();
            try {
                return TlsChannelCredentials.newBuilder()
                        .trustManager(pem.rootCa().toFile())
                        .keyManager(pem.certificateChain().toFile(), pem.privateKey().toFile())
                        .build();
            } catch (java.io.IOException failure) {
                throw new IllegalStateException(
                        "Failed to load PEM shuffle TLS material", failure);
            }
        }
        return buildTlsCredentials(options.tlsConfig());
    }

    private static KeyStore loadKeyStore(String path, String password) {
        char[] passwordChars = password != null ? password.toCharArray() : null;
        try {
            String storeType = path.endsWith(".p12") || path.endsWith(".pfx") ? "PKCS12" : "JKS";
            KeyStore store = KeyStore.getInstance(storeType);
            try (InputStream is = Files.newInputStream(Path.of(path))) {
                store.load(is, passwordChars);
            }
            return store;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load key store from " + path, e);
        } finally {
            if (passwordChars != null) {
                Arrays.fill(passwordChars, '\0');
            }
        }
    }

    /**
     * Returns the number of active client connections.
     * Note: Caffeine eviction is asynchronous, so this is an approximation.
     */
    public int activeConnectionCount() {
        clients.cleanUp();
        return (int) clients.estimatedSize();
    }

    /**
     * Forces pending eviction maintenance to run synchronously.
     * Useful in tests to ensure eviction callbacks have completed.
     */
    public void cleanUp() {
        clients.cleanUp();
    }

    @Override
    public void close() {
        clients.invalidateAll();
        clients.cleanUp();
        LOG.info("event=remote_shuffle_client_pool_closed");
    }
}
