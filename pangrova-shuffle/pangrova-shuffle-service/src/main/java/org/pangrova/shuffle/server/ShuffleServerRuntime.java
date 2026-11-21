package org.pangrova.shuffle.server;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import org.pangrova.shuffle.config.ShuffleServiceConfig;
import org.pangrova.shuffle.security.CoordinatorPeerInterceptor;
import org.pangrova.shuffle.security.ShuffleCapabilityAuthority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns the production gRPC listener and all durable backend resources. */
public final class ShuffleServerRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerRuntime.class);
    private static final int GRACEFUL_SHUTDOWN_SECONDS = 30;
    private static final int RPC_ENVELOPE_BYTES = 64 * 1024;
    private static final int CLEANUP_RECOVERY_BATCH = 16;
    private static final int CLEANUP_RECOVERY_INTERVAL_SECONDS = 30;

    private final Server server;
    private final ShuffleServiceEngine engine;
    private final CanonicalShuffleGrpcServer transport;
    private final ScheduledExecutorService maintenance;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ShuffleServerRuntime(
            Server server,
            ShuffleServiceEngine engine,
            CanonicalShuffleGrpcServer transport,
            ScheduledExecutorService maintenance) {
        this.server = server;
        this.engine = engine;
        this.transport = transport;
        this.maintenance = maintenance;
    }

    /** Creates all backends, binds an mTLS listener, and starts accepting RPCs. */
    public static ShuffleServerRuntime start(ShuffleServiceConfig config) throws IOException {
        PostgresShuffleStore metadata = PostgresShuffleStore.create(config.database());
        S3ShuffleObjectStorage objects = null;
        try {
            objects = S3ShuffleObjectStorage.create(config.objectStorage());
            metadata.requireObjectNamespace(config.objectStorage());
            ShuffleServiceEngine engine = new ShuffleServiceEngine(config, metadata,
                    metadata.batchStore(), objects,
                    ShuffleCapabilityAuthority.load(config.security().capabilitySecret()),
                    Clock.systemUTC());
            CanonicalShuffleGrpcServer transport = new CanonicalShuffleGrpcServer(
                    engine, config.limits());
            try {
                Server server = server(config, transport).start();
                ScheduledExecutorService maintenance = maintenance(engine);
                return new ShuffleServerRuntime(server, engine, transport, maintenance);
            } catch (IOException | RuntimeException failure) {
                transport.close();
                throw failure;
            }
        } catch (IOException | RuntimeException failure) {
            if (objects != null) {
                objects.close();
            }
            metadata.close();
            throw failure;
        }
    }

    private static ScheduledExecutorService maintenance(ShuffleServiceEngine engine) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "shuffle-cleanup-recovery");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(() -> recoverCleanups(engine), 0,
                CLEANUP_RECOVERY_INTERVAL_SECONDS, TimeUnit.SECONDS);
        return executor;
    }

    private static void recoverCleanups(ShuffleServiceEngine engine) {
        try {
            int recovered = engine.recoverInterruptedCleanups(CLEANUP_RECOVERY_BATCH);
            if (recovered > 0) {
                LOG.info("shuffleCleanupRecovery completed recoveredCount={}", recovered);
            }
        } catch (RuntimeException failure) {
            LOG.error("shuffleCleanupRecovery failed", failure);
        }
        try {
            int compensated = engine.sweepUncertainWritePrefixes(CLEANUP_RECOVERY_BATCH);
            if (compensated > 0) {
                LOG.info("shuffleUncertainWriteSweep completed prefixCount={}", compensated);
            }
        } catch (RuntimeException failure) {
            LOG.error("shuffleUncertainWriteSweep failed", failure);
        }
    }

    private static Server server(
            ShuffleServiceConfig config,
            CanonicalShuffleGrpcServer transport)
            throws IOException {
        var security = config.security();
        var ssl = serverSslContext(security);
        int inboundLimit = Math.addExact(
                config.limits().maxChunkBytes(), RPC_ENVELOPE_BYTES);
        return NettyServerBuilder.forPort(config.server().port())
                .sslContext(ssl).maxInboundMessageSize(inboundLimit).directExecutor()
                .addService(transport)
                .intercept(new CoordinatorPeerInterceptor(
                        config.security().coordinatorTlsIdentities()))
                .build();
    }

    static SslContext serverSslContext(ShuffleServiceConfig.Security security)
            throws IOException {
        return GrpcSslContexts.forServer(
                        security.certificateChain().toFile(), security.privateKey().toFile())
                .trustManager(security.rootCa().toFile())
                .clientAuth(ClientAuth.REQUIRE)
                .build();
    }

    /** Blocks until the process listener terminates. */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        maintenance.shutdownNow();
        server.shutdown();
        boolean interrupted = false;
        try {
            if (!server.awaitTermination(GRACEFUL_SHUTDOWN_SECONDS, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException failure) {
            interrupted = true;
            server.shutdownNow();
        } finally {
            transport.close();
            engine.close();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
