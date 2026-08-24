package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v1.shuffle.CleanupRequest;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v1.shuffle.ShuffleServiceGrpc;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Coordinator-side {@link ShuffleLifecycleManager} that communicates with
 * remote shuffle servers via gRPC to manage shuffle registration, commit,
 * and cleanup on behalf of the DAG dispatch engine.
 *
 * <p>Unlike the worker-side {@code RemoteShuffleExchangeManager} which also
 * handles data channels (sink/source), this class only manages the lifecycle
 * RPCs the coordinator needs: register before task dispatch, and cleanup
 * after query completion or failure.</p>
 *
 * <p>Thread-safe: gRPC stubs are thread-safe; internal maps use
 * {@link ConcurrentHashMap}.</p>
 */
@ThreadSafe
public final class CoordinatorShuffleLifecycleManager
        implements ShuffleLifecycleManager, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(CoordinatorShuffleLifecycleManager.class);
    private static final long RPC_TIMEOUT_MS = 10_000;
    private static final long CHANNEL_SHUTDOWN_SECONDS = 5;

    private final List<ShuffleServerEndpoint> endpoints;
    private final ConcurrentHashMap<String, ManagedChannel> channels =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();

    /**
     * Creates a coordinator shuffle lifecycle manager.
     *
     * @param endpoints remote shuffle server endpoints (must not be empty)
     */
    public CoordinatorShuffleLifecycleManager(List<ShuffleServerEndpoint> endpoints) {
        checkNotNull(endpoints, "endpoints must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException(
                    "shuffle endpoints must not be empty for REMOTE mode");
        }
        this.endpoints = List.copyOf(endpoints);
        LOG.info("event=coordinator_shuffle_lifecycle_init endpointCount={}",
                endpoints.size());
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        checkNotNull(registration, "registration must not be null");
        ShuffleServerEndpoint endpoint = selectEndpoint(registration.queryId());
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);

        RegisterShuffleRequest request = RegisterShuffleRequest.newBuilder()
                .setQueryId(registration.queryId())
                .setStageId(registration.stageId())
                .setNumPartitions(registration.numPartitions())
                .setEstimatedBytesPerPartition(registration.estimatedBytesPerPartition())
                .setRequireReplication(registration.requireReplication())
                .build();

        RegisterShuffleResponse response = stub
                .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .registerShuffle(request);

        List<ShuffleServerEndpoint> handleEndpoints = toEndpoints(response);
        LOG.info("event=shuffle_registered queryId={} stageId={} shuffleId={} "
                        + "endpointCount={}",
                registration.queryId(), registration.stageId(),
                response.getShuffleId(), handleEndpoints.size());

        return new ShuffleHandle(response.getShuffleId(), handleEndpoints);
    }

    private static List<ShuffleServerEndpoint> toEndpoints(RegisterShuffleResponse response) {
        List<ShuffleServerEndpoint> result = new ArrayList<>(response.getEndpointsCount());
        for (var ep : response.getEndpointsList()) {
            result.add(new ShuffleServerEndpoint(ep.getHost(), ep.getPort()));
        }
        return result;
    }

    /**
     * Commit is a no-op at the coordinator level. Workers commit partitions
     * directly to the shuffle server after writing all data. The coordinator
     * only needs register (before dispatch) and cleanup (after query end).
     */
    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        checkNotNull(queryId, "queryId must not be null");
        LOG.debug("event=shuffle_commit_delegated queryId={} stageId={} "
                + "reason=workers_commit_partitions_directly",
                queryId, stageId);
        return ShuffleCommitResult.EMPTY;
    }

    @Override
    public void cleanupShuffle(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        ShuffleServerEndpoint endpoint = selectEndpoint(queryId);
        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = stubFor(endpoint);

        try {
            CleanupRequest request = CleanupRequest.newBuilder()
                    .setQueryId(queryId)
                    .build();
            var response = stub
                    .withDeadlineAfter(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .cleanup(request);
            LOG.info("event=shuffle_cleanup queryId={} partitionsCleaned={} "
                            + "bytesFreed={}",
                    queryId, response.getPartitionsCleaned(),
                    response.getBytesFreed());
        } catch (Exception e) {
            LOG.warn("event=shuffle_cleanup_failed queryId={} error={}",
                    queryId, e.getMessage());
        }
    }

    @Override
    public void close() {
        for (var entry : channels.entrySet()) {
            ManagedChannel ch = entry.getValue();
            ch.shutdown();
            try {
                if (!ch.awaitTermination(CHANNEL_SHUTDOWN_SECONDS,
                        TimeUnit.SECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ch.shutdownNow();
            }
        }
        channels.clear();
        stubs.clear();
        LOG.info("event=coordinator_shuffle_lifecycle_closed");
    }

    private ShuffleServerEndpoint selectEndpoint(String queryId) {
        int index = Math.floorMod(queryId.hashCode(), endpoints.size());
        return endpoints.get(index);
    }

    private ShuffleServiceGrpc.ShuffleServiceBlockingStub stubFor(
            ShuffleServerEndpoint endpoint) {
        String key = endpoint.host() + ":" + endpoint.port();
        return stubs.computeIfAbsent(key, k -> {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(endpoint.host(), endpoint.port())
                    .usePlaintext()
                    .build();
            channels.put(key, channel);
            return ShuffleServiceGrpc.newBlockingStub(channel);
        });
    }
}
