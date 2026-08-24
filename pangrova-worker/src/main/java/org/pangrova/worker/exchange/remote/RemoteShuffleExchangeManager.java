package org.pangrova.worker.exchange.remote;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.v1.shuffle.CommitPartitionResponse;
import org.pangrova.protocol.v1.shuffle.CompressionCodec;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v1.shuffle.RegisterShuffleResponse;
import org.pangrova.shuffle.exchange.ExchangeChannelManager;
import org.pangrova.shuffle.exchange.ExchangeSource;
import org.pangrova.shuffle.ShuffleCommitResult;
import org.pangrova.shuffle.ShuffleServerEndpoint;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.pangrova.shuffle.exchange.ExchangeSink;
import org.pangrova.shuffle.ShuffleHandle;
import org.pangrova.shuffle.ShuffleLifecycleManager;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.pangrova.shuffle.ShuffleRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ExchangeChannelManager} and {@link ShuffleLifecycleManager} implementation
 * backed by a Remote Shuffle Service.
 *
 * <p>Creates {@link RemoteShuffleExchangeSink} for writing and {@link RemoteShuffleExchangeSource}
 * for reading, both communicating with remote shuffle servers via gRPC through
 * {@link RemoteShuffleClientPool}.</p>
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} for shuffle handle tracking.</p>
 */
@ThreadSafe
public final class RemoteShuffleExchangeManager implements ExchangeChannelManager, ShuffleLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleExchangeManager.class);

    private final RemoteShuffleClientPool clientPool;
    private final CompressionCodec codec;
    private final List<ShuffleServerEndpoint> serverEndpoints;
    private final boolean replicationEnabled;

    /** Tracks shuffle registrations by queryId:stageId for commit and cleanup. */
    private final ConcurrentHashMap<String, ShuffleRegistrationEntry> shuffleEntries = new ConcurrentHashMap<>();

    /** Internal record pairing a handle with its partition count for commit iteration. */
    private record ShuffleRegistrationEntry(ShuffleHandle handle, int numPartitions) {}

    public RemoteShuffleExchangeManager(RemoteShuffleClientPool clientPool, CompressionCodec codec,
                              List<ShuffleServerEndpoint> serverEndpoints) {
        this(clientPool, codec, serverEndpoints, false);
    }

    /** Creates a manager with explicit replication setting. */
    public RemoteShuffleExchangeManager(RemoteShuffleClientPool clientPool, CompressionCodec codec,
                              List<ShuffleServerEndpoint> serverEndpoints,
                              boolean replicationEnabled) {
        this.clientPool = checkNotNull(clientPool, "clientPool must not be null");
        this.codec = codec != null ? codec : CompressionCodec.NONE;
        this.serverEndpoints = List.copyOf(checkNotNull(serverEndpoints, "serverEndpoints must not be null"));
        checkArgument(!serverEndpoints.isEmpty(), "serverEndpoints must not be empty");
        this.replicationEnabled = replicationEnabled;
        if (replicationEnabled) {
            checkArgument(serverEndpoints.size() >= 2,
                    "replicationEnabled requires at least 2 server endpoints, got " + serverEndpoints.size());
        }
    }

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        checkNotNull(registration, "registration must not be null");
        ShuffleServerEndpoint primary = selectServer(registration.stageId());
        RemoteShuffleClient client = clientPool.getOrCreateClient(primary.host(), primary.port());
        RegisterShuffleRequest request = buildRegisterRequest(registration);
        try {
            if (replicationEnabled) {
                ShuffleServerEndpoint replica = selectReplica(registration.stageId());
                RemoteShuffleClient replicaClient = clientPool.getOrCreateClient(replica.host(), replica.port());
                replicaClient.registerShuffle(request);
                return doRegister(client, request, registration, List.of(primary, replica));
            }
            return doRegister(client, request, registration, List.of(primary));
        } catch (Exception e) {
            throw new RemoteShuffleClientException("Failed to register shuffle for queryId="
                    + registration.queryId() + " stageId=" + registration.stageId(), e);
        }
    }

    private static RegisterShuffleRequest buildRegisterRequest(ShuffleRegistration reg) {
        return RegisterShuffleRequest.newBuilder()
                .setQueryId(reg.queryId())
                .setStageId(reg.stageId())
                .setNumPartitions(reg.numPartitions())
                .setEstimatedBytesPerPartition(reg.estimatedBytesPerPartition())
                .setRequireReplication(reg.requireReplication())
                .build();
    }

    private ShuffleHandle doRegister(RemoteShuffleClient client, RegisterShuffleRequest request,
                                     ShuffleRegistration registration, List<ShuffleServerEndpoint> endpoints) {
        RegisterShuffleResponse response = client.registerShuffle(request);
        String shuffleId = response.getShuffleId();
        ShuffleHandle handle = new ShuffleHandle(shuffleId, endpoints);
        shuffleEntries.put(buildKey(registration.queryId(), registration.stageId()),
                new ShuffleRegistrationEntry(handle, registration.numPartitions()));
        LOG.info("event=remote_shuffle_registered queryId={} stageId={} shuffleId={} endpoints={}",
                registration.queryId(), registration.stageId(), shuffleId, endpoints.size());
        return handle;
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        String key = buildKey(queryId, stageId);
        ShuffleRegistrationEntry entry = shuffleEntries.get(key);
        if (entry == null) {
            LOG.warn("event=remote_shuffle_commit_no_handle queryId={} stageId={}", queryId, stageId);
            return ShuffleCommitResult.EMPTY;
        }
        ShuffleHandle handle = entry.handle();
        RemoteShuffleClient client = getClientForHandle(handle);
        Map<Integer, ShufflePartitionStats> partitionStats = new HashMap<>();
        long totalBytes = 0;
        long totalRows = 0;
        for (int partitionId = 0; partitionId < entry.numPartitions(); partitionId++) {
            CommitPartitionResponse response = client.commitPartition(handle.shuffleId(), partitionId);
            long bytes = response.getTotalBytes();
            long rows = response.getTotalRows();
            partitionStats.put(partitionId, new ShufflePartitionStats(partitionId, bytes, rows));
            totalBytes += bytes;
            totalRows += rows;
        }
        LOG.info("event=remote_shuffle_committed queryId={} stageId={} shuffleId={} "
                + "partitions={} totalBytes={} totalRows={}",
                queryId, stageId, handle.shuffleId(),
                entry.numPartitions(), totalBytes, totalRows);
        return new ShuffleCommitResult(partitionStats, totalBytes, totalRows);
    }

    @Override
    public void cleanupShuffle(String queryId) {
        try {
            ShuffleServerEndpoint primary = serverEndpoints.get(0);
            RemoteShuffleClient client = clientPool.getOrCreateClient(primary.host(), primary.port());
            client.cleanup(queryId);
        } catch (Exception e) {
            LOG.warn("event=remote_shuffle_cleanup_rpc_failed queryId={} error={}",
                    queryId, e.getMessage());
        }
        shuffleEntries.keySet().removeIf(k -> k.startsWith(queryId + ":"));
        LOG.info("event=remote_shuffle_cleanup queryId={}", queryId);
    }

    @Override
    public ExchangeSink createSink(String queryId, int partitionId) {
        checkNotNull(queryId, "queryId must not be null");
        checkArgument(partitionId >= 0, "partitionId must not be negative");
        ShuffleHandle handle = findHandle(queryId);
        RemoteShuffleClient client = getClientForHandle(handle);
        LOG.debug("event=remote_shuffle_sink_created queryId={} partitionId={}", queryId, partitionId);
        return new RemoteShuffleExchangeSink(handle.shuffleId(), partitionId, client, codec);
    }

    @Override
    public ExchangeSource createSource(String queryId, int partitionId, ExchangeReadMode readMode) {
        checkNotNull(queryId, "queryId must not be null");
        checkNotNull(readMode, "readMode must not be null");
        checkArgument(partitionId >= 0, "partitionId must not be negative");
        ShuffleHandle handle = findHandle(queryId);
        RemoteShuffleClient client = getClientForHandle(handle);
        LOG.debug("event=remote_shuffle_source_created queryId={} partitionId={} readMode={}",
                queryId, partitionId, readMode);
        return new RemoteShuffleExchangeSource(handle.shuffleId(), partitionId, client, readMode);
    }

    @Override
    public void close() {
        shuffleEntries.clear();
        clientPool.close();
        LOG.info("event=remote_shuffle_exchange_manager_closed");
    }

    private ShuffleHandle findHandle(String queryId) {
        return shuffleEntries.entrySet().stream()
                .filter(e -> e.getKey().startsWith(queryId + ":"))
                .map(e -> e.getValue().handle())
                .findFirst()
                .orElseThrow(() -> new RemoteShuffleClientException(
                        "No shuffle handle found for queryId=" + queryId));
    }

    private RemoteShuffleClient getClientForHandle(ShuffleHandle handle) {
        if (handle.endpoints().isEmpty()) {
            throw new RemoteShuffleClientException("No endpoints in shuffle handle: " + handle.shuffleId());
        }
        ShuffleServerEndpoint ep = handle.endpoints().get(0);
        return clientPool.getOrCreateClient(ep.host(), ep.port());
    }

    ShuffleServerEndpoint selectServer(int stageId) {
        return serverEndpoints.get(Math.abs(stageId) % serverEndpoints.size());
    }

    /**
     * Selects a replica endpoint different from the primary for the given stageId.
     * Uses (stageId + 1) mod size to ensure primary and replica are always different nodes.
     */
    ShuffleServerEndpoint selectReplica(int stageId) {
        return serverEndpoints.get((Math.abs(stageId) + 1) % serverEndpoints.size());
    }

    private static String buildKey(String queryId, int stageId) {
        return queryId + ":" + stageId;
    }
}
