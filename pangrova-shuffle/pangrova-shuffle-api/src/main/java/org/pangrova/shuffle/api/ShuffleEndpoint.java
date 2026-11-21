package org.pangrova.shuffle.api;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Describes a remote shuffle endpoint for data retrieval.
 * Identifies an upstream task endpoint for direct worker-to-worker exchange.
 *
 * <p>Distinct from {@link org.pangrova.shuffle.api.ShuffleServerEndpoint} which describes
 * a Remote Shuffle Service server. This record describes a task-level shuffle endpoint
 * with query/stage/partition context for worker-to-worker data transport.</p>
 *
 * @param host        the shuffle server hostname
 * @param port        the shuffle server port (must be positive)
 * @param queryId     the owning query identifier
 * @param stageId     the producing stage identifier
 * @param partitionId the shuffle partition identifier
 */
public record ShuffleEndpoint(String host, int port, String queryId, int stageId, int partitionId) {

    public ShuffleEndpoint {
        checkNotNull(host, "host must not be null");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive, got: " + port);
        }
        checkNotNull(queryId, "queryId must not be null");
    }
}
