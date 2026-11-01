package org.pangrova.shuffle.core.spool;

import java.nio.file.Path;

import org.pangrova.shuffle.api.capability.ShuffleDurability;
import org.pangrova.shuffle.api.spool.SpoolReader;
import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;
import org.pangrova.shuffle.api.spool.SpoolStore;
import org.pangrova.shuffle.api.spool.SpoolWriter;

/**
 * Object-storage-backed {@link SpoolStore} (ADR-002 D6): committed shuffle segments are immutable objects
 * under {@code &lt;queryId&gt;/s&lt;stageId&gt;/p&lt;partitionId&gt;/m&lt;mapId&gt;-a&lt;attemptId&gt;.arrow}.
 *
 * <p>Durability is {@link ShuffleDurability#DURABLE}: segments survive worker loss, enabling
 * task-level retry without recomputing upstream stages.
 */
public final class ObjectStoreSpoolStore implements SpoolStore {

    private final ShuffleObjectStorage objects;
    private final Path tempDir;

    public ObjectStoreSpoolStore(ShuffleObjectStorage objects, Path tempDir) {
        if (objects == null) {
            throw new IllegalArgumentException("objects must not be null");
        }
        if (tempDir == null) {
            throw new IllegalArgumentException("tempDir must not be null");
        }
        this.objects = objects;
        this.tempDir = tempDir;
    }

    @Override
    public SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
        validateQueryId(queryId);
        String key = segmentKey(queryId, stageId, partitionId, mapId, attemptId);
        return new ObjectStoreSpoolWriter(objects, key, tempDir);
    }

    @Override
    public SpoolReader read(String queryId, int stageId, int partitionId,
                            int startMapIndex, int endMapIndex) {
        validateQueryId(queryId);
        String prefix = partitionPrefix(queryId, stageId, partitionId);
        return new ObjectStoreSpoolReader(objects, prefix, startMapIndex, endMapIndex);
    }

    @Override
    public void delete(String queryId) {
        validateQueryId(queryId);
        String after = null;
        do {
            after = objects.deletePrefixPage(queryId + "/", after, 1000);
        } while (after != null);
    }

    @Override
    public ShuffleDurability durability() {
        return ShuffleDurability.DURABLE;
    }

    @Override
    public void close() {
        objects.close();
    }

    static String segmentKey(String queryId, int stageId, int partitionId,
                             int mapId, long attemptId) {
        return partitionPrefix(queryId, stageId, partitionId)
                + "m" + mapId + "-a" + attemptId + ".arrow";
    }

    static String partitionPrefix(String queryId, int stageId, int partitionId) {
        return queryId + "/s" + stageId + "/p" + partitionId + "/";
    }

    static void validateQueryId(String queryId) {
        if (queryId == null
                || ".".equals(queryId)
                || "..".equals(queryId)
                || !queryId.matches("[a-zA-Z0-9_\\-.]+")) {
            throw new IllegalArgumentException(
                    "Invalid queryId (must match [a-zA-Z0-9_\\-.]+, not '.' or '..'): " + queryId);
        }
    }
}
