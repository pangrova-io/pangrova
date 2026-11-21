package org.pangrova.indexing.build;

import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMappings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link BuildFileMapping} batches to {@link IndexFileMapping}
 * records and persists them to the metadata store.
 *
 * <p>Extracted to eliminate triple duplication across
 * {@code IndexDdlExecutor}, {@code IndexCommitHandler}, and
 * {@code IndexConsistencyChecker}.</p>
 */
public final class BuildMappingPersister {

    private BuildMappingPersister() {}

    /**
     * Converts and persists a batch of build file mappings.
     *
     * @param store      metadata store for persistence
     * @param batch      build file mappings from the build coordinator
     * @param generation rebuild generation counter
     */
    public static void persist(IndexFileMappings store,
                               List<BuildFileMapping> batch,
                               int generation) {
        persist(store, batch, generation, "");
    }

    /**
     * Converts and persists a batch of build file mappings with snapshot token.
     *
     * @param store         metadata store for persistence
     * @param batch         build file mappings from the build coordinator
     * @param generation    rebuild generation counter
     * @param snapshotToken connector snapshot ID for consistency filtering
     */
    public static void persist(IndexFileMappings store,
                               List<BuildFileMapping> batch,
                               int generation,
                               String snapshotToken) {
        if (batch.isEmpty()) return;
        var attemptId = batch.get(0).attemptId();
        Instant now = Instant.now();
        long verifiedAt = System.currentTimeMillis();
        String token = snapshotToken != null ? snapshotToken : "";
        List<IndexFileMapping> mappings = new ArrayList<>(batch.size());
        long sequence = 0;
        for (BuildFileMapping bfm : batch) {
            if (!attemptId.equals(bfm.attemptId())) {
                throw new IllegalArgumentException("Mapping batch spans build attempts");
            }
            mappings.add(new IndexFileMapping(
                    bfm.indexId(), bfm.dataFilePath(), bfm.indexFilePath(),
                    bfm.rowCount(), bfm.indexSizeBytes(), bfm.checksum(),
                    generation, token, sequence++, now, verifiedAt));
        }
        store.batchRegisterIndexFiles(attemptId.toString(), mappings);
    }
}
