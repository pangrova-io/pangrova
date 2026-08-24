package org.pangrova.coordinator.index;

import org.pangrova.indexing.build.BuildFileMapping;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(BuildMappingPersister.class);

    private BuildMappingPersister() {}

    /**
     * Converts and persists a batch of build file mappings.
     *
     * @param store      metadata store for persistence
     * @param batch      build file mappings from the build coordinator
     * @param generation rebuild generation counter
     */
    public static void persist(IndexMetadataStore store,
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
    public static void persist(IndexMetadataStore store,
                               List<BuildFileMapping> batch,
                               int generation,
                               String snapshotToken) {
        try {
            Instant now = Instant.now();
            long verifiedAt = System.currentTimeMillis();
            String token = snapshotToken != null ? snapshotToken : "";
            List<IndexFileMapping> mappings = new ArrayList<>(batch.size());
            for (BuildFileMapping bfm : batch) {
                mappings.add(new IndexFileMapping(
                        bfm.indexId(), bfm.dataFilePath(), bfm.indexFilePath(),
                        bfm.rowCount(), bfm.indexSizeBytes(), bfm.checksum(),
                        generation, token, 0L, now, verifiedAt));
            }
            store.batchRegisterIndexFiles(mappings);
        } catch (Exception e) {
            LOG.warn("action=persist_file_mappings_failed batch={} error={}",
                    batch.size(), e.getMessage());
        }
    }
}
