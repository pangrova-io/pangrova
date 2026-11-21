package org.pangrova.indexing.build;

import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;
import java.util.concurrent.ExecutorService;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.connector.api.index.SnapshotPinProvider;

public record IndexBuildContext(
        IndexingConfig config,
        IndexEngineRegistry engineRegistry,
        IndexStorageManager storageManager,
        IndexSerializer serializer,
        IndexBuildClaimRepository claimRepository,
        GenerationChecker generationChecker,
        SnapshotPinProvider snapshotPinProvider,
        ExecutorService buildExecutor) {
    public IndexBuildContext {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
    }

    public IndexBuildContext(
            IndexingConfig config,
            IndexEngineRegistry engineRegistry,
            IndexStorageManager storageManager,
            IndexSerializer serializer) {
        this(config, engineRegistry, storageManager, serializer, null, null, null, null);
    }

    public boolean canBuild() {
        return engineRegistry != null && storageManager != null && serializer != null;
    }
}
