package org.pangrova.indexing.core.build;

import java.util.concurrent.ExecutorService;
import org.pangrova.indexing.core.registry.IndexEngineRegistry;
import org.pangrova.indexing.core.config.IndexingConfig;
import org.pangrova.indexing.core.serde.IndexSerializer;
import org.pangrova.indexing.core.storage.IndexStorageManager;
import org.pangrova.spi.connector.index.SnapshotPinProvider;

public record IndexBuildContext(
        IndexingConfig config,
        IndexEngineRegistry engineRegistry,
        IndexStorageManager storageManager,
        IndexSerializer serializer,
        BuildClaimHandler claimHandler,
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
