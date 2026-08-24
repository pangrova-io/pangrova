package org.pangrova.indexing.eval;

import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.spi.index.eval.IndexFileEvaluator;
import org.pangrova.spi.index.eval.IndexFileEvaluatorFactory;

/**
 * Default factory that self-bootstraps all indexing dependencies.
 *
 * <p>Discovered via ServiceLoader at worker startup. Creates a fully
 * initialized {@link DefaultIndexFileEvaluator} backed by the standard
 * engine registry, cache, storage manager, and serializer.</p>
 *
 * <p>No-arg constructor required by ServiceLoader contract.</p>
 */
public final class DefaultIndexFileEvaluatorFactory implements IndexFileEvaluatorFactory {

    @Override
    public IndexFileEvaluator create() {
        String baseIndexPath = System.getProperty("pangrova.index.basePath");
        if (baseIndexPath == null || baseIndexPath.isBlank()) {
            throw new IllegalStateException(
                    "pangrova.index.basePath system property is required but not set");
        }
        IndexEngineRegistry engineRegistry = new IndexEngineRegistry();
        IndexingConfig config = new IndexingConfig(
                baseIndexPath, true, null, null);
        IndexCache cache = new IndexCache(config.cacheSizeBytes());
        IndexStorageManager storageManager = new IndexStorageManager();
        TypeSerializerRegistry serdeRegistry = TypeSerializerRegistry.createDefault();
        IndexSerializer serializer = new IndexSerializer(serdeRegistry);
        IndexEvaluationService evaluationService = new IndexEvaluationService(
                engineRegistry, cache, storageManager, serializer);
        return new DefaultIndexFileEvaluator(evaluationService);
    }
}
