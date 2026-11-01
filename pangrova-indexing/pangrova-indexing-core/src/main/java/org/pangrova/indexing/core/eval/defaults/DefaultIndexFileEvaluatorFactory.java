package org.pangrova.indexing.core.eval.defaults;

import org.pangrova.indexing.assembly.serde.DefaultTypeSerializers;

import org.pangrova.indexing.core.registry.IndexEngineRegistry;
import org.pangrova.indexing.core.config.IndexingConfig;
import org.pangrova.indexing.core.eval.IndexEvaluationService;
import org.pangrova.indexing.core.serde.IndexSerializer;
import org.pangrova.indexing.core.serde.TypeSerializerRegistry;
import org.pangrova.indexing.core.storage.IndexCache;
import org.pangrova.indexing.core.storage.IndexStorageManager;
import org.pangrova.spi.indexing.eval.IndexFileEvaluator;
import org.pangrova.spi.indexing.eval.IndexFileEvaluatorFactory;

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
        TypeSerializerRegistry serdeRegistry = DefaultTypeSerializers.createRegistry();
        IndexSerializer serializer = new IndexSerializer(serdeRegistry);
        IndexEvaluationService evaluationService = new IndexEvaluationService(
                engineRegistry, cache, storageManager, serializer);
        return new DefaultIndexFileEvaluator(evaluationService);
    }
}
