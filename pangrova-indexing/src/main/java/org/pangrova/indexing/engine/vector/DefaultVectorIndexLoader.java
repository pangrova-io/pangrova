package org.pangrova.indexing.engine.vector;

import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.spi.connector.index.ColumnValueReader;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.IndexStrategyProvider;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.data.IndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.vector.VectorIndexLoader;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation of {@link VectorIndexLoader} SPI.
 *
 * <p>Bridges Worker-side ANN search requests to the indexing subsystem
 * ({@link IndexEngineRegistry}, {@link IndexCache}).
 * Discovered by Worker via {@link java.util.ServiceLoader}.</p>
 *
 * <p>Search strategy:
 * <ol>
 *   <li>Cache lookup — returns immediately on hit</li>
 *   <li>Storage fallback — loads from object storage (S3/HDFS) on cache miss</li>
 *   <li>Cache population — populates cache after successful storage load</li>
 * </ol>
 *
 * <p>Cache eviction is handled by Caffeine's W-TinyLFU policy (size-based with
 * frequency-aware admission). The maximum cache size is configurable at construction.</p>
 *
 * <p>No-arg constructor for {@link java.util.ServiceLoader} discovery.
 * Parameterized constructor for explicit wiring and testing.</p>
 */
public final class DefaultVectorIndexLoader implements VectorIndexLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultVectorIndexLoader.class);

    private final IndexEngineRegistry engineRegistry;
    private final IndexCache cache;
    private final IndexStorageManager storageManager;
    private final IndexSerializer serializer;
    private final FileSystemAccessor fsAccessor;
    private final IndexFilePathResolver pathResolver;
    private final ColumnValueReader columnValueReader;

    /** Default cache size for ServiceLoader-discovered instances: 64 MB. */
    private static final long DEFAULT_CACHE_SIZE_BYTES = 64L * 1024 * 1024;

    /** Default max index file size for vector indexes: 512 MB (larger than scalar indexes). */
    private static final long DEFAULT_VECTOR_MAX_INDEX_FILE_BYTES = 512L * 1024 * 1024;

    static final String GENERATION_PARAM = "_index_generation";

    /**
     * Resolves the storage path of a vector index file given a data file path
     * and vector column name.
     */
    @FunctionalInterface
    public interface IndexFilePathResolver {
        /**
         * Resolves the index file path in object storage for the given data file and column.
         *
         * @param dataFilePath the data file path
         * @param vectorColumn the vector column name
         * @return the index file path in storage, or null if unknown
         */
        String resolve(String dataFilePath, String vectorColumn);
    }

    /**
     * No-arg constructor for ServiceLoader discovery.
     * Discovers FileSystemAccessor via ServiceLoader; fails fast if not found
     * (build-to-read chain requires storage access).
     */
    public DefaultVectorIndexLoader() {
        this(new IndexEngineRegistry(), new IndexCache(DEFAULT_CACHE_SIZE_BYTES),
                new IndexStorageManager(DEFAULT_VECTOR_MAX_INDEX_FILE_BYTES),
                new IndexSerializer(
                        org.pangrova.indexing.serde.TypeSerializerRegistry.createDefault(), true),
                requireFileSystemAccessor(),
                defaultPathResolver(),
                discoverColumnValueReader());
    }

    /**
     * Full constructor with storage fallback support.
     *
     * @param engineRegistry index engine registry for vector search
     * @param cache          in-memory index cache (size-based eviction via Caffeine W-TinyLFU)
     * @param storageManager reads serialized index files from object storage
     * @param serializer     deserializes index data from bytes
     * @param fsAccessor     file system accessor for S3/HDFS access
     * @param pathResolver   resolves index file paths from data file + column
     */
    public DefaultVectorIndexLoader(IndexEngineRegistry engineRegistry,
                                    IndexCache cache,
                                    IndexStorageManager storageManager,
                                    IndexSerializer serializer,
                                    FileSystemAccessor fsAccessor,
                                    IndexFilePathResolver pathResolver,
                                    ColumnValueReader columnValueReader) {
        if (storageManager == null) {
            throw new IllegalStateException(
                    "IndexStorageManager must not be null; vector index build->read chain requires storage");
        }
        if (serializer == null) {
            throw new IllegalStateException(
                    "IndexSerializer must not be null; vector index deserialization requires a serializer");
        }
        if (fsAccessor == null) {
            throw new IllegalStateException(
                    "FileSystemAccessor must not be null; vector index read path requires storage access. "
                    + "Ensure a FileSystemAccessor SPI implementation is on the classpath.");
        }
        if (pathResolver == null) {
            throw new IllegalStateException(
                    "IndexFilePathResolver must not be null");
        }
        this.engineRegistry = engineRegistry;
        this.cache = cache;
        this.storageManager = storageManager;
        this.serializer = serializer;
        this.fsAccessor = fsAccessor;
        this.pathResolver = pathResolver;
        this.columnValueReader = columnValueReader;
    }

    @Override
    public VectorSearchResult searchFile(String dataFilePath,
                                         String vectorColumn,
                                         VectorSearchRequest request,
                                         Map<String, String> searchParams) {
        boolean forceBruteForce = "brute_force".equals(searchParams != null
                ? searchParams.get("mode") : null);
        int generation = parseGeneration(searchParams);
        VectorIndexData indexData = forceBruteForce ? null
                : loadVectorIndex(dataFilePath, vectorColumn, generation);
        if (indexData != null) {
            IndexStrategyProvider provider = engineRegistry.getProvider(IndexType.VECTOR);
            List<VectorSearchResult.ScoredRow> neighbors =
                    provider.vectorSearch(indexData, request, searchParams);
            return new VectorSearchResult(dataFilePath, neighbors);
        }
        if (columnValueReader != null) {
            return bruteForceSearch(dataFilePath, vectorColumn, request);
        }
        LOG.warn("event=vector_search_no_index file={} column={} "
                + "action=signaling_no_index reason=index_not_found_in_cache_or_storage",
                dataFilePath, vectorColumn);
        return new VectorSearchResult(dataFilePath, List.of(), false);
    }

    private VectorSearchResult bruteForceSearch(String dataFilePath, String vectorColumn,
                                                VectorSearchRequest request) {
        try {
            Iterator<List<Comparable<?>>> rows = columnValueReader.readColumns(
                    dataFilePath, List.of(vectorColumn));
            List<VectorSearchResult.ScoredRow> neighbors =
                    BruteForceVectorScanner.scan(rows, dataFilePath, request);
            LOG.info("event=brute_force_fallback file={} column={} results={}",
                    dataFilePath, vectorColumn, neighbors.size());
            return new VectorSearchResult(dataFilePath, neighbors, false);
        } catch (IOException e) {
            LOG.warn("event=brute_force_fallback_failed file={} column={} error={}",
                    dataFilePath, vectorColumn, e.getMessage());
            return new VectorSearchResult(dataFilePath, List.of(), false);
        }
    }

    private VectorIndexData loadVectorIndex(String dataFilePath, String vectorColumn,
                                               int generation) {
        String cacheKey = "vector:" + vectorColumn;
        IndexData cached = cache.get(cacheKey, dataFilePath, generation);
        if (cached instanceof VectorIndexData vid) {
            return vid;
        }
        LOG.debug("event=vector_index_cache_miss file={} column={}", dataFilePath, vectorColumn);
        return loadFromStorage(dataFilePath, vectorColumn, cacheKey, generation);
    }

    /**
     * Loads vector index data from object storage and populates the cache on success.
     * Returns null if storage is not configured, path cannot be resolved, or load fails.
     */
    private VectorIndexData loadFromStorage(String dataFilePath, String vectorColumn,
                                            String cacheKey, int generation) {
        // Storage is guaranteed configured by constructor validation
        String indexFilePath = pathResolver.resolve(dataFilePath, vectorColumn);
        if (indexFilePath == null) {
            LOG.debug("event=index_path_unresolved file={} column={}",
                    dataFilePath, vectorColumn);
            return null;
        }

        Optional<byte[]> rawBytes = storageManager.readIndex(fsAccessor, indexFilePath);
        if (rawBytes.isEmpty()) {
            LOG.debug("event=index_not_in_storage file={} indexPath={}",
                    dataFilePath, indexFilePath);
            return null;
        }

        try {
            Optional<IndexData> deserialized = serializer.deserialize(rawBytes.get());
            if (deserialized.isEmpty()) {
                LOG.debug("event=index_version_unsupported file={} indexPath={}",
                        dataFilePath, indexFilePath);
                return null;
            }
            IndexData data = deserialized.get();
            if (!(data instanceof VectorIndexData vid)) {
                LOG.warn("event=index_type_mismatch file={} indexPath={} "
                        + "expectedType=VECTOR actualType={}",
                        dataFilePath, indexFilePath, data.indexType());
                return null;
            }
            cache.put(cacheKey, dataFilePath, generation, vid);
            LOG.debug("event=vector_index_loaded_from_storage file={} column={} sizeBytes={}",
                    dataFilePath, vectorColumn, vid.estimatedSizeInBytes());
            return vid;
        } catch (Exception e) {
            LOG.warn("event=vector_index_deserialize_failed file={} indexPath={} error={}",
                    dataFilePath, indexFilePath, e.getMessage());
            return null;
        }
    }

    private static FileSystemAccessor requireFileSystemAccessor() {
        try {
            java.util.ServiceLoader<FileSystemAccessor> loader =
                    java.util.ServiceLoader.load(FileSystemAccessor.class);
            for (FileSystemAccessor accessor : loader) {
                LOG.info("event=file_system_accessor_discovered impl={}",
                        accessor.getClass().getName());
                return accessor;
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to discover FileSystemAccessor via ServiceLoader; "
                    + "vector index read path is broken without storage access", e);
        }
        throw new IllegalStateException(
                "No FileSystemAccessor found via ServiceLoader; "
                + "vector index build->read chain requires a FileSystemAccessor on the classpath");
    }

    /** Default path resolver: convention-based path from data file + column. */
    private static IndexFilePathResolver defaultPathResolver() {
        return (dataFilePath, vectorColumn) ->
                dataFilePath + ".vector." + vectorColumn + ".idx";
    }

    /** Discovers a ColumnValueReader via ServiceLoader; returns null if none available. */
    private static ColumnValueReader discoverColumnValueReader() {
        try {
            java.util.ServiceLoader<ColumnValueReader> loader =
                    java.util.ServiceLoader.load(ColumnValueReader.class);
            for (ColumnValueReader reader : loader) {
                LOG.info("event=column_value_reader_discovered impl={} brute_force_fallback=enabled",
                        reader.getClass().getName());
                return reader;
            }
        } catch (Exception e) {
            LOG.debug("event=column_value_reader_discovery_failed reason={}", e.getMessage());
        }
        LOG.info("event=column_value_reader_not_found brute_force_fallback=disabled");
        return null;
    }

    private static int parseGeneration(Map<String, String> searchParams) {
        if (searchParams == null) {
            return 0;
        }
        String gen = searchParams.get(GENERATION_PARAM);
        if (gen == null) {
            return 0;
        }
        try {
            return Integer.parseInt(gen);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
