package org.pangrova.indexing.engine.vector;

import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link DefaultVectorIndexLoader} storage fallback behavior.
 *
 * <p>Verifies: cold start (empty cache) → load from storage → cache populated →
 * second load hits cache.</p>
 */
class DefaultVectorIndexLoaderTest {

    private static final String DATA_FILE = "/data/warehouse/table/part-00001.parquet";
    private static final String VECTOR_COLUMN = "embedding";
    private static final String INDEX_FILE_PATH = "/indexes/v1/catalog/schema/table/idx-001/abc123.idx";
    private static final int DIMENSION = 8;
    private static final int VECTOR_COUNT = 10;

    private IndexCache cache;
    private IndexSerializer serializer;
    private IndexStorageManager storageManager;
    private InMemoryFileSystemAccessor fsAccessor;
    private DefaultVectorIndexLoader loader;

    @BeforeEach
    void setUp() {
        cache = new IndexCache(16L * 1024 * 1024); // 16 MB
        serializer = new IndexSerializer(TypeSerializerRegistry.createDefault());
        storageManager = new IndexStorageManager();
        fsAccessor = new InMemoryFileSystemAccessor();

        DefaultVectorIndexLoader.IndexFilePathResolver pathResolver =
                (dataFilePath, vectorColumn) -> INDEX_FILE_PATH;

        loader = new DefaultVectorIndexLoader(
                new IndexEngineRegistry(),
                cache,
                storageManager,
                serializer,
                fsAccessor,
                pathResolver,
                null);
    }

    @Test
    void coldStartLoadsFromStorageAndPopulatesCache() {
        VectorIndexData indexData = createTestVectorIndex();
        byte[] serializedBytes = serializer.serialize(indexData);
        fsAccessor.writeFile(INDEX_FILE_PATH, serializedBytes);

        // Verify cache is empty (cold start)
        assertThat(cache.size()).isZero();

        // First search: cache miss → loads from storage
        VectorSearchResult result = loader.searchFile(
                DATA_FILE, VECTOR_COLUMN, createSearchRequest(), Map.of());

        assertThat(result).isNotNull();
        assertThat(result.dataFilePath()).isEqualTo(DATA_FILE);

        // Verify cache is now populated
        assertThat(cache.size()).isEqualTo(1);
        String cacheKey = "vector:" + VECTOR_COLUMN;
        assertThat(cache.get(cacheKey, DATA_FILE, 0)).isNotNull();
        assertThat(cache.get(cacheKey, DATA_FILE, 0)).isInstanceOf(VectorIndexData.class);
    }

    @Test
    void secondLoadHitsCache() {
        VectorIndexData indexData = createTestVectorIndex();
        byte[] serializedBytes = serializer.serialize(indexData);
        fsAccessor.writeFile(INDEX_FILE_PATH, serializedBytes);

        // First load: populates cache
        loader.searchFile(DATA_FILE, VECTOR_COLUMN, createSearchRequest(), Map.of());
        long missCountAfterFirst = cache.missCount();

        // Second load: should hit cache (no additional miss)
        loader.searchFile(DATA_FILE, VECTOR_COLUMN, createSearchRequest(), Map.of());
        long missCountAfterSecond = cache.missCount();

        assertThat(missCountAfterSecond).isEqualTo(missCountAfterFirst);
        assertThat(cache.hitCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void returnsEmptyWhenIndexNotInStorage() {
        // No index file written to storage
        VectorSearchResult result = loader.searchFile(
                DATA_FILE, VECTOR_COLUMN, createSearchRequest(), Map.of());

        assertThat(result).isNotNull();
        assertThat(result.dataFilePath()).isEqualTo(DATA_FILE);
        assertThat(result.neighbors()).isEmpty();
        assertThat(cache.size()).isZero();
    }

    @Test
    void returnsEmptyWhenPathResolverReturnsNull() {
        DefaultVectorIndexLoader loaderNullPath = new DefaultVectorIndexLoader(
                new IndexEngineRegistry(),
                cache,
                storageManager,
                serializer,
                fsAccessor,
                (dataFilePath, vectorColumn) -> null,
                null);

        VectorSearchResult result = loaderNullPath.searchFile(
                DATA_FILE, VECTOR_COLUMN, createSearchRequest(), Map.of());

        assertThat(result).isNotNull();
        assertThat(result.neighbors()).isEmpty();
    }

    @Test
    void cacheEvictsWhenSizeExceeded() {
        // Build a real index and measure its size
        VectorIndexData indexData = createTestVectorIndex();
        byte[] serializedBytes = serializer.serialize(indexData);
        long indexSize = indexData.estimatedSizeInBytes();

        // Cache can hold exactly one index entry (not two)
        IndexCache smallCache = new IndexCache(indexSize + 64);
        DefaultVectorIndexLoader smallLoader = new DefaultVectorIndexLoader(
                new IndexEngineRegistry(),
                smallCache,
                storageManager,
                serializer,
                fsAccessor,
                (dataFilePath, vectorColumn) ->
                        "/indexes/" + dataFilePath + "/" + vectorColumn + ".idx",
                null);

        // Write two index files to storage
        String path1 = "/indexes//data/file1.parquet/embedding.idx";
        String path2 = "/indexes//data/file2.parquet/embedding.idx";
        fsAccessor.writeFile(path1, serializedBytes);
        fsAccessor.writeFile(path2, serializedBytes);

        // Load first index — populates cache
        smallLoader.searchFile("/data/file1.parquet", "embedding",
                createSearchRequest(), Map.of());
        assertThat(smallCache.size()).isEqualTo(1);

        // Load second index — should trigger eviction of first
        smallLoader.searchFile("/data/file2.parquet", "embedding",
                createSearchRequest(), Map.of());

        // Caffeine evicts based on weight — at least one eviction should have occurred
        smallCache.size(); // trigger cleanup
        assertThat(smallCache.evictionCount()).isGreaterThanOrEqualTo(1);
    }

    private static VectorIndexData createTestVectorIndex() {
        VectorStrategyProvider provider = new VectorStrategyProvider();
        List<List<Comparable<?>>> rows = new ArrayList<>(VECTOR_COUNT);
        for (int i = 0; i < VECTOR_COUNT; i++) {
            float[] vec = new float[DIMENSION];
            for (int d = 0; d < DIMENSION; d++) {
                vec[d] = (float) (i * 0.1 + d * 0.01);
            }
            List<Float> floatList = new ArrayList<>(DIMENSION);
            for (float f : vec) {
                floatList.add(f);
            }
            rows.add(List.of(new ComparableFloatList(floatList)));
        }
        Map<String, String> props = Map.of(
                "algorithm", "HNSW", "dimension", String.valueOf(DIMENSION),
                "metric", "L2", "hnsw.M", "8", "hnsw.efConstruction", "50");
        return (VectorIndexData) provider.build(rows.iterator(), props);
    }

    private static VectorSearchRequest createSearchRequest() {
        float[] queryVector = new float[DIMENSION];
        for (int d = 0; d < DIMENSION; d++) {
            queryVector[d] = 0.5f;
        }
        return new VectorSearchRequest(VECTOR_COLUMN, queryVector, 5, DistanceMetric.L2);
    }

    /**
     * In-memory implementation of {@link FileSystemAccessor} for unit testing
     * without real S3/HDFS infrastructure.
     */
    private static final class InMemoryFileSystemAccessor implements FileSystemAccessor {

        private final ConcurrentHashMap<String, byte[]> files = new ConcurrentHashMap<>();

        void writeFile(String path, byte[] data) {
            files.put(path, data.clone());
        }

        @Override
        public InputStream openInputStream(String path) throws IOException {
            byte[] data = files.get(path);
            if (data == null) {
                throw new IOException("File not found: " + path);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public OutputStream openOutputStream(String path) {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    super.close();
                    files.put(path, toByteArray());
                }
            };
        }

        @Override
        public void deleteFile(String path) {
            files.remove(path);
        }

        @Override
        public boolean exists(String path) {
            return files.containsKey(path);
        }

        @Override
        public long fileSize(String path) throws IOException {
            byte[] data = files.get(path);
            if (data == null) {
                throw new IOException("File not found: " + path);
            }
            return data.length;
        }

        @Override
        public List<String> listFiles(String directoryPath) {
            return files.keySet().stream()
                    .filter(p -> p.startsWith(directoryPath))
                    .toList();
        }

        @Override
        public void close() {
            files.clear();
        }
    }

    /**
     * A List that implements Comparable for passing through the vector build API.
     */
    private static final class ComparableFloatList extends ArrayList<Float>
            implements Comparable<ComparableFloatList> {

        ComparableFloatList(List<Float> values) {
            super(values);
        }

        @Override
        public int compareTo(ComparableFloatList other) {
            return Integer.compare(this.size(), other.size());
        }
    }
}
