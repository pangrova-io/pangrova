/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.indexing.engine.vector;

import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.index.vector.VectorSearchRequest;
import org.pangrova.spi.index.vector.VectorSearchResult;
import org.pangrova.spi.index.vector.DistanceMetric;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultVectorIndexLoaderBruteForceTest {

    private static final String FILE = "/data/table/part-00001.parquet";
    private static final String COLUMN = "embedding";
    private static final long CACHE_SIZE = 1024L * 1024;

    private static DefaultVectorIndexLoader createLoader() {
        return new DefaultVectorIndexLoader(
                new IndexEngineRegistry(),
                new IndexCache(CACHE_SIZE),
                new IndexStorageManager(),
                new IndexSerializer(TypeSerializerRegistry.createDefault(), true),
                new StubFileSystemAccessor(),
                (dataFilePath, vectorColumn) -> dataFilePath + ".vector." + vectorColumn + ".idx",
                null);
    }

    /** Minimal FileSystemAccessor for test construction. */
    private static class StubFileSystemAccessor implements FileSystemAccessor {
        @Override public java.io.InputStream openInputStream(String path) { return java.io.InputStream.nullInputStream(); }
        @Override public java.io.OutputStream openOutputStream(String path) { return java.io.OutputStream.nullOutputStream(); }
        @Override public boolean exists(String path) { return false; }
        @Override public long fileSize(String path) { return 0; }
        @Override public void deleteFile(String path) {}
        @Override public java.util.List<String> listFiles(String dir) { return java.util.List.of(); }
        @Override public void close() {}
    }

    // ---- Fail-fast constructor validation ----

    @Test
    void constructor_failsFast_whenStorageManagerNull() {
        assertThrows(IllegalStateException.class, () ->
                new DefaultVectorIndexLoader(
                        new IndexEngineRegistry(), new IndexCache(CACHE_SIZE),
                        null,
                        new IndexSerializer(TypeSerializerRegistry.createDefault(), true),
                        new StubFileSystemAccessor(),
                        (f, c) -> f + ".idx", null));
    }

    @Test
    void constructor_failsFast_whenSerializerNull() {
        assertThrows(IllegalStateException.class, () ->
                new DefaultVectorIndexLoader(
                        new IndexEngineRegistry(), new IndexCache(CACHE_SIZE),
                        new IndexStorageManager(), null,
                        new StubFileSystemAccessor(),
                        (f, c) -> f + ".idx", null));
    }

    @Test
    void constructor_failsFast_whenFsAccessorNull() {
        assertThrows(IllegalStateException.class, () ->
                new DefaultVectorIndexLoader(
                        new IndexEngineRegistry(), new IndexCache(CACHE_SIZE),
                        new IndexStorageManager(),
                        new IndexSerializer(TypeSerializerRegistry.createDefault(), true),
                        null, (f, c) -> f + ".idx", null));
    }

    @Test
    void constructor_failsFast_whenPathResolverNull() {
        assertThrows(IllegalStateException.class, () ->
                new DefaultVectorIndexLoader(
                        new IndexEngineRegistry(), new IndexCache(CACHE_SIZE),
                        new IndexStorageManager(),
                        new IndexSerializer(TypeSerializerRegistry.createDefault(), true),
                        new StubFileSystemAccessor(), null, null));
    }

    // ---- searchFile returns empty when no index available ----

    @Test
    void searchFile_returnsEmpty_whenNoIndexCachedOrStored() {
        DefaultVectorIndexLoader loader = createLoader();
        VectorSearchRequest request = new VectorSearchRequest(
                COLUMN, new float[]{0.0f, 0.0f}, 5, DistanceMetric.L2);

        VectorSearchResult result = loader.searchFile(FILE, COLUMN, request, Map.of());

        assertEquals(FILE, result.dataFilePath());
        assertTrue(result.neighbors().isEmpty(),
                "searchFile should return empty when no index is cached or in storage");
    }

    @Test
    void searchFile_returnsDataFilePath_inResult() {
        DefaultVectorIndexLoader loader = createLoader();
        VectorSearchRequest request = new VectorSearchRequest(
                COLUMN, new float[]{1.0f, 2.0f}, 3, DistanceMetric.L2);

        VectorSearchResult result = loader.searchFile(FILE, COLUMN, request, Map.of());

        assertEquals(FILE, result.dataFilePath());
    }
}
