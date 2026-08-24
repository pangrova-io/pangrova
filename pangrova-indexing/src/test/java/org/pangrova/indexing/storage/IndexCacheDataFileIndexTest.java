package org.pangrova.indexing.storage;

import org.pangrova.spi.index.data.BloomFilterIndexData;
import org.pangrova.spi.index.data.IndexData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IndexCache} dataFileIndex secondary index.
 * Validates O(k) invalidation by dataFilePath and cross-index cleanup
 * between indexIdIndex and dataFileIndex.
 */
class IndexCacheDataFileIndexTest {

    private static IndexData testData(int marker) {
        return new BloomFilterIndexData(new byte[]{(byte) marker}, 3, 8);
    }

    @Test
    void invalidateDataFileRemovesAllEntriesForPath() {
        IndexCache cache = new IndexCache(1024 * 1024);
        String targetFile = "/data/file-A.parquet";

        cache.put("idx-1", targetFile, 0, testData(1));
        cache.put("idx-2", targetFile, 0, testData(2));
        cache.put("idx-1", "/data/file-B.parquet", 0, testData(3));

        cache.invalidateDataFile(targetFile);

        assertThat(cache.get("idx-1", targetFile, 0)).isNull();
        assertThat(cache.get("idx-2", targetFile, 0)).isNull();
        assertThat(cache.get("idx-1", "/data/file-B.parquet", 0)).isNotNull();
    }

    @Test
    void invalidateDataFileCleansUpIndexIdIndex() {
        IndexCache cache = new IndexCache(1024 * 1024);
        String targetFile = "/data/file-A.parquet";

        cache.put("idx-1", targetFile, 0, testData(1));
        cache.put("idx-1", "/data/file-B.parquet", 0, testData(2));

        cache.invalidateDataFile(targetFile);

        // idx-1 still has file-B, so invalidateIndex should only remove file-B
        cache.invalidateIndex("idx-1");
        assertThat(cache.get("idx-1", "/data/file-B.parquet", 0)).isNull();
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void invalidateIndexCleansUpDataFileIndex() {
        IndexCache cache = new IndexCache(1024 * 1024);
        String file = "/data/file-A.parquet";

        cache.put("idx-1", file, 0, testData(1));
        cache.put("idx-2", file, 0, testData(2));

        cache.invalidateIndex("idx-1");

        // idx-2 entry for the same file should still exist
        assertThat(cache.get("idx-2", file, 0)).isNotNull();
        assertThat(cache.get("idx-1", file, 0)).isNull();

        // invalidateDataFile should now only remove idx-2's entry
        cache.invalidateDataFile(file);
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void invalidateDataFileForNonExistentPathIsNoOp() {
        IndexCache cache = new IndexCache(1024 * 1024);
        cache.put("idx-1", "/data/file-A.parquet", 0, testData(1));

        cache.invalidateDataFile("/data/non-existent.parquet");

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get("idx-1", "/data/file-A.parquet", 0)).isNotNull();
    }

    @Test
    void invalidateDataFileHandlesMultipleGenerations() {
        IndexCache cache = new IndexCache(1024 * 1024);
        String file = "/data/file-A.parquet";

        cache.put("idx-1", file, 0, testData(1));
        cache.put("idx-1", file, 1, testData(2));

        cache.invalidateDataFile(file);

        assertThat(cache.get("idx-1", file, 0)).isNull();
        assertThat(cache.get("idx-1", file, 1)).isNull();
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    void olderGenerationCleanupMaintainsDataFileIndex() {
        IndexCache cache = new IndexCache(1024 * 1024);
        String file = "/data/file-A.parquet";

        cache.put("idx-1", file, 0, testData(1));

        // Cache miss with newer generation triggers older generation cleanup
        IndexData result = cache.get("idx-1", file, 2);
        assertThat(result).isNull();

        // Old generation entry should be gone
        assertThat(cache.get("idx-1", file, 0)).isNull();

        // dataFileIndex should be cleaned up too — invalidateDataFile should be no-op
        cache.invalidateDataFile(file);
        assertThat(cache.size()).isEqualTo(0);
    }
}
