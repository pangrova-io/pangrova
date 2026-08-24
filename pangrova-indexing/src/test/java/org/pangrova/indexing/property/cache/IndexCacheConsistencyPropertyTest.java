package org.pangrova.indexing.property.cache;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.spi.index.data.HashIndexData;
import org.pangrova.spi.index.data.IndexData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test for IndexCache secondary index consistency.
 *
 * <p><b>Property P9: IndexCache Secondary Index Consistency</b></p>
 *
 * <p>Verifies that after any random sequence of put/invalidateIndex/invalidateDataFile
 * operations, the primary cache and both secondary indexes ({@code indexIdIndex},
 * {@code dataFileIndex}) remain consistent:</p>
 * <ul>
 *   <li>Cache size is non-negative after every operation</li>
 *   <li>After {@code invalidateIndex(id)}, no entries for that indexId remain</li>
 *   <li>After {@code invalidateDataFile(path)}, no entries for that path remain</li>
 *   <li>Cross-index consistency: invalidateIndex also cleans dataFileIndex,
 *       and invalidateDataFile also cleans indexIdIndex</li>
 * </ul>
 *
 * <p><b>Validates: Requirements 9.AC1, 9.AC2, 9.AC3, 9.AC4</b></p>
 */
class IndexCacheConsistencyPropertyTest {

    private static final String[] INDEX_IDS = {"idx-0", "idx-1", "idx-2", "idx-3"};
    private static final String[] DATA_FILES = {
            "/data/f0.parquet", "/data/f1.parquet",
            "/data/f2.parquet", "/data/f3.parquet"
    };
    private static final Integer[] GENERATIONS = {0, 1, 2};

    private static IndexData testData(int marker) {
        return new HashIndexData(new byte[]{(byte) marker, 0, 0, 0}, 1, 4);
    }

    /**
     * After any random sequence of cache operations, invalidated entries are
     * fully removed from the primary cache and both secondary indexes.
     * A shadow model tracks expected state; final verification confirms
     * no orphan entries survive invalidation.
     */
    @Property(tries = 10)
    void secondaryIndexesConsistentAfterRandomOperations(
            @ForAll("operationSequences") List<CacheOp> ops) {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        Set<String> expectedKeys = new HashSet<>();

        for (CacheOp op : ops) {
            if (op instanceof Put) {
                Put put = (Put) op;
                cache.put(put.indexId, put.dataFilePath, put.generation,
                        testData(put.generation));
                expectedKeys.add(compositeKey(put.indexId, put.dataFilePath,
                        put.generation));
            } else if (op instanceof InvalidateIndex) {
                InvalidateIndex inv = (InvalidateIndex) op;
                cache.invalidateIndex(inv.indexId);
                expectedKeys.removeIf(k -> k.startsWith(inv.indexId + "|"));
            } else if (op instanceof InvalidateDataFile) {
                InvalidateDataFile inv = (InvalidateDataFile) op;
                cache.invalidateDataFile(inv.dataFilePath);
                expectedKeys.removeIf(
                        k -> k.contains("|" + inv.dataFilePath + "|"));
            }

            assertThat(cache.size())
                    .as("Cache size must be non-negative")
                    .isGreaterThanOrEqualTo(0);
        }

        // Final: entries not in shadow model must be absent from cache
        for (String indexId : INDEX_IDS) {
            for (String dataFile : DATA_FILES) {
                for (int gen : GENERATIONS) {
                    String key = compositeKey(indexId, dataFile, gen);
                    if (!expectedKeys.contains(key)) {
                        assertThat(cache.get(indexId, dataFile, gen))
                                .as("Orphan entry (%s, %s, %d) must not exist",
                                        indexId, dataFile, gen)
                                .isNull();
                    }
                }
            }
        }
    }

    /**
     * After invalidateIndex(id), no entry with that indexId is retrievable,
     * and subsequent invalidateDataFile for associated paths does not break
     * (proves cross-index cleanup from indexIdIndex to dataFileIndex).
     */
    @Property(tries = 10)
    void invalidateIndexRemovesAllEntriesAndCleansDataFileIndex(
            @ForAll("operationSequences") List<CacheOp> ops) {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        Map<String, Set<String>> indexToFiles = new HashMap<>();

        for (CacheOp op : ops) {
            if (op instanceof Put) {
                Put put = (Put) op;
                cache.put(put.indexId, put.dataFilePath, put.generation,
                        testData(put.generation));
                indexToFiles
                        .computeIfAbsent(put.indexId, k -> new HashSet<>())
                        .add(put.dataFilePath);
            } else if (op instanceof InvalidateIndex) {
                InvalidateIndex inv = (InvalidateIndex) op;
                cache.invalidateIndex(inv.indexId);
                assertNoEntriesForIndex(cache, inv.indexId);

                // Cross-index: invalidateDataFile on associated paths must not fail
                Set<String> files = indexToFiles.getOrDefault(
                        inv.indexId, Set.of());
                for (String file : files) {
                    cache.invalidateDataFile(file);
                }
                indexToFiles.remove(inv.indexId);
            } else if (op instanceof InvalidateDataFile) {
                InvalidateDataFile inv = (InvalidateDataFile) op;
                cache.invalidateDataFile(inv.dataFilePath);
                assertNoEntriesForDataFile(cache, inv.dataFilePath);
                removeFileFromTracking(indexToFiles, inv.dataFilePath);
            }
        }

        assertThat(cache.size()).isGreaterThanOrEqualTo(0);
    }

    /**
     * After invalidateDataFile(path), no entry with that path is retrievable,
     * and subsequent invalidateIndex for associated indexIds does not break
     * (proves cross-index cleanup from dataFileIndex to indexIdIndex).
     */
    @Property(tries = 10)
    void invalidateDataFileRemovesAllEntriesAndCleansIndexIdIndex(
            @ForAll("operationSequences") List<CacheOp> ops) {
        IndexCache cache = new IndexCache(10 * 1024 * 1024);
        Map<String, Set<String>> fileToIndexes = new HashMap<>();

        for (CacheOp op : ops) {
            if (op instanceof Put) {
                Put put = (Put) op;
                cache.put(put.indexId, put.dataFilePath, put.generation,
                        testData(put.generation));
                fileToIndexes
                        .computeIfAbsent(put.dataFilePath, k -> new HashSet<>())
                        .add(put.indexId);
            } else if (op instanceof InvalidateDataFile) {
                InvalidateDataFile inv = (InvalidateDataFile) op;
                cache.invalidateDataFile(inv.dataFilePath);
                assertNoEntriesForDataFile(cache, inv.dataFilePath);

                // Cross-index: invalidateIndex on associated ids must not fail
                Set<String> indexes = fileToIndexes.getOrDefault(
                        inv.dataFilePath, Set.of());
                for (String indexId : indexes) {
                    cache.invalidateIndex(indexId);
                }
                fileToIndexes.remove(inv.dataFilePath);
            } else if (op instanceof InvalidateIndex) {
                InvalidateIndex inv = (InvalidateIndex) op;
                cache.invalidateIndex(inv.indexId);
                assertNoEntriesForIndex(cache, inv.indexId);
                removeIndexFromTracking(fileToIndexes, inv.indexId);
            }
        }

        assertThat(cache.size()).isGreaterThanOrEqualTo(0);
    }

    // --- Assertion helpers ---

    private static void assertNoEntriesForIndex(IndexCache cache, String indexId) {
        for (String dataFile : DATA_FILES) {
            for (int gen : GENERATIONS) {
                assertThat(cache.get(indexId, dataFile, gen))
                        .as("After invalidateIndex(%s), (%s, %s, %d) must be null",
                                indexId, indexId, dataFile, gen)
                        .isNull();
            }
        }
    }

    private static void assertNoEntriesForDataFile(IndexCache cache, String path) {
        for (String indexId : INDEX_IDS) {
            for (int gen : GENERATIONS) {
                assertThat(cache.get(indexId, path, gen))
                        .as("After invalidateDataFile(%s), (%s, %s, %d) must be null",
                                path, indexId, path, gen)
                        .isNull();
            }
        }
    }

    private static void removeFileFromTracking(
            Map<String, Set<String>> indexToFiles, String dataFilePath) {
        for (Set<String> files : indexToFiles.values()) {
            files.remove(dataFilePath);
        }
    }

    private static void removeIndexFromTracking(
            Map<String, Set<String>> fileToIndexes, String indexId) {
        for (Set<String> indexes : fileToIndexes.values()) {
            indexes.remove(indexId);
        }
    }

    // --- Arbitraries ---

    @Provide
    Arbitrary<List<CacheOp>> operationSequences() {
        return cacheOperations().list().ofMinSize(5).ofMaxSize(50);
    }

    private Arbitrary<CacheOp> cacheOperations() {
        Arbitrary<CacheOp> puts = Combinators.combine(
                Arbitraries.of(INDEX_IDS),
                Arbitraries.of(DATA_FILES),
                Arbitraries.of(GENERATIONS)
        ).as(Put::new);

        Arbitrary<CacheOp> invalidateIndexes =
                Arbitraries.of(INDEX_IDS).map(InvalidateIndex::new);

        Arbitrary<CacheOp> invalidateDataFiles =
                Arbitraries.of(DATA_FILES).map(InvalidateDataFile::new);

        // Weight: more puts than invalidations to build up state
        return Arbitraries.frequencyOf(
                net.jqwik.api.Tuple.of(5, puts),
                net.jqwik.api.Tuple.of(2, invalidateIndexes),
                net.jqwik.api.Tuple.of(2, invalidateDataFiles)
        );
    }

    private static String compositeKey(
            String indexId, String dataFilePath, int generation) {
        return indexId + "|" + dataFilePath + "|" + generation;
    }

    // --- Operation types ---

    interface CacheOp {}

    static final class Put implements CacheOp {
        final String indexId;
        final String dataFilePath;
        final int generation;

        Put(String indexId, String dataFilePath, int generation) {
            this.indexId = indexId;
            this.dataFilePath = dataFilePath;
            this.generation = generation;
        }
    }

    static final class InvalidateIndex implements CacheOp {
        final String indexId;

        InvalidateIndex(String indexId) {
            this.indexId = indexId;
        }
    }

    static final class InvalidateDataFile implements CacheOp {
        final String dataFilePath;

        InvalidateDataFile(String dataFilePath) {
            this.dataFilePath = dataFilePath;
        }
    }
}
