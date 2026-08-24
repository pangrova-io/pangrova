package org.pangrova.indexing.engine.vector;

import com.github.benmanes.caffeine.cache.RemovalCause;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Memory-mapped HNSW graph that supports incremental loading.
 *
 * <p>Instead of fully deserializing the graph into heap memory, this class
 * maps the index file into virtual address space and loads data on demand:
 * <ul>
 *   <li>Entry point and first-level (level 0) neighbors are loaded eagerly</li>
 *   <li>Vectors are accessed directly from the mapped buffer on demand</li>
 *   <li>Deeper level adjacency lists are loaded lazily during search traversal</li>
 * </ul>
 *
 * <p>This approach eliminates the O(n) deserialization cost on every search,
 * replacing it with O(1) startup plus on-demand page faults managed by the OS.</p>
 *
 * <p>Thread safety: read operations are safe for concurrent use. The lazy level
 * loading uses {@link ConcurrentHashMap#computeIfAbsent} for safe publication.</p>
 */
final class MappedHnswGraph implements Closeable {

    private static final int HEADER_SIZE = 16;
    private static final int GRAPH_META_SIZE = 20;

    private final MappedByteBuffer mappedBuffer;
    private final FileChannel channel;
    private final Path tempFilePath;
    private final int dimension;
    private final int nodeCount;
    private final int entryPoint;
    private final int maxLevel;
    private final int vectorsOffset;

    // Eagerly loaded nodeId -> physical file rowOffset table (8 bytes/node).
    // Search results report this physical offset, not the dense nodeId, so
    // ANN output aligns with the brute-force scanner when the vector column
    // contains NULLs.
    private final long[] rowOffsets;

    // Eagerly loaded level-0 adjacency (most accessed during search)
    private final List<List<Integer>> level0Adjacency;

    // Lazily loaded deeper levels: level -> (nodeId -> neighbors)
    private final ConcurrentHashMap<Integer, List<List<Integer>>> lazyLevels;

    // Per-level offset table for fast random access into the mapped buffer
    private final int[] levelOffsets;
    private final int levelCount;

    private MappedHnswGraph(MappedByteBuffer mappedBuffer, FileChannel channel,
                            Path tempFilePath,
                            MappedGraphLayout layout, AdjacencySections sections) {
        this.mappedBuffer = mappedBuffer;
        this.channel = channel;
        this.tempFilePath = tempFilePath;
        this.dimension = layout.dimension();
        this.nodeCount = layout.nodeCount();
        this.entryPoint = layout.entryPoint();
        this.maxLevel = layout.maxLevel();
        this.vectorsOffset = layout.vectorsOffset();
        this.rowOffsets = sections.rowOffsets();
        this.level0Adjacency = sections.level0Adjacency();
        this.lazyLevels = new ConcurrentHashMap<>();
        this.levelOffsets = sections.levelOffsets();
        this.levelCount = sections.levelCount();
    }

    /** Scalar header fields of a mapped HNSW graph file. */
    private record MappedGraphLayout(int dimension, int nodeCount, int entryPoint,
                                     int maxLevel, int vectorsOffset) {}

    /** Row-offset table and adjacency section offsets parsed from the file. */
    private record AdjacencySections(long[] rowOffsets, int[] levelOffsets,
                                     int levelCount, List<List<Integer>> level0Adjacency) {}

    /** Level offsets together with the eagerly loaded level-0 adjacency. */
    private record LevelParseResult(int[] levelOffsets, int levelCount,
                                    List<List<Integer>> level0Adjacency) {}

    /** Level-0 adjacency list together with the buffer position after reading it. */
    private record LevelReadResult(List<List<Integer>> adjacency, int nextPos) {}

    /**
     * Opens an HNSW index file with memory-mapped access.
     * Eagerly loads the entry point metadata and level-0 adjacency lists.
     */
    static MappedHnswGraph open(Path indexFile) throws IOException {
        return open(indexFile, null);
    }

    private static MappedHnswGraph open(Path indexFile, Path tempFileToDelete) throws IOException {
        FileChannel channel = FileChannel.open(indexFile, StandardOpenOption.READ);
        long fileSize = channel.size();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read header
        int magic = buffer.getInt(0);
        if (magic != HnswGraphCodec.MAGIC) {
            channel.close();
            throw new IllegalArgumentException(
                    "Invalid HNSW magic: 0x" + Integer.toHexString(magic));
        }
        byte version = buffer.get(4);
        if (version != HnswGraphCodec.VERSION) {
            channel.close();
            throw new IllegalArgumentException("Unsupported HNSW version: " + version);
        }
        // offset: magic(4) + version(1) + algo(1) = 6
        int dimension = buffer.getInt(6);

        // Read graph meta at offset HEADER_SIZE
        int metaOffset = HEADER_SIZE;
        int maxLevel = buffer.getInt(metaOffset);
        int entryPoint = buffer.getInt(metaOffset + 4);
        int nodeCount = buffer.getInt(metaOffset + 8);

        int vectorsOffset = HEADER_SIZE + GRAPH_META_SIZE;
        int vectorBytes = nodeCount * dimension * 4;
        int rowOffsetsOffset = vectorsOffset + vectorBytes;
        int adjacencyOffset = rowOffsetsOffset + nodeCount * 8;

        // Eagerly load the nodeId -> physical rowOffset table (small: 8 bytes/node).
        long[] rowOffsets = readRowOffsets(buffer, rowOffsetsOffset, nodeCount);

        // Parse level offsets and eagerly load level 0
        LevelParseResult levels = parseLevels(buffer, adjacencyOffset);

        MappedGraphLayout layout = new MappedGraphLayout(
                dimension, nodeCount, entryPoint, maxLevel, vectorsOffset);
        AdjacencySections sections = new AdjacencySections(
                rowOffsets, levels.levelOffsets(), levels.levelCount(), levels.level0Adjacency());
        return new MappedHnswGraph(buffer, channel, tempFileToDelete, layout, sections);
    }

    private static long[] readRowOffsets(ByteBuffer buffer, int rowOffsetsOffset, int nodeCount) {
        long[] rowOffsets = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            rowOffsets[i] = buffer.getLong(rowOffsetsOffset + i * 8);
        }
        return rowOffsets;
    }

    /**
     * Walks the per-level adjacency section table, recording section offsets
     * and eagerly reading level 0 (the search hot path); deeper levels are
     * skipped and loaded on demand during traversal.
     */
    private static LevelParseResult parseLevels(ByteBuffer buffer, int adjacencyOffset) {
        buffer.position(adjacencyOffset);
        int levelCount = buffer.getInt();

        int[] levelOffsets = new int[levelCount];
        List<List<Integer>> level0Adjacency = Collections.emptyList();

        int currentPos = adjacencyOffset + 4; // after levelCount int
        for (int level = 0; level < levelCount; level++) {
            levelOffsets[level] = currentPos;
            int nodesAtLevel = buffer.getInt(currentPos);
            currentPos += 4;

            if (level == 0) {
                // Eagerly load level 0 — this is the hot path for search
                LevelReadResult read = readLevel0(buffer, currentPos, nodesAtLevel);
                level0Adjacency = read.adjacency();
                currentPos = read.nextPos();
            } else {
                currentPos = skipLevelBytes(currentPos, buffer, nodesAtLevel);
            }
        }
        return new LevelParseResult(levelOffsets, levelCount, level0Adjacency);
    }

    private static LevelReadResult readLevel0(ByteBuffer buffer, int pos, int nodesAtLevel) {
        List<List<Integer>> adjacency = new ArrayList<>(nodesAtLevel);
        for (int nodeId = 0; nodeId < nodesAtLevel; nodeId++) {
            int neighborCount = Short.toUnsignedInt(buffer.getShort(pos));
            pos += 2;
            List<Integer> neighbors = new ArrayList<>(neighborCount);
            for (int n = 0; n < neighborCount; n++) {
                neighbors.add(buffer.getInt(pos));
                pos += 4;
            }
            adjacency.add(neighbors);
        }
        return new LevelReadResult(adjacency, pos);
    }

    private static int skipLevelBytes(int pos, ByteBuffer buffer, int nodesAtLevel) {
        for (int nodeId = 0; nodeId < nodesAtLevel; nodeId++) {
            int neighborCount = Short.toUnsignedInt(buffer.getShort(pos));
            pos += 2 + neighborCount * 4;
        }
        return pos;
    }

    /**
     * Process-wide cache of mapped HNSW graphs keyed by payload identity.
     *
     * <p>Lifetime is bound to {@code IndexCache}: the cache uses {@code weakKeys()}
     * (identity-based) so when {@code IndexCache} evicts a {@link org.pangrova.spi.index.data.vector.VectorIndexData}
     * (the only strong holder of the payload {@code byte[]}), the GC reclaims
     * the payload, this cache entry is evicted, and the removal listener closes
     * the underlying file channel and unmaps the file.</p>
     *
     * <p>This eliminates the per-search temp-file write + mmap pattern that
     * leaked one descriptor per HNSW search.</p>
     */
    private static final com.github.benmanes.caffeine.cache.Cache<byte[], MappedHnswGraph> SHARED_CACHE =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .weakKeys()
                    .removalListener((byte[] key, MappedHnswGraph value, RemovalCause cause) -> {
                        if (value != null) {
                            try {
                                value.close();
                            } catch (IOException ignored) {
                                // best-effort close on eviction
                            }
                        }
                    })
                    .build();

    /**
     * Returns a cached MappedHnswGraph for the given payload, creating one on
     * first access. The mapped graph is reused for the lifetime of the payload
     * {@code byte[]} — IndexCache eviction of the VectorIndexData transitively
     * evicts the cached mapped graph via {@code weakKeys()}.
     *
     * @param payload index payload (must not be null or empty)
     * @return cached or newly-mapped graph
     * @throws IOException if mapping fails on first creation
     */
    static MappedHnswGraph fromPayload(byte[] payload) throws IOException {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException(
                    "Cannot create MappedHnswGraph from empty payload");
        }
        MappedHnswGraph cached = SHARED_CACHE.getIfPresent(payload);
        if (cached != null && cached.isOpen()) {
            return cached;
        }
        // Race-safe: create outside the cache, then publish via put().
        // If two threads race, both create temp files but only one MappedHnswGraph
        // wins the put; the loser is closed by the cache's removal listener when
        // the loser's payload is GC'd (or here, since we never strongly reference it).
        MappedHnswGraph fresh = openFresh(payload);
        synchronized (SHARED_CACHE) {
            MappedHnswGraph existing = SHARED_CACHE.getIfPresent(payload);
            if (existing != null && existing.isOpen()) {
                try {
                    fresh.close();
                } catch (IOException ignored) {
                    // best-effort close of loser
                }
                return existing;
            }
            SHARED_CACHE.put(payload, fresh);
            return fresh;
        }
    }

    private static MappedHnswGraph openFresh(byte[] payload) throws IOException {
        Path tempFile = Files.createTempFile("hnsw-mmap-", ".idx");
        tempFile.toFile().deleteOnExit();
        Files.write(tempFile, payload);
        return open(tempFile, tempFile);
    }

    int nodeCount() {
        return nodeCount;
    }

    int dimension() {
        return dimension;
    }

    int entryPoint() {
        return entryPoint;
    }

    int maxLevel() {
        return Math.max(maxLevel, 0);
    }

    /**
     * Returns the physical 0-based file row offset for the given node.
     * This is the value ANN search results must report (not the dense nodeId),
     * so results align with the brute-force scanner and row fetching.
     */
    long rowOffset(int nodeId) {
        if (nodeId < 0 || nodeId >= nodeCount) {
            throw new IndexOutOfBoundsException(
                    "nodeId=" + nodeId + " out of range [0, " + nodeCount + ")");
        }
        return rowOffsets[nodeId];
    }

    /**
     * Reads a vector directly from the memory-mapped buffer.
     * The OS page cache handles caching — no heap allocation for the buffer.
     */
    float[] vector(int nodeId) {
        if (nodeId < 0 || nodeId >= nodeCount) {
            throw new IndexOutOfBoundsException(
                    "nodeId=" + nodeId + " out of range [0, " + nodeCount + ")");
        }
        int offset = vectorsOffset + nodeId * dimension * 4;
        float[] vec = new float[dimension];
        ByteBuffer slice = mappedBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(offset);
        for (int d = 0; d < dimension; d++) {
            vec[d] = slice.getFloat();
        }
        return vec;
    }

    /**
     * Returns neighbors for a node at the given level.
     * Level 0 is served from eagerly-loaded heap data; deeper levels are
     * loaded on demand from the memory-mapped buffer.
     */
    List<Integer> neighbors(int nodeId, int level) {
        if (level == 0) {
            if (nodeId >= level0Adjacency.size()) {
                return Collections.emptyList();
            }
            return level0Adjacency.get(nodeId);
        }
        List<List<Integer>> levelData = lazyLevels.computeIfAbsent(
                level, this::loadLevel);
        if (nodeId >= levelData.size()) {
            return Collections.emptyList();
        }
        return levelData.get(nodeId);
    }

    /**
     * Loads a single level's adjacency lists from the mapped buffer on demand.
     */
    private List<List<Integer>> loadLevel(int level) {
        if (level >= levelCount) {
            return Collections.emptyList();
        }
        int pos = levelOffsets[level];
        ByteBuffer slice = mappedBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(pos);

        int nodesAtLevel = slice.getInt();
        List<List<Integer>> levelList = new ArrayList<>(nodesAtLevel);
        for (int nodeId = 0; nodeId < nodesAtLevel; nodeId++) {
            int neighborCount = Short.toUnsignedInt(slice.getShort());
            List<Integer> neighbors = new ArrayList<>(neighborCount);
            for (int n = 0; n < neighborCount; n++) {
                neighbors.add(slice.getInt());
            }
            levelList.add(neighbors);
        }
        return levelList;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        if (tempFilePath != null) {
            Files.deleteIfExists(tempFilePath);
        }
    }

    boolean isOpen() {
        return channel.isOpen();
    }
}
