package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Binary codec for HNSW graph serialization and deserialization.
 *
 * <p>Package-private: used by {@link HnswEngine} for build/search round-trip
 * and by {@link org.pangrova.indexing.serde.vector.HnswSerializer} for storage.</p>
 *
 * <p>Format:
 * <pre>
 * [Header: magic(4) | version(1) | algorithmType(1) | dimension(4) | metric(1) | reserved(5)]
 * [GraphMeta: maxLevel(4) | entryPointId(4) | nodeCount(4) | M(4) | efConstruction(4)]
 * [Vectors: dimension * 4 bytes per node]
 * [RowOffsets: nodeCount * 8 bytes] (physical 0-based file row per node)
 * [Adjacency: for each level → for each node → neighborCount(2) | neighborIds(4 each)]
 * [CRC32(4)]
 * </pre>
 */
final class HnswGraphCodec {

    static final int MAGIC = 0x484E5357; // "HNSW"
    static final byte VERSION = 2;
    static final byte ALGORITHM_HNSW = 0;
    private static final int HEADER_SIZE = 16; // magic(4)+version(1)+algo(1)+dim(4)+metric(1)+reserved(5)
    private static final int GRAPH_META_SIZE = 20; // maxLevel(4)+entry(4)+nodeCount(4)+M(4)+efC(4)
    /** Largest neighbor count encodable in the unsigned-short wire field. */
    private static final int MAX_NEIGHBORS_PER_NODE = 65535;
    /** Mask to fold a 32-bit CRC value into a signed int. */
    private static final long CRC_INT_MASK = 0xFFFFFFFFL;

    private HnswGraphCodec() {}
    static byte[] serialize(HnswGraph graph, int dimension, DistanceMetric metric,
                            int m, int efConstruction) {
        int nodeCount = graph.nodeCount();
        int vectorBytes = nodeCount * dimension * 4;
        int rowOffsetBytes = nodeCount * 8;
        int adjacencyBytes = estimateAdjacencySize(graph);
        int totalSize = HEADER_SIZE + GRAPH_META_SIZE + vectorBytes + rowOffsetBytes
                + adjacencyBytes + 4; // +4 for CRC

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        writeHeader(buf, dimension, metric);
        writeGraphMeta(buf, graph, m, efConstruction);
        writeVectors(buf, graph, dimension);
        writeRowOffsets(buf, graph);
        writeAdjacency(buf, graph);
        writeCrc(buf);
        return buf.array();
    }

    static HnswGraph deserialize(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return new HnswGraph(List.of(), new long[0], 0, List.of(), -1, -1);
        }
        int minSize = HEADER_SIZE + GRAPH_META_SIZE + 4; // header + meta + CRC
        if (payload.length < minSize) {
            throw new IllegalArgumentException(
                    "HNSW payload too small: " + payload.length + " bytes, minimum " + minSize);
        }
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        verifyCrc(buf);
        buf.rewind();

        int[] headerOut = readHeader(buf);
        int dimension = headerOut[0];
        int[] metaOut = readGraphMeta(buf);
        int maxLevel = metaOut[0];
        int entryPoint = metaOut[1];
        int nodeCount = metaOut[2];

        List<float[]> vectors = readVectors(buf, nodeCount, dimension);
        long[] rowOffsets = readRowOffsets(buf, nodeCount);
        List<List<List<Integer>>> adjacency = readAdjacency(buf, maxLevel, nodeCount);
        return new HnswGraph(vectors, rowOffsets, dimension, adjacency, entryPoint, maxLevel);
    }

    // --- Write helpers ---

    private static void writeHeader(ByteBuffer buf, int dimension, DistanceMetric metric) {
        buf.putInt(MAGIC);
        buf.put(VERSION);
        buf.put(ALGORITHM_HNSW);
        buf.putInt(dimension);
        buf.put((byte) metric.ordinal());
        buf.put(new byte[5]); // reserved
    }

    private static void writeGraphMeta(ByteBuffer buf, HnswGraph graph, int m, int efConstruction) {
        buf.putInt(graph.maxLevel());
        buf.putInt(graph.entryPoint());
        buf.putInt(graph.nodeCount());
        buf.putInt(m);
        buf.putInt(efConstruction);
    }

    private static void writeVectors(ByteBuffer buf, HnswGraph graph, int dimension) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            float[] vec = graph.vector(i);
            for (int d = 0; d < dimension; d++) {
                buf.putFloat(vec[d]);
            }
        }
    }

    private static void writeRowOffsets(ByteBuffer buf, HnswGraph graph) {
        long[] rowOffsets = graph.rowOffsets();
        int nodeCount = graph.nodeCount();
        for (int i = 0; i < nodeCount; i++) {
            buf.putLong(rowOffsets[i]);
        }
    }

    private static void writeAdjacency(ByteBuffer buf, HnswGraph graph) {
        int levelCount = graph.levelCount();
        buf.putInt(levelCount);
        for (int level = 0; level < levelCount; level++) {
            int nodeCount = graph.nodeCount();
            buf.putInt(nodeCount);
            for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
                List<Integer> neighbors = graph.neighbors(nodeId, level);
                if (neighbors.size() > MAX_NEIGHBORS_PER_NODE) {
                    throw new IllegalStateException(
                            "neighbor count " + neighbors.size() + " exceeds unsigned short max for node " + nodeId);
                }
                buf.putShort((short) neighbors.size());
                for (int neighbor : neighbors) {
                    buf.putInt(neighbor);
                }
            }
        }
    }

    private static void writeCrc(ByteBuffer buf) {
        int dataLen = buf.position();
        CRC32 crc = new CRC32();
        crc.update(buf.array(), buf.arrayOffset(), dataLen);
        buf.putInt((int) (crc.getValue() & CRC_INT_MASK));
    }

    // --- Read helpers ---

    private static int[] readHeader(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid HNSW magic: 0x" + Integer.toHexString(magic));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported HNSW version: " + version);
        }
        buf.get(); // algorithmType (already known)
        int dimension = buf.getInt();
        buf.get(); // metric ordinal (caller knows from VectorIndexData)
        buf.position(buf.position() + 5); // skip reserved
        return new int[]{dimension};
    }

    private static int[] readGraphMeta(ByteBuffer buf) {
        int maxLevel = buf.getInt();
        int entryPoint = buf.getInt();
        int nodeCount = buf.getInt();
        buf.getInt(); // M (not needed for deserialization)
        buf.getInt(); // efConstruction (not needed for deserialization)
        return new int[]{maxLevel, entryPoint, nodeCount};
    }

    private static List<float[]> readVectors(ByteBuffer buf, int nodeCount, int dimension) {
        List<float[]> vectors = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            float[] vec = new float[dimension];
            for (int d = 0; d < dimension; d++) {
                vec[d] = buf.getFloat();
            }
            vectors.add(vec);
        }
        return vectors;
    }

    private static long[] readRowOffsets(ByteBuffer buf, int nodeCount) {
        long[] rowOffsets = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            rowOffsets[i] = buf.getLong();
        }
        return rowOffsets;
    }

    private static List<List<List<Integer>>> readAdjacency(ByteBuffer buf, int maxLevel, int nodeCount) {
        int levelCount = buf.getInt();
        List<List<List<Integer>>> adjacency = new ArrayList<>(levelCount);
        for (int level = 0; level < levelCount; level++) {
            int nodesAtLevel = buf.getInt();
            List<List<Integer>> levelList = new ArrayList<>(nodesAtLevel);
            for (int nodeId = 0; nodeId < nodesAtLevel; nodeId++) {
                int neighborCount = Short.toUnsignedInt(buf.getShort());
                List<Integer> neighbors = new ArrayList<>(neighborCount);
                for (int n = 0; n < neighborCount; n++) {
                    neighbors.add(buf.getInt());
                }
                levelList.add(neighbors);
            }
            adjacency.add(levelList);
        }
        return adjacency;
    }

    private static void verifyCrc(ByteBuffer buf) {
        int dataLen = buf.limit() - 4;
        CRC32 crc = new CRC32();
        crc.update(buf.array(), buf.arrayOffset(), dataLen);
        buf.position(dataLen);
        int stored = buf.getInt();
        int computed = (int) (crc.getValue() & CRC_INT_MASK);
        if (stored != computed) {
            throw new IllegalArgumentException("CRC32 mismatch: stored=0x"
                    + Integer.toHexString(stored) + " computed=0x" + Integer.toHexString(computed));
        }
    }

    private static int estimateAdjacencySize(HnswGraph graph) {
        int size = 4; // levelCount
        for (int level = 0; level < graph.levelCount(); level++) {
            size += 4; // nodesAtLevel
            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                size += 2; // neighborCount (short)
                size += graph.neighbors(nodeId, level).size() * 4; // neighborIds
            }
        }
        return size;
    }
}
