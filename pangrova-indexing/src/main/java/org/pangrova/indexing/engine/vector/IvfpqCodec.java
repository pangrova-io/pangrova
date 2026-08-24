package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.DistanceMetric;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Binary codec for IVFPQ index serialization and deserialization.
 *
 * <p>Package-private: used by {@link IvfpqEngine} for build/search round-trip
 * and by {@link org.pangrova.indexing.serde.vector.IvfpqSerializer} for storage.</p>
 *
 * <p>Format:
 * <pre>
 * [Header: magic(4) | version(1) | algorithmType(1) | dimension(4) | metric(1) | reserved(5)]
 * [IvfpqMeta: nlist(4) | pqM(4) | nbits(4) | vectorCount(4)]
 * [Centroids: nlist * dimension * 4 bytes]
 * [Codebook: pqM * ksub * subDim * 4 bytes]
 * [InvertedLists: for each cluster → count(4) | (rowOffset(8) + codes(pqM * codeBytes)) per entry]
 * [CRC32(4)]
 * </pre>
 */
final class IvfpqCodec {

    static final int MAGIC = 0x49565051; // "IVPQ"
    static final byte VERSION = 1;
    static final byte ALGORITHM_IVFPQ = 1;
    /** Mask to fold a 32-bit CRC value into a signed int. */
    private static final long CRC_INT_MASK = 0xFFFFFFFFL;

    private IvfpqCodec() {}

    static byte[] serialize(IvfpqIndex index, int dimension, DistanceMetric metric,
                            int nlist, int pqM, int nbits) {
        if (pqM <= 0 || dimension % pqM != 0) {
            throw new IllegalArgumentException(
                    "dimension (" + dimension + ") must be divisible by pqM (" + pqM + ")");
        }
        int ksub = 1 << nbits;
        int subDim = dimension / pqM;
        int codeBytes = codeSize(pqM, nbits);
        int size = estimateSize(nlist, dimension, pqM, ksub, subDim, codeBytes, index);

        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        writeHeader(buf, dimension, metric);
        writeMeta(buf, nlist, pqM, nbits, index.vectorCount());
        writeCentroids(buf, index.centroids(), nlist, dimension);
        writeCodebook(buf, index.codebook(), pqM, ksub, subDim);
        writeInvertedLists(buf, index, nlist, pqM, nbits);
        writeCrc(buf);

        byte[] result = new byte[buf.position()];
        buf.rewind();
        buf.get(result);
        return result;
    }

    private static final int HEADER_SIZE = 16; // magic(4)+version(1)+algo(1)+dim(4)+metric(1)+reserved(5)
    private static final int META_SIZE = 16; // nlist(4)+pqM(4)+nbits(4)+vectorCount(4)

    static IvfpqIndex deserialize(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return new IvfpqIndex(new float[0][0], new float[0][0][0], List.of(), 0, 1, 8, 0);
        }
        int minSize = HEADER_SIZE + META_SIZE + 4; // header + meta + CRC
        if (payload.length < minSize) {
            throw new IllegalArgumentException(
                    "IVFPQ payload too small: " + payload.length + " bytes, minimum " + minSize);
        }
        ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
        verifyCrc(buf);
        buf.rewind();

        int dimension = readHeader(buf);
        int[] meta = readMeta(buf);
        int nlist = meta[0];
        int pqM = meta[1];
        int nbits = meta[2];
        int vectorCount = meta[3];
        if (pqM <= 0 || dimension % pqM != 0) {
            throw new IllegalArgumentException(
                    "dimension (" + dimension + ") must be divisible by pqM (" + pqM + ")");
        }
        int ksub = 1 << nbits;
        int subDim = dimension / pqM;
        int codeBytes = codeSize(pqM, nbits);

        float[][] centroids = readCentroids(buf, nlist, dimension);
        float[][][] codebook = readCodebook(buf, pqM, ksub, subDim);
        List<List<IvfpqIndex.EncodedVector>> lists = readInvertedLists(buf, nlist, pqM, nbits);
        return new IvfpqIndex(centroids, codebook, lists, dimension, pqM, nbits, vectorCount);
    }

    // --- Write helpers ---

    private static void writeHeader(ByteBuffer buf, int dimension, DistanceMetric metric) {
        buf.putInt(MAGIC);
        buf.put(VERSION);
        buf.put(ALGORITHM_IVFPQ);
        buf.putInt(dimension);
        buf.put((byte) metric.ordinal());
        buf.put(new byte[5]); // reserved
    }

    private static void writeMeta(ByteBuffer buf, int nlist, int pqM, int nbits, int vectorCount) {
        buf.putInt(nlist);
        buf.putInt(pqM);
        buf.putInt(nbits);
        buf.putInt(vectorCount);
    }

    private static void writeCentroids(ByteBuffer buf, float[][] centroids, int nlist, int dimension) {
        for (int c = 0; c < nlist; c++) {
            for (int d = 0; d < dimension; d++) {
                buf.putFloat(centroids[c][d]);
            }
        }
    }

    private static void writeCodebook(ByteBuffer buf, float[][][] codebook, int pqM, int ksub, int subDim) {
        for (int s = 0; s < pqM; s++) {
            for (int k = 0; k < ksub; k++) {
                for (int d = 0; d < subDim; d++) {
                    buf.putFloat(codebook[s][k][d]);
                }
            }
        }
    }

    private static void writeInvertedLists(ByteBuffer buf, IvfpqIndex index, int nlist,
                                           int pqM, int nbits) {
        for (int c = 0; c < nlist; c++) {
            List<IvfpqIndex.EncodedVector> list = index.invertedList(c);
            buf.putInt(list.size());
            for (IvfpqIndex.EncodedVector ev : list) {
                buf.putLong(ev.rowOffset());
                if (nbits <= 8) {
                    for (int s = 0; s < pqM; s++) {
                        buf.put((byte) ev.codes()[s]);
                    }
                } else {
                    for (int s = 0; s < pqM; s++) {
                        buf.putInt(ev.codes()[s]);
                    }
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

    private static int readHeader(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid IVFPQ magic: 0x" + Integer.toHexString(magic));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported IVFPQ version: " + version);
        }
        buf.get(); // algorithmType
        int dimension = buf.getInt();
        buf.get(); // metric ordinal
        buf.position(buf.position() + 5); // reserved
        return dimension;
    }

    private static int[] readMeta(ByteBuffer buf) {
        return new int[]{buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt()};
    }

    private static float[][] readCentroids(ByteBuffer buf, int nlist, int dimension) {
        float[][] centroids = new float[nlist][dimension];
        for (int c = 0; c < nlist; c++) {
            for (int d = 0; d < dimension; d++) {
                centroids[c][d] = buf.getFloat();
            }
        }
        return centroids;
    }

    private static float[][][] readCodebook(ByteBuffer buf, int pqM, int ksub, int subDim) {
        float[][][] codebook = new float[pqM][ksub][subDim];
        for (int s = 0; s < pqM; s++) {
            for (int k = 0; k < ksub; k++) {
                for (int d = 0; d < subDim; d++) {
                    codebook[s][k][d] = buf.getFloat();
                }
            }
        }
        return codebook;
    }

    private static List<List<IvfpqIndex.EncodedVector>> readInvertedLists(ByteBuffer buf, int nlist,
                                                                          int pqM, int nbits) {
        List<List<IvfpqIndex.EncodedVector>> lists = new ArrayList<>(nlist);
        for (int c = 0; c < nlist; c++) {
            int count = buf.getInt();
            List<IvfpqIndex.EncodedVector> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long rowOffset = buf.getLong();
                int[] codes = new int[pqM];
                if (nbits <= 8) {
                    for (int s = 0; s < pqM; s++) {
                        codes[s] = Byte.toUnsignedInt(buf.get());
                    }
                } else {
                    for (int s = 0; s < pqM; s++) {
                        codes[s] = buf.getInt();
                    }
                }
                list.add(new IvfpqIndex.EncodedVector(rowOffset, codes));
            }
            lists.add(list);
        }
        return lists;
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

    private static int codeSize(int pqM, int nbits) {
        return nbits <= 8 ? pqM : pqM * 4;
    }

    private static int estimateSize(int nlist, int dimension, int pqM, int ksub, int subDim,
                                    int codeBytes, IvfpqIndex index) {
        int headerMeta = 16 + 16; // header + meta
        int centroidBytes = nlist * dimension * 4;
        int codebookBytes = pqM * ksub * subDim * 4;
        int listBytes = nlist * 4; // count per cluster
        for (int c = 0; c < nlist; c++) {
            listBytes += index.invertedList(c).size() * (8 + codeBytes); // rowOffset + codes
        }
        return headerMeta + centroidBytes + codebookBytes + listBytes + 4; // +CRC
    }
}
