package org.pangrova.indexing.engine.bitmap.serializer;

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Bitmap codec backed by the official RoaringBitmap library.
 *
 * <p>Wire format:
 * <pre>
 *   [magic:4B = 'A','R','B','M'] [version:4B BE int] [roaringPayload:NB]
 * </pre>
 * The 8-byte header lets readers detect bare RoaringBitmap payloads written
 * by older builds and reject incompatible versions explicitly instead of
 * crashing inside the RoaringBitmap deserializer.</p>
 */
public final class RoaringBitmapCodec {

    /** "ARBM" magic — Pangrova RoaringBitMap. */
    public static final int MAGIC = 0x4152424D;
    /** Current format version; bump on any wire-incompatible change. */
    public static final int VERSION = 1;
    /** Header size in bytes (magic + version). */
    public static final int HEADER_SIZE = 8;

    private RoaringBitmapCodec() {}

    /**
     * Serializes a set of row positions to a RoaringBitmap byte array.
     */
    public static byte[] serialize(BitSet positions) {
        RoaringBitmap bitmap = fromBitSet(positions);
        return toBytes(bitmap);
    }

    /**
     * Serializes a RoaringBitmap instance to a byte array.
     */
    public static byte[] serialize(RoaringBitmap bitmap) {
        bitmap.runOptimize();
        return toBytes(bitmap);
    }

    /**
     * Deserializes a byte array back to a BitSet of row positions.
     */
    public static BitSet deserialize(byte[] data) {
        RoaringBitmap bitmap = fromBytes(data);
        BitSet result = new BitSet();
        bitmap.forEach((int pos) -> result.set(pos));
        return result;
    }

    /**
     * Deserializes a byte array to a RoaringBitmap instance.
     */
    public static RoaringBitmap deserializeToRoaring(byte[] data) {
        return fromBytes(data);
    }

    /**
     * Checks if a specific position is set in the serialized bitmap.
     */
    public static boolean contains(byte[] data, int position) {
        RoaringBitmap bitmap = fromBytes(data);
        return bitmap.contains(position);
    }

    private static RoaringBitmap fromBitSet(BitSet positions) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int pos = positions.nextSetBit(0); pos >= 0; pos = positions.nextSetBit(pos + 1)) {
            bitmap.add(pos);
        }
        bitmap.runOptimize();
        return bitmap;
    }

    private static byte[] toBytes(RoaringBitmap bitmap) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    HEADER_SIZE + bitmap.serializedSizeInBytes());
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(MAGIC);
            dos.writeInt(VERSION);
            bitmap.serialize(dos);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize RoaringBitmap", e);
        }
    }

    private static RoaringBitmap fromBytes(byte[] data) {
        if (data == null || data.length < HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "Bitmap payload too short: expected at least " + HEADER_SIZE
                            + " bytes, got " + (data == null ? 0 : data.length));
        }
        ByteBuffer header = ByteBuffer.wrap(data, 0, HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        int magic = header.getInt();
        int version = header.getInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                    "Invalid bitmap magic: expected 0x" + Integer.toHexString(MAGIC)
                            + " got 0x" + Integer.toHexString(magic));
        }
        if (version != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported bitmap version: " + version + " (expected " + VERSION + ")");
        }
        try {
            RoaringBitmap bitmap = new RoaringBitmap();
            DataInputStream dis = new DataInputStream(
                    new ByteArrayInputStream(data, HEADER_SIZE, data.length - HEADER_SIZE));
            bitmap.deserialize(dis);
            return bitmap;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize RoaringBitmap", e);
        }
    }
}
