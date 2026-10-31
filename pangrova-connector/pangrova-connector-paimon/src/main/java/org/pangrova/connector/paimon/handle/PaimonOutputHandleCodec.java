package org.pangrova.connector.paimon.handle;

import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Deterministic bounded codec for {@link PaimonOutputTableHandle}. */
public final class PaimonOutputHandleCodec implements ConnectorOutputHandleCodec {

    public static final String TYPE_URL = "pangrova.io/connector/paimon/output-handle";
    public static final int VERSION = 4;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;
    private static final int MAX_ENTRIES = 100_000;
    private static final int MAX_ALLOCATION_BYTES = 32 * 1024 * 1024;

    @Override
    public String typeUrl() { return TYPE_URL; }

    @Override
    public int codecVersion() { return VERSION; }

    @Override
    public byte[] serialize(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof PaimonOutputTableHandle paimon)) {
            throw new IllegalArgumentException("Expected PaimonOutputTableHandle");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeString(out, paimon.schemaName());
            writeString(out, paimon.tableName());
            writeString(out, paimon.tableLocation());
            writeString(out, paimon.tableUuid());
            writeString(out, paimon.schemaFingerprint());
            writeString(out, paimon.rowTypeJson());
            writeList(out, paimon.primaryKeys());
            writeList(out, paimon.partitionKeys());
            writeMap(out, paimon.overwritePartition());
            writeString(out, paimon.mode());
            out.writeLong(paimon.baseSnapshotId());
            var allocation = paimon.nativePhysicalWriteAllocation();
            out.writeBoolean(allocation.isPresent());
            if (allocation.isPresent()) {
                byte[] physical = PaimonPhysicalAllocationCodec.encode(allocation.orElseThrow());
                if (physical.length > MAX_ALLOCATION_BYTES) {
                    throw new IllegalArgumentException("Physical allocation exceeds limit");
                }
                out.writeInt(physical.length);
                out.write(physical);
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory Paimon handle encoding failed", impossible);
        }
    }

    @Override
    public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported Paimon output handle version: " + version);
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            PaimonOutputTableHandle handle = new PaimonOutputTableHandle(
                    readString(in), readString(in), readString(in), readString(in), readString(in),
                    readString(in),
                    new PaimonOutputTableHandle.Layout(readList(in), readList(in)),
                    new PaimonOutputTableHandle.WriteSemantics(
                            readMap(in), readString(in), in.readLong(), Optional.empty()));
            if (in.readBoolean()) {
                int length = in.readInt();
                if (length < 1 || length > MAX_ALLOCATION_BYTES || length > in.available()) {
                    throw new IllegalArgumentException("Invalid persisted physical allocation length");
                }
                handle = handle.withPhysicalAllocation(
                        PaimonPhysicalAllocationCodec.decode(in.readNBytes(length), handle));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in Paimon output handle");
            }
            return handle;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Malformed Paimon output handle", failure);
        }
    }

    @Override
    public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof PaimonOutputTableHandle paimon)) {
            throw new IllegalArgumentException("Expected PaimonOutputTableHandle");
        }
        return List.copyOf(PaimonTableHandleFactory.parseRowType(paimon).getFieldNames());
    }

    private static void writeList(DataOutputStream out, List<String> values) throws IOException {
        requireEntryCount(values.size());
        out.writeInt(values.size());
        for (String value : values) writeString(out, value);
    }

    private static List<String> readList(DataInputStream in) throws IOException {
        int count = readEntryCount(in);
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) result.add(readString(in));
        return List.copyOf(result);
    }

    private static void writeMap(DataOutputStream out, Map<String, String> values) throws IOException {
        requireEntryCount(values.size());
        out.writeInt(values.size());
        for (var entry : new TreeMap<>(values).entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    private static Map<String, String> readMap(DataInputStream in) throws IOException {
        int count = readEntryCount(in);
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            String previous = result.put(readString(in), readString(in));
            if (previous != null) throw new IllegalArgumentException("Duplicate Paimon partition key");
        }
        return Map.copyOf(result);
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) throw new IllegalArgumentException("Paimon field exceeds limit");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > in.available()) {
            throw new IllegalArgumentException("Invalid Paimon output handle field length");
        }
        return new String(in.readNBytes(length), StandardCharsets.UTF_8);
    }

    private static int readEntryCount(DataInputStream in) throws IOException {
        int count = in.readInt();
        requireEntryCount(count);
        return count;
    }

    private static void requireEntryCount(int count) {
        if (count < 0 || count > MAX_ENTRIES) throw new IllegalArgumentException("Invalid Paimon entry count");
    }
}
