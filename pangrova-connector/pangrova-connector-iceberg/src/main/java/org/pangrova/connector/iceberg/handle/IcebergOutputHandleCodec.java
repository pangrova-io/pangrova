package org.pangrova.connector.iceberg.handle;

import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/** Deterministic bounded codec for {@link IcebergOutputTableHandle}. */
public final class IcebergOutputHandleCodec implements ConnectorOutputHandleCodec {

    public static final String TYPE_URL = "pangrova.io/connector/iceberg/output-handle";
    public static final int VERSION = 5;
    private static final int MAX_STRING_BYTES = 16 * 1024 * 1024;
    private static final int MAX_PARTITIONS = 4096;

    @Override
    public String typeUrl() {
        return TYPE_URL;
    }

    @Override
    public int codecVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof IcebergOutputTableHandle iceberg)) {
            throw new IllegalArgumentException("Expected IcebergOutputTableHandle");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            writeString(out, iceberg.schemaName());
            writeString(out, iceberg.tableName());
            writeString(out, iceberg.tableUuid());
            writeString(out, iceberg.tableLocation());
            writeString(out, iceberg.schemaJson());
            writeString(out, iceberg.partitionSpecJson());
            out.writeInt(iceberg.formatVersion());
            out.writeLong(iceberg.baseSnapshotId());
            writeString(out, iceberg.writeMode());
            writeString(out, iceberg.overwriteScope().name());
            writePartitions(out, iceberg.staticPartitions());
            writePartitions(out, iceberg.declarationProperties());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory Iceberg handle encoding failed", impossible);
        }
    }

    @Override
    public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
        requireVersion(version);
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            IcebergOutputTableHandle handle = new IcebergOutputTableHandle(
                    readString(in), readString(in), readString(in), readString(in),
                    readString(in), readString(in), in.readInt(), in.readLong(), readString(in),
                    readOverwriteScope(in), readPartitions(in), readPartitions(in));
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in Iceberg output handle");
            }
            return handle;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Malformed Iceberg output handle", failure);
        }
    }

    @Override
    public java.util.List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof IcebergOutputTableHandle iceberg)) {
            throw new IllegalArgumentException("Expected IcebergOutputTableHandle");
        }
        return org.apache.iceberg.SchemaParser.fromJson(iceberg.schemaJson()).columns().stream()
                .map(org.apache.iceberg.types.Types.NestedField::name)
                .toList();
    }

    private static void requireVersion(int version) {
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported Iceberg output handle version: " + version);
        }
    }

    private static IcebergOverwriteScope readOverwriteScope(DataInputStream in) throws IOException {
        String name = readString(in);
        try {
            return IcebergOverwriteScope.valueOf(name);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException(
                    "Unknown Iceberg overwrite scope: " + name, invalid);
        }
    }

    private static void writePartitions(DataOutputStream out, Map<String, String> partitions) throws IOException {
        if (partitions.size() > MAX_PARTITIONS) {
            throw new IllegalArgumentException("Too many Iceberg static partitions");
        }
        out.writeInt(partitions.size());
        for (var entry : new TreeMap<>(partitions).entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    private static Map<String, String> readPartitions(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_PARTITIONS) {
            throw new IllegalArgumentException("Invalid Iceberg static partition count");
        }
        Map<String, String> partitions = new TreeMap<>();
        String previous = null;
        for (int index = 0; index < count; index++) {
            String key = readString(in);
            if (previous != null && previous.compareTo(key) >= 0) {
                throw new IllegalArgumentException("Non-canonical Iceberg static partitions");
            }
            partitions.put(key, readString(in));
            previous = key;
        }
        return partitions;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException("Iceberg output handle field exceeds limit");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > in.available()) {
            throw new IllegalArgumentException("Invalid Iceberg output handle field length");
        }
        return new String(in.readNBytes(length), StandardCharsets.UTF_8);
    }
}
