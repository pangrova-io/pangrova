package org.pangrova.connector.paimon.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.pangrova.spi.connector.metadata.NativePhysicalWriteAllocation;

/** Obtains complete physical schema metadata from the same SDK conversion used by its file writer. */
public final class PaimonPhysicalSchemas {
    private PaimonPhysicalSchemas() {}

    /**
     * Obtains the physical append schema from the exact SDK table declaration.
     *
     * @param schema the selected SDK table schema
     * @return complete physical and declared column metadata
     */
    public static NativePhysicalWriteAllocation.Schema forAppend(TableSchema schema) {
        return fromRowType(schema.id(), schema.logicalRowType(), schema.options());
    }

    /**
     * Converts a persisted SDK row declaration without changing physical units.
     *
     * @param schemaId the selected SDK schema identity
     * @param rowType the complete SDK row declaration
     * @param options the selected SDK table options
     * @return the physical schema and exact column declarations
     */
    public static NativePhysicalWriteAllocation.Schema fromRowType(
            long schemaId, RowType rowType, Map<String, String> options) {
        var fields = rowType.getFields().stream()
                .map(field -> ArrowUtils.toArrowField(field.name(), field.id(), field.type(), 0)).toList();
        byte[] metadata = serialize(new org.apache.arrow.vector.types.pojo.Schema(fields));
        var columns = rowType.getFields().stream().map(field -> new NativePhysicalWriteAllocation.Column(
                field.id(), field.name(), PaimonDeclaredMetadata.declared(
                        field.type(), field.name(), options))).toList();
        return new NativePhysicalWriteAllocation.Schema(schemaId, metadata, sha256(metadata), columns);
    }

    private static byte[] serialize(org.apache.arrow.vector.types.pojo.Schema schema) {
        try (var bytes = new ByteArrayOutputStream();
             var channel = new WriteChannel(Channels.newChannel(bytes))) {
            MessageSerializer.serialize(channel, schema);
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new IllegalStateException("Paimon SDK physical schema serialization failed", failure);
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
