package org.pangrova.connector.api.metadata.table;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.types.TypeSignature;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Stable, framed representation of a connector's typed column declaration. */
@Experimental
public final class ColumnSchemaEncoding {
    private ColumnSchemaEncoding() {}

    /**
     * Encodes a column using big-endian lengths, exact UTF-8 names, explicit
     * optional values, and recursive type-signature parameters. No display
     * formatting or implementation class names participate in this contract.
     */
    public static byte[] encode(ConnectorTableMetadata.Column column) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(buffer)) {
            writeText(output, column.name());
            writeText(output, column.type().name());
            output.writeBoolean(column.fieldId().isPresent());
            if (column.fieldId().isPresent()) {
                output.writeInt(column.fieldId().getAsInt());
            }
            output.writeBoolean(column.parameterizedType() != null);
            if (column.parameterizedType() != null) {
                writeType(output, column.parameterizedType().getTypeSignature());
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("Column declaration cannot be encoded", failure);
        }
        return buffer.toByteArray();
    }

    private static void writeType(DataOutputStream output, TypeSignature signature) throws IOException {
        writeText(output, signature.base());
        output.writeInt(signature.parameters().size());
        for (TypeSignature.TypeParameter parameter : signature.parameters()) {
            writeParameter(output, parameter);
        }
    }

    private static void writeParameter(DataOutputStream output, TypeSignature.TypeParameter parameter)
            throws IOException {
        boolean numeric = parameter.longValue() != null;
        if (numeric == (parameter.typeSignature() != null)) {
            throw new IllegalArgumentException("Type parameter must have exactly one value");
        }
        output.writeBoolean(parameter.fieldName() != null);
        if (parameter.fieldName() != null) {
            writeText(output, parameter.fieldName());
        }
        output.writeBoolean(numeric);
        if (numeric) {
            output.writeLong(parameter.longValue());
        } else {
            writeType(output, parameter.typeSignature());
        }
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        ByteBuffer utf8 = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(value));
        byte[] bytes = new byte[utf8.remaining()];
        utf8.get(bytes);
        output.writeInt(bytes.length);
        output.write(bytes);
    }
}
