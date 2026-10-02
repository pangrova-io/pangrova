package org.pangrova.spi.coordination;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Exact connector schema and declaration content admitted before allocation, including declaration absence. */
@Experimental
public record WriteDeclarationSnapshot(QualifiedTableName table, String schemaSha256,
                                       String generatedColumnsSha256, boolean hasGeneratedColumns) {
    /** Requires exact table identity and canonical content digests. */
    public WriteDeclarationSnapshot {
        Objects.requireNonNull(table, "table");
        requireDigest(schemaSha256);
        requireDigest(generatedColumnsSha256);
    }

    /** Captures actual SDK schema and validated canonical generated-definition bytes without a metadata mirror. */
    public static WriteDeclarationSnapshot capture(QualifiedTableName table, ConnectorTableMetadata metadata,
                                                   byte[] generatedDefinitions, boolean hasGeneratedColumns) {
        if (!metadata.table().equals(new org.pangrova.spi.connector.metadata.table.SchemaTableName(
                table.schema(), table.table()))
                || !metadata.typedColumns().stream().map(ConnectorTableMetadata.Column::name).toList()
                        .equals(metadata.columns())) {
            throw new IllegalArgumentException("Write receipt requires the complete actual connector schema");
        }
        return new WriteDeclarationSnapshot(table, schemaDigest(metadata),
                generatedColumnsDigest(generatedDefinitions), hasGeneratedColumns);
    }

    /** Hashes the validated GeneratedColumnSet protobuf bytes, including its explicit empty declaration. */
    public static String generatedColumnsDigest(byte[] canonicalDefinitions) {
        return sha256(Objects.requireNonNull(canonicalDefinitions, "canonicalDefinitions"));
    }

    private static String schemaDigest(ConnectorTableMetadata metadata) {
        try (var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes)) {
            output.writeInt(metadata.typedColumns().size());
            for (var column : metadata.typedColumns()) {
                writeText(output, column.name());
                writeText(output, column.type().name());
                output.writeInt(column.fieldId().orElse(-1));
                writeText(output, column.parameterizedType() == null
                        ? "" : column.parameterizedType().getDisplayName());
            }
            return sha256(bytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode in-memory connector schema", impossible);
        }
    }

    private static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JDK SHA-256 unavailable", impossible);
        }
    }

    private static void requireDigest(String digest) {
        if (digest == null || !digest.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Write declaration requires a canonical SHA-256 digest");
        }
    }
}
