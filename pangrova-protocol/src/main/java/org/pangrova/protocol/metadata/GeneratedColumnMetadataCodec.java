package org.pangrova.protocol.metadata;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Canonical connector-property codec for generated-column catalog metadata.
 *
 * <p>The payload is a generated v2 protobuf shared by every runtime. It is not
 * an execution plan: the logical model is resolved to an exact version and
 * signature when a query or durable background page is planned.</p>
 */
public final class GeneratedColumnMetadataCodec {

    public static final int SCHEMA_VERSION = 1;
    public static final String PAYLOAD_PROPERTY =
            "pangrova.generated-columns.v1.payload";
    public static final String SHA256_PROPERTY =
            "pangrova.generated-columns.v1.sha256";

    private static final int MAX_DEFINITIONS = 256;
    private static final int MAX_PAYLOAD_BYTES = 1024 * 1024;

    private GeneratedColumnMetadataCodec() {}

    /** Returns the two canonical properties after validating the definition set. */
    public static Map<String, String> encode(GeneratedColumnSet definitions) {
        validate(definitions);
        byte[] payload = definitions.toByteArray();
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "generated-column metadata exceeds " + MAX_PAYLOAD_BYTES + " bytes");
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(PAYLOAD_PROPERTY, Base64.getEncoder().encodeToString(payload));
        properties.put(SHA256_PROPERTY, Base64.getEncoder().encodeToString(sha256(payload)));
        return Map.copyOf(properties);
    }

    /**
     * Decodes a catalog property map. An absent pair means no generated
     * columns; a partial, corrupt, oversized, or unsupported payload fails
     * closed instead of silently treating the columns as ordinary data.
     */
    public static GeneratedColumnSet decode(Map<String, String> properties) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        String encoded = properties.get(PAYLOAD_PROPERTY);
        String encodedDigest = properties.get(SHA256_PROPERTY);
        if (metadataAbsent(encoded, encodedDigest)) {
            return GeneratedColumnSet.newBuilder()
                    .setSchemaVersion(SCHEMA_VERSION)
                    .build();
        }
        requireMetadataPair(encoded, encodedDigest);
        byte[] payload = decodeBase64(encoded, "payload");
        validatePayloadSize(payload);
        byte[] expectedDigest = decodeBase64(encodedDigest, "SHA-256");
        validateDigest(expectedDigest, payload);
        return parse(payload);
    }

    private static boolean metadataAbsent(String encoded, String encodedDigest) {
        return encoded == null && encodedDigest == null;
    }

    private static void requireMetadataPair(String encoded, String encodedDigest) {
        if (encoded == null || encodedDigest == null) {
            throw new IllegalArgumentException(
                    "generated-column metadata payload and SHA-256 must be present together");
        }
    }

    private static void validatePayloadSize(byte[] payload) {
        if (payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "generated-column metadata exceeds " + MAX_PAYLOAD_BYTES + " bytes");
        }
    }

    private static void validateDigest(byte[] expectedDigest, byte[] payload) {
        if (expectedDigest.length != 32
                || !MessageDigest.isEqual(expectedDigest, sha256(payload))) {
            throw new IllegalArgumentException(
                    "generated-column metadata SHA-256 mismatch");
        }
    }

    private static GeneratedColumnSet parse(byte[] payload) {
        try {
            GeneratedColumnSet definitions = GeneratedColumnSet.parseFrom(payload);
            validate(definitions);
            return definitions;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "generated-column metadata is not a valid v2 protobuf", e);
        }
    }

    /** Validates structural invariants shared by DDL, planning, and recovery. */
    public static void validate(GeneratedColumnSet definitions) {
        validateSet(definitions);
        Set<String> targets = new HashSet<>();
        for (GeneratedColumnDefinition definition : definitions.getDefinitionsList()) {
            validateDefinition(definition, targets);
        }
    }

    private static void validateSet(GeneratedColumnSet definitions) {
        if (definitions == null) {
            throw new IllegalArgumentException("definitions must not be null");
        }
        if (definitions.getSchemaVersion() != SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "unsupported generated-column schema version: "
                            + definitions.getSchemaVersion());
        }
        if (definitions.getDefinitionsCount() > MAX_DEFINITIONS) {
            throw new IllegalArgumentException(
                    "generated-column definition count exceeds " + MAX_DEFINITIONS);
        }
    }

    private static void validateDefinition(
            GeneratedColumnDefinition definition, Set<String> targets) {
        requireText(definition.getTargetColumn(), "target_column");
        String target = definition.getTargetColumn().toLowerCase(Locale.ROOT);
        if (!targets.add(target)) {
            throw new IllegalArgumentException(
                    "duplicate generated target column: " + definition.getTargetColumn());
        }
        validateCapability(definition);
        validateExecutionMode(definition);
        validateOutputType(definition);
        validateSourceColumns(definition);
        validateOptions(definition);
    }

    private static void validateCapability(GeneratedColumnDefinition definition) {
        if (!definition.hasCapability()) {
            throw new IllegalArgumentException(
                    "generated column capability must be present: "
                            + definition.getTargetColumn());
        }
        requireText(definition.getCapability().getCapabilityId(), "capability_id");
        requireText(definition.getCapability().getSemanticVersion(),
                "capability semantic_version");
        if (definition.getCapability().getSignatureSha256().size() != 32) {
            throw new IllegalArgumentException(
                    "generated column capability signature must be SHA-256: "
                            + definition.getTargetColumn());
        }
    }

    private static void validateExecutionMode(GeneratedColumnDefinition definition) {
        if (definition.getExecutionMode() == ExecutionMode.EXECUTION_MODE_UNSPECIFIED) {
            throw new IllegalArgumentException(
                    "generated column execution mode must be specified: "
                            + definition.getTargetColumn());
        }
    }

    private static void validateOutputType(GeneratedColumnDefinition definition) {
        if (!definition.hasOutputType()) {
            throw new IllegalArgumentException(
                    "generated column output type must be present: "
                            + definition.getTargetColumn());
        }
        requireText(definition.getOutputType().getCanonicalSql(), "output canonical_sql");
    }

    private static void validateSourceColumns(GeneratedColumnDefinition definition) {
        if (definition.getSourceColumnsCount() == 0) {
            throw new IllegalArgumentException(
                    "generated column source_columns must not be empty: "
                            + definition.getTargetColumn());
        }
        Set<String> sources = new HashSet<>();
        for (String source : definition.getSourceColumnsList()) {
            requireText(source, "source_column");
            if (!sources.add(source.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "duplicate generated source column '" + source + "' for "
                                + definition.getTargetColumn());
            }
        }
    }

    private static void validateOptions(GeneratedColumnDefinition definition) {
        Set<String> optionNames = new HashSet<>();
        definition.getOptionsList().forEach(option -> {
            requireText(option.getName(), "option name");
            if (!optionNames.add(option.getName().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "duplicate generated-column option '" + option.getName() + "'");
            }
        });
    }

    private static byte[] decodeBase64(String value, String field) {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "generated-column " + field + " is not valid Base64", e);
        }
    }

    private static byte[] sha256(byte[] payload) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
