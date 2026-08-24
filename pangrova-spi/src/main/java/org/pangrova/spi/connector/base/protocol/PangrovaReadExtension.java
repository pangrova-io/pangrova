package org.pangrova.spi.connector.base.protocol;

import java.util.Map;

/**
 * Read extension descriptor carrying source identity, versioning, schema, and read mode.
 *
 * <p>This is a Java model placeholder. The actual Protobuf message will be defined
 * when protobuf-maven-plugin is wired into pangrova-connector.</p>
 */
public record PangrovaReadExtension(
        String sourceId,
        String sourceType,
        long versionId,
        long versionTimestampMs,
        long fromVersionId,
        long toVersionId,
        int schemaVersion,
        byte[] arrowSchema,
        ReadMode readMode,
        Map<String, String> properties) {

    public enum ReadMode {
        SNAPSHOT,
        TIME_TRAVEL,
        INCREMENTAL,
        CHANGELOG
    }

    public PangrovaReadExtension {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }
}
