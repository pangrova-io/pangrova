package org.pangrova.connector.api.protocol;

import org.pangrova.connector.api.enums.ConnectorReadMode;
import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

/**
 * Read extension descriptor carrying source identity, versioning, schema, and read mode.
 *
 * <p>This is a Java model placeholder. The actual Protobuf message will be defined
 * when protobuf-maven-plugin is wired into pangrova-connector.</p>
 */
@Experimental
public record PangrovaReadExtension(
        String sourceId,
        String sourceType,
        long versionId,
        long versionTimestampMs,
        long fromVersionId,
        long toVersionId,
        int schemaVersion,
        byte[] arrowSchema,
        ConnectorReadMode readMode,
        Map<String, String> properties) {


    public PangrovaReadExtension {
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }
}
