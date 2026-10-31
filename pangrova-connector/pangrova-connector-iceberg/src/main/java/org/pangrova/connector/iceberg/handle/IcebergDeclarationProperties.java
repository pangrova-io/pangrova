package org.pangrova.connector.iceberg.handle;

import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.protocol.metadata.GeneratedColumnMetadataCodec;
import org.pangrova.common.api.annotation.Internal;

import java.util.LinkedHashMap;
import java.util.Map;

/** Canonical, non-secret declaration properties shared by write handles and snapshot evidence. */
@Internal
public final class IcebergDeclarationProperties {
    private IcebergDeclarationProperties() {}

    /** Captures generated declarations, including explicit absence, and logical vector dimensions. */
    public static Map<String, String> capture(Map<String, String> properties) {
        var result = new LinkedHashMap<>(GeneratedColumnMetadataCodec.encode(
                GeneratedColumnMetadataCodec.decode(properties)));
        properties.forEach((key, value) -> {
            if (key.startsWith(IcebergTypeMapper.VECTOR_DIMENSION_KEY_PREFIX)) {
                result.put(key, value);
            }
        });
        return Map.copyOf(result);
    }

    /** Requires a complete canonical capture, without arbitrary table properties. */
    public static Map<String, String> requireCanonical(Map<String, String> properties) {
        var canonical = capture(properties);
        if (!canonical.equals(properties)) {
            throw new IllegalArgumentException("Iceberg write declaration properties are not canonical");
        }
        return canonical;
    }
}
