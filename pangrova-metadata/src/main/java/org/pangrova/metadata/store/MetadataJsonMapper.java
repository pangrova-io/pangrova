package org.pangrova.metadata.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.common.json.JsonMapperFactory;

/**
 * Shared ObjectMapper singleton for metadata store serialization.
 *
 * <p>Delegates to {@link JsonMapperFactory} for configuration (JavaTimeModule,
 * ISO-8601 dates, lenient deserialization). All metadata store classes should
 * use this singleton instead of creating per-class ObjectMapper instances.</p>
 */
public final class MetadataJsonMapper {

    private static final ObjectMapper INSTANCE = JsonMapperFactory.create();

    private MetadataJsonMapper() {}

    /**
     * Returns the shared ObjectMapper instance.
     *
     * <p>The returned instance is thread-safe for read operations (serialization/deserialization).
     * Callers must not reconfigure the returned mapper.</p>
     */
    public static ObjectMapper instance() {
        return INSTANCE;
    }
}
