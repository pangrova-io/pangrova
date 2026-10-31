package org.pangrova.common.json.provider.jackson;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Centralized ObjectMapper factory.
 *
 * <p>All modules should use this factory instead of creating ObjectMapper instances directly.
 * The shared instance is pre-configured, thread-safe, and effectively immutable — callers
 * receive a defensive copy so modifications cannot pollute global state.
 *
 * <p>Default configuration applied to all instances:
 * <ul>
 *   <li>{@code FAIL_ON_UNKNOWN_PROPERTIES = false} — forward-compatible DTO deserialization</li>
 *   <li>{@code JavaTimeModule} registered — proper java.time serialization</li>
 *   <li>{@code WRITE_DATES_AS_TIMESTAMPS = false} — ISO-8601 date format</li>
 * </ul>
 */
public final class JsonMapperFactory {

    private static final ObjectMapper SHARED = configure(new ObjectMapper());

    private JsonMapperFactory() {}

    /**
     * Returns a defensively-copied ObjectMapper with sensible defaults.
     *
     * <p>Each call returns a new copy so callers cannot mutate the shared configuration.
     * For high-throughput paths prefer {@link #reader()} / {@link #writer()} to avoid
     * the copy overhead.
     */
    public static ObjectMapper shared() {
        return SHARED.copy();
    }

    /**
     * Returns a thread-safe {@link ObjectReader} backed by the shared configuration.
     * Preferred over {@link #shared()} for read-only deserialization in hot paths.
     */
    public static ObjectReader reader() {
        return SHARED.reader();
    }

    /**
     * Returns a thread-safe {@link ObjectWriter} backed by the shared configuration.
     * Preferred over {@link #shared()} for write-only serialization in hot paths.
     */
    public static ObjectWriter writer() {
        return SHARED.writer();
    }

    /**
     * Creates a new, independently-configured ObjectMapper.
     * Callers are responsible for the lifecycle of the returned instance.
     */
    public static ObjectMapper create() {
        return configure(new ObjectMapper());
    }

    private static ObjectMapper configure(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return mapper;
    }
}
