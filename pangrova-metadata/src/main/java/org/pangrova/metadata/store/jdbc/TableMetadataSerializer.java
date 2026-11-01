package org.pangrova.metadata.store.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.metadata.api.error.MetadataSerializationException;
import org.pangrova.metadata.api.schema.TableMetadata;

/**
 * JSON serializer/deserializer for TableMetadata using Jackson.
 */
public class TableMetadataSerializer {

    private final ObjectMapper mapper;

    public TableMetadataSerializer() {
        this.mapper = JsonMapperFactory.create();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String serialize(TableMetadata metadata) {
        try {
            return mapper.writeValueAsString(metadata);
        } catch (Exception e) {
            throw new MetadataSerializationException("Failed to serialize TableMetadata", e);
        }
    }

    public TableMetadata deserialize(String json) {
        try {
            return mapper.readValue(json, TableMetadata.class);
        } catch (Exception e) {
            throw new MetadataSerializationException("Failed to deserialize TableMetadata", e);
        }
    }
}
