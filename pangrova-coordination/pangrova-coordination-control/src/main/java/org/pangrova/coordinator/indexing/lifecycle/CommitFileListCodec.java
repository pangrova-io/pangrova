package org.pangrova.coordinator.indexing.lifecycle;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.common.json.JsonMapperFactory;

import java.util.List;

/** Lossless JSON encoding for connector-owned data-file paths in the durable commit log. */
final class CommitFileListCodec {

    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() {};

    private CommitFileListCodec() {}

    static String encode(List<String> paths) {
        List<String> value = paths == null ? List.of() : List.copyOf(paths);
        if (value.stream().anyMatch(path -> path == null || path.isBlank())) {
            throw new IllegalArgumentException("Commit file paths must not be null or blank");
        }
        try {
            return JsonMapperFactory.writer().writeValueAsString(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Cannot encode commit file paths", failure);
        }
    }

    static List<String> decode(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            List<String> value = JsonMapperFactory.reader().forType(STRING_LIST).readValue(json);
            if (value == null || value.stream().anyMatch(path -> path == null || path.isBlank())) {
                throw new IllegalArgumentException("Commit file paths must be a JSON string array");
            }
            return List.copyOf(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Commit file paths are not valid JSON", failure);
        }
    }
}
