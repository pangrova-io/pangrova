package org.pangrova.common.json;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanonicalJsonMapperTest {
    public record Snapshot(Instant createdAt, List<String> fields) { }

    @Test
    void canonicalBytesAndTypedReadsPreserveTheExistingConfiguration() throws Exception {
        var snapshot = new Snapshot(Instant.parse("2026-10-07T00:00:00Z"), List.of("id"));
        String canonical = "{\"createdAt\":\"2026-10-07T00:00:00Z\",\"fields\":[\"id\"]}";

        assertThat(CanonicalJsonMapper.writeValueAsString(snapshot)).isEqualTo(canonical);
        assertThat(CanonicalJsonMapper.writeValueAsBytes(snapshot))
                .containsExactly(canonical.getBytes(StandardCharsets.UTF_8));
        assertThat(CanonicalJsonMapper.readValue(canonical.getBytes(StandardCharsets.UTF_8), Snapshot.class))
                .isEqualTo(snapshot);
        assertThat(CanonicalJsonMapper.readValue(
                "{\"fields\":[\"id\"]}", new TypeReference<Map<String, List<String>>>() { }))
                .containsExactlyEntriesOf(Map.of("fields", List.of("id")));
    }

    @Test
    void returnedTreesAreIndependentlyOwnedWithoutAConfigurationHandle() throws Exception {
        var object = CanonicalJsonMapper.createObjectNode();
        var array = CanonicalJsonMapper.createArrayNode();
        object.put("field", "original");
        array.add(object);
        byte[] frozen = CanonicalJsonMapper.writeValueAsBytes(array);
        object.put("field", "changed");
        array.removeAll();

        assertThat(new String(frozen, StandardCharsets.UTF_8)).isEqualTo("[{\"field\":\"original\"}]");
        assertThat(CanonicalJsonMapper.readTree("{\"field\":\"original\"}").path("field").asText())
                .isEqualTo("original");
        assertThat(CanonicalJsonMapper.createObjectNode()).isEmpty();
        assertThat(CanonicalJsonMapper.createArrayNode()).isEmpty();
    }

    @Test
    void malformedMetadataRemainsAnExplicitParsingFailure() {
        assertThatThrownBy(() -> CanonicalJsonMapper.readTree("{invalid"))
                .isInstanceOf(JsonProcessingException.class);
        assertThatThrownBy(() -> CanonicalJsonMapper.readValue("[invalid", new TypeReference<List<String>>() { }))
                .isInstanceOf(JsonProcessingException.class);
    }
}
