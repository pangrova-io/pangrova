package org.pangrova.common.json.provider.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class JsonMapperFactoryTest {

    @Test
    void sharedReturnsNonNull() {
        assertThat(JsonMapperFactory.shared()).isNotNull();
    }

    @Test
    void sharedReturnsDifferentInstancesEachCall() {
        ObjectMapper first = JsonMapperFactory.shared();
        ObjectMapper second = JsonMapperFactory.shared();
        assertThat(first).isNotSameAs(second);
    }

    @Test
    void createReturnsIndependentInstance() {
        ObjectMapper a = JsonMapperFactory.create();
        ObjectMapper b = JsonMapperFactory.create();
        assertThat(a).isNotSameAs(b);
        assertThat(a).isNotSameAs(JsonMapperFactory.shared());
    }

    @Test
    void readerAndWriterAreNonNull() {
        assertThat(JsonMapperFactory.reader()).isNotNull();
        assertThat(JsonMapperFactory.writer()).isNotNull();
    }

    @Test
    void unknownPropertiesAreIgnoredOnDeserialization() throws JsonProcessingException {
        String json = "{\"name\":\"test\",\"unknownField\":42}";
        ObjectMapper mapper = JsonMapperFactory.shared();
        assertThatCode(() -> mapper.readValue(json, SimpleDto.class)).doesNotThrowAnyException();
        SimpleDto dto = mapper.readValue(json, SimpleDto.class);
        assertThat(dto.name).isEqualTo("test");
    }

    @Test
    void javaTimeInstantRoundTrip() throws JsonProcessingException {
        Instant now = Instant.parse("2024-06-15T10:30:00Z");
        TimeDto original = new TimeDto();
        original.timestamp = now;

        ObjectMapper mapper = JsonMapperFactory.shared();
        String json = mapper.writeValueAsString(original);

        // ISO-8601 format, not numeric timestamp
        assertThat(json).contains("2024-06-15");
        assertThat(json).doesNotMatch(".*\"timestamp\"\\s*:\\s*\\d+.*");

        TimeDto restored = mapper.readValue(json, TimeDto.class);
        assertThat(restored.timestamp).isEqualTo(now);
    }

    @Test
    void javaTimeLocalDateRoundTrip() throws JsonProcessingException {
        DateDto original = new DateDto();
        original.date = LocalDate.of(2024, 1, 15);
        original.dateTime = LocalDateTime.of(2024, 1, 15, 9, 30, 0);

        ObjectMapper mapper = JsonMapperFactory.shared();
        String json = mapper.writeValueAsString(original);

        assertThat(json).contains("2024-01-15");

        DateDto restored = mapper.readValue(json, DateDto.class);
        assertThat(restored.date).isEqualTo(original.date);
        assertThat(restored.dateTime).isEqualTo(original.dateTime);
    }

    @Test
    void readerWriterRoundTrip() throws JsonProcessingException {
        SimpleDto original = new SimpleDto();
        original.name = "via-reader-writer";

        ObjectWriter writer = JsonMapperFactory.writer();
        String json = writer.writeValueAsString(original);

        ObjectReader reader = JsonMapperFactory.reader().forType(SimpleDto.class);
        SimpleDto restored = reader.readValue(json);
        assertThat(restored.name).isEqualTo("via-reader-writer");
    }

    @Test
    void mutatingCopyDoesNotAffectSubsequentSharedCalls() throws JsonProcessingException {
        ObjectMapper copy = JsonMapperFactory.shared();
        // Mutate the copy — should not affect future shared() calls
        copy.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        // A fresh shared() should still tolerate unknown properties
        ObjectMapper fresh = JsonMapperFactory.shared();
        String json = "{\"name\":\"ok\",\"extra\":1}";
        assertThatCode(() -> fresh.readValue(json, SimpleDto.class)).doesNotThrowAnyException();
    }

    // --- test DTOs ---

    static class SimpleDto {
        public String name;
    }

    static class TimeDto {
        public Instant timestamp;
    }

    static class DateDto {
        public LocalDate date;
        public LocalDateTime dateTime;
    }
}
