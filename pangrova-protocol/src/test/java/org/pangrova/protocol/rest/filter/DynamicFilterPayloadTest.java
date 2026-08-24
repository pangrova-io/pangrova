package org.pangrova.protocol.rest.filter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DynamicFilterPayload}.
 *
 * <p><b>Validates: Requirements 1.38, 2.38, 2.39</b></p>
 */
class DynamicFilterPayloadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Nested
    class Construction {

        @Test
        void validPayloadPreservesAllFields() {
            byte[] data = {1, 2, 3, 4};
            var payload = new DynamicFilterPayload(
                    "f-1", "BLOOM", data, "s-2", 1);

            assertThat(payload.filterId()).isEqualTo("f-1");
            assertThat(payload.filterType()).isEqualTo("BLOOM");
            assertThat(payload.filterData()).isEqualTo(data);
            assertThat(payload.targetStageId()).isEqualTo("s-2");
            assertThat(payload.version()).isEqualTo(1);
        }

        @Test
        void filterDataIsDefensivelyCopied() {
            byte[] data = {1, 2, 3};
            var payload = new DynamicFilterPayload(
                    "f-1", "BLOOM", data, "s-2", 1);

            // Mutate original array
            data[0] = 99;
            assertThat(payload.filterData()[0]).isEqualTo((byte) 1);

            // Mutate returned array
            byte[] returned = payload.filterData();
            returned[0] = 99;
            assertThat(payload.filterData()[0]).isEqualTo((byte) 1);
        }

        @Test
        void emptyFilterDataAllowed() {
            var payload = new DynamicFilterPayload(
                    "f-1", "BLOOM", new byte[0], "s-2", 0);
            assertThat(payload.filterData()).isEmpty();
        }

        @Test
        void nullFilterIdThrows() {
            assertThatThrownBy(() -> new DynamicFilterPayload(
                    null, "BLOOM", new byte[0], "s-2", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterId");
        }

        @Test
        void blankFilterTypeThrows() {
            assertThatThrownBy(() -> new DynamicFilterPayload(
                    "f-1", "  ", new byte[0], "s-2", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterType");
        }

        @Test
        void nullFilterDataThrows() {
            assertThatThrownBy(() -> new DynamicFilterPayload(
                    "f-1", "BLOOM", null, "s-2", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("filterData");
        }

        @Test
        void blankTargetStageIdThrows() {
            assertThatThrownBy(() -> new DynamicFilterPayload(
                    "f-1", "BLOOM", new byte[0], "", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("targetStageId");
        }
    }

    @Nested
    class JsonRoundTrip {

        @Test
        void fullPayloadRoundTrip() throws Exception {
            byte[] data = {10, 20, 30, 40, 50};
            var original = new DynamicFilterPayload(
                    "f-1", "MIN_MAX", data, "s-3", 2);

            String json = MAPPER.writeValueAsString(original);
            var deserialized = MAPPER.readValue(json, DynamicFilterPayload.class);

            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        void emptyFilterDataRoundTrip() throws Exception {
            var original = new DynamicFilterPayload(
                    "f-1", "IN_LIST", new byte[0], "s-1", 0);

            String json = MAPPER.writeValueAsString(original);
            var deserialized = MAPPER.readValue(json, DynamicFilterPayload.class);

            assertThat(deserialized).isEqualTo(original);
        }
    }

    @Nested
    class VersionCompatibility {

        @Test
        void unknownFieldsIgnoredOnDeserialization() throws Exception {
            String json = """
                    {
                      "filter_id": "f-1",
                      "filter_type": "BLOOM",
                      "filter_data": "AQID",
                      "target_stage_id": "s-2",
                      "version": 1,
                      "future_field": "should-be-ignored"
                    }
                    """;

            var payload = MAPPER.readValue(json, DynamicFilterPayload.class);

            assertThat(payload.filterId()).isEqualTo("f-1");
            assertThat(payload.filterType()).isEqualTo("BLOOM");
            assertThat(payload.filterData()).isEqualTo(new byte[]{1, 2, 3});
            assertThat(payload.targetStageId()).isEqualTo("s-2");
            assertThat(payload.version()).isEqualTo(1);
        }

        @Test
        void missingVersionFieldDefaultsToZero() throws Exception {
            String json = """
                    {
                      "filter_id": "f-1",
                      "filter_type": "BLOOM",
                      "filter_data": "AQID",
                      "target_stage_id": "s-2"
                    }
                    """;

            var payload = MAPPER.readValue(json, DynamicFilterPayload.class);

            assertThat(payload.version()).isZero();
        }

        @Test
        void oldFormatMessageDeserializesSuccessfully() throws Exception {
            // Simulates an old-format message with only basic fields
            String oldFormatJson = """
                    {
                      "filter_id": "f-old",
                      "filter_type": "BLOOM",
                      "filter_data": "",
                      "target_stage_id": "s-old"
                    }
                    """;

            var payload = MAPPER.readValue(oldFormatJson, DynamicFilterPayload.class);

            assertThat(payload.filterId()).isEqualTo("f-old");
            assertThat(payload.filterType()).isEqualTo("BLOOM");
            assertThat(payload.filterData()).isEmpty();
            assertThat(payload.targetStageId()).isEqualTo("s-old");
            assertThat(payload.version()).isZero();
        }
    }

    @Nested
    class EqualityAndHashCode {

        @Test
        void equalPayloadsAreEqual() {
            byte[] data = {1, 2, 3};
            var a = new DynamicFilterPayload("f-1", "BLOOM", data, "s-1", 1);
            var b = new DynamicFilterPayload("f-1", "BLOOM", data, "s-1", 1);
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @Test
        void differentFilterDataNotEqual() {
            var a = new DynamicFilterPayload("f-1", "BLOOM", new byte[]{1}, "s-1", 1);
            var b = new DynamicFilterPayload("f-1", "BLOOM", new byte[]{2}, "s-1", 1);
            assertThat(a).isNotEqualTo(b);
        }
    }
}
