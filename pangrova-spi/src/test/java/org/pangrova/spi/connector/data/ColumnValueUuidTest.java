package org.pangrova.spi.connector.data;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnValueUuidTest {
    @ParameterizedTest
    @ValueSource(strings = {"00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff", "00112233-4455-6677-8899-aabbccddeeff"})
    void uuidValuesSurviveTypedResultRowsWithoutStringCoercion(String text) {
        UUID uuid = UUID.fromString(text);
        var row = TypedRow.fromRawMap(Map.of("id", uuid));
        assertThat(row.get("id")).isInstanceOf(ColumnValue.UuidValue.class);
        assertThat(row.getRaw("id")).isSameAs(uuid);
        assertThat(row.toRawMap()).containsEntry("id", uuid);
        assertThat(TypedRow.builder().putRaw("id", uuid).build()).isEqualTo(row);
    }
}
