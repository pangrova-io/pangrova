package org.pangrova.streaming.cdc;

import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;
import org.pangrova.streaming.cdc.format.DebeziumJsonConverter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies CDC event field evolution is handled by the strong-typed model.
 *
 * <p><b>Validates: Requirements 1.58, 2.58</b></p>
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Extra unknown fields in CDC events are preserved (schema evolution)</li>
 *   <li>Missing optional fields use defaults</li>
 *   <li>Round-trip: JSON → CdcEnvelope → verify all fields</li>
 * </ul>
 */
class CdcEnvelopeFieldEvolutionTest {

    @Test
    void extraUnknownFieldsArePreservedInCdcRow() {
        String json = """
                {"payload":{"op":"c",\
                "before":null,\
                "after":{"id":1,"name":"alice","email":"alice@example.com","age":30,"new_field":"surprise"},\
                "ts_ms":1700000000000,\
                "source":{"db":"mydb","table":"users"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(envelope.after()).isNotNull();
        assertThat(envelope.after().columnNames())
                .containsExactlyInAnyOrder("id", "name", "email", "age", "new_field");
        assertThat(envelope.after().get("new_field")).isEqualTo("surprise");
        assertThat(envelope.after().get("id")).isEqualTo(1);
        assertThat(envelope.after().get("name")).isEqualTo("alice");
        assertThat(envelope.after().get("email")).isEqualTo("alice@example.com");
        assertThat(envelope.after().get("age")).isEqualTo(30);
    }

    @Test
    void missingOptionalFieldsUseDefaults() {
        // Minimal message: no "source" block, no "before"
        String json = """
                {"op":"c","before":null,"after":{"id":1},\
                "ts_ms":1700000000000}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(envelope.before()).isNull();
        assertThat(envelope.after()).isNotNull();
        assertThat(envelope.after().get("id")).isEqualTo(1);
        assertThat(envelope.tableName()).isEqualTo("unknown");
        // No struct_version in source → falls back to field-name hash strategy
        // Single field "id" → List.of("id").hashCode()
        assertThat(envelope.schemaVersion()).isEqualTo(java.util.List.of("id").hashCode());
        assertThat(envelope.sourceInfo()).isEmpty();
    }

    @Test
    void roundTripInsertEventPreservesAllFields() {
        String json = """
                {"payload":{"op":"c",\
                "before":null,\
                "after":{"id":42,"name":"bob","active":true,"score":3.14},\
                "ts_ms":1700000000000,\
                "source":{"db":"testdb","table":"accounts","struct_version":"2","connector":"mysql"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(envelope.tableName()).isEqualTo("testdb.accounts");
        assertThat(envelope.eventTime()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
        assertThat(envelope.schemaVersion()).isEqualTo(2);
        assertThat(envelope.before()).isNull();

        CdcRow after = envelope.after();
        assertThat(after).isNotNull();
        assertThat(after.get("id")).isEqualTo(42);
        assertThat(after.get("name")).isEqualTo("bob");
        assertThat(after.get("active")).isEqualTo(true);
        assertThat(after.get("score")).isEqualTo(3.14);

        assertThat(envelope.sourceInfo())
                .containsEntry("db", "testdb")
                .containsEntry("table", "accounts")
                .containsEntry("connector", "mysql");
    }

    @Test
    void roundTripUpdateEventPreservesBeforeAndAfter() {
        String json = """
                {"payload":{"op":"u",\
                "before":{"id":1,"name":"old_name","status":"active"},\
                "after":{"id":1,"name":"new_name","status":"active","extra_col":"added"},\
                "ts_ms":1700000000000,\
                "source":{"db":"mydb","table":"users"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.operation()).isEqualTo(CdcOperation.UPDATE);

        CdcRow before = envelope.before();
        assertThat(before).isNotNull();
        assertThat(before.get("id")).isEqualTo(1);
        assertThat(before.get("name")).isEqualTo("old_name");
        assertThat(before.columnCount()).isEqualTo(3);

        CdcRow after = envelope.after();
        assertThat(after).isNotNull();
        assertThat(after.get("name")).isEqualTo("new_name");
        // Schema evolution: after has extra column not in before
        assertThat(after.containsColumn("extra_col")).isTrue();
        assertThat(after.get("extra_col")).isEqualTo("added");
        assertThat(after.columnCount()).isEqualTo(4);
    }

    @Test
    void roundTripDeleteEventPreservesBefore() {
        String json = """
                {"payload":{"op":"d",\
                "before":{"id":99,"name":"deleted_user"},\
                "after":null,\
                "ts_ms":1700000000000,\
                "source":{"db":"mydb","table":"users"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.operation()).isEqualTo(CdcOperation.DELETE);
        assertThat(envelope.after()).isNull();
        assertThat(envelope.before()).isNotNull();
        assertThat(envelope.before().get("id")).isEqualTo(99);
        assertThat(envelope.before().get("name")).isEqualTo("deleted_user");
    }

    @Test
    void cdcRowIsImmutable() {
        Map<String, Object> mutable = new java.util.LinkedHashMap<>();
        mutable.put("id", 1);
        mutable.put("name", "test");

        CdcRow row = CdcRow.of(mutable);

        // Mutating the original map does not affect the CdcRow
        mutable.put("injected", "bad");
        assertThat(row.containsColumn("injected")).isFalse();
        assertThat(row.columnCount()).isEqualTo(2);

        // toMap() returns unmodifiable view
        assertThat(row.toMap()).isUnmodifiable();
    }

    @Test
    void cdcEnvelopeSourceInfoIsImmutable() {
        String json = """
                {"payload":{"op":"c","before":null,"after":{"id":1},\
                "ts_ms":1700000000000,\
                "source":{"db":"mydb","table":"users"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        assertThat(envelope.sourceInfo()).isUnmodifiable();
    }

    @Test
    void nullColumnValuesArePreserved() {
        String json = """
                {"payload":{"op":"c","before":null,\
                "after":{"id":1,"name":null,"email":"test@test.com"},\
                "ts_ms":1700000000000,\
                "source":{"db":"mydb","table":"users"}}}""";

        CdcEnvelope envelope = parseDebezium(json);

        CdcRow after = envelope.after();
        assertThat(after).isNotNull();
        assertThat(after.containsColumn("name")).isTrue();
        assertThat(after.get("name")).isNull();
        assertThat(after.get("email")).isEqualTo("test@test.com");
    }

    private static final DebeziumJsonConverter CONVERTER = new DebeziumJsonConverter();

    private static CdcEnvelope parseDebezium(String json) {
        return CONVERTER.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
