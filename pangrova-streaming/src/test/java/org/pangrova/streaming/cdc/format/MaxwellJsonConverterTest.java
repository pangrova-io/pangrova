package org.pangrova.streaming.cdc.format;

import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRecordConversionException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaxwellJsonConverterTest {

    private final MaxwellJsonConverter converter = new MaxwellJsonConverter();

    @Test
    void formatIdIsMaxwellJson() {
        assertThat(converter.formatId()).isEqualTo("maxwell-json");
    }

    @Test
    void parseInsertEvent() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "insert",
                  "data": {"id": 1, "name": "Alice"},
                  "ts": 1700000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(env.tableName()).isEqualTo("mydb.users");
        assertThat(env.before()).isNull();
        assertThat(env.after()).isNotNull();
        assertThat(env.after().get("id")).isEqualTo(1);
        assertThat(env.after().get("name")).isEqualTo("Alice");
        // Maxwell ts is in seconds
        assertThat(env.eventTime()).isEqualTo(Instant.ofEpochSecond(1700000000L));
    }

    @Test
    void parseUpdateEvent() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "update",
                  "data": {"id": 1, "name": "Bob"},
                  "old": {"name": "Alice"},
                  "ts": 1700000001
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.UPDATE);
        assertThat(env.before().get("name")).isEqualTo("Alice");
        assertThat(env.after().get("name")).isEqualTo("Bob");
    }

    @Test
    void parseDeleteEvent() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "delete",
                  "data": {"id": 1, "name": "Alice"},
                  "ts": 1700000002
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.DELETE);
        assertThat(env.before()).isNotNull();
        assertThat(env.before().get("name")).isEqualTo("Alice");
        assertThat(env.after()).isNull();
    }

    @Test
    void missingTypeFieldThrows() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("'type' field");
    }

    @Test
    void unknownOperationTypeThrows() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "truncate",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Unknown Maxwell operation type");
    }

    @Test
    void malformedJsonThrows() {
        byte[] garbage = "not json".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> converter.deserialize(garbage))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Failed to parse Maxwell JSON");
    }

    @Test
    void updateWithoutOldFieldHasNullBefore() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "update",
                  "data": {"id": 1, "name": "Bob"},
                  "ts": 1700000001
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.UPDATE);
        assertThat(env.before()).isNull();
        assertThat(env.after().get("name")).isEqualTo("Bob");
    }

    @Test
    void missingDatabaseUsesTableOnly() {
        String json = """
                {
                  "table": "users",
                  "type": "insert",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.tableName()).isEqualTo("users");
    }

    @Test
    void caseInsensitiveOperationType() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "INSERT",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
    }

    @Test
    void missingTimestampDefaultsToEpoch() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "insert",
                  "data": {"id": 1}
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.eventTime()).isEqualTo(Instant.EPOCH);
    }

    @Test
    void schemaVersionAlwaysZero() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "insert",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.schemaVersion()).isZero();
    }

    @Test
    void sourceInfoContainsDatabaseAndTable() {
        String json = """
                {
                  "database": "mydb",
                  "table": "users",
                  "type": "insert",
                  "data": {"id": 1},
                  "ts": 1700000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.sourceInfo())
                .containsEntry("database", "mydb")
                .containsEntry("table", "users");
    }
}
