/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.streaming.cdcformat.debezium;

import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConversionException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumJsonConverterTest {

    private final DebeziumJsonConverter converter = new DebeziumJsonConverter();

    @Test
    void formatIdIsDebeziumJson() {
        assertThat(converter.formatId()).isEqualTo("debezium-json");
    }

    @Test
    void parseInsertEvent() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1, "name": "Alice"},
                  "source": {"db": "inventory", "table": "customers", "struct_version": "1"},
                  "op": "c",
                  "ts_ms": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(env.tableName()).isEqualTo("inventory.customers");
        assertThat(env.before()).isNull();
        assertThat(env.after()).isNotNull();
        assertThat(env.after().get("id")).isEqualTo(1);
        assertThat(env.after().get("name")).isEqualTo("Alice");
        assertThat(env.eventTime()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
        assertThat(env.schemaVersion()).isEqualTo(1);
        assertThat(env.sourceInfo()).containsEntry("db", "inventory");
    }

    @Test
    void parseUpdateEvent() {
        String json = """
                {
                  "before": {"id": 1, "name": "Alice"},
                  "after": {"id": 1, "name": "Bob"},
                  "source": {"db": "inventory", "table": "customers"},
                  "op": "u",
                  "ts_ms": 1700000001000
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
                  "before": {"id": 1, "name": "Alice"},
                  "after": null,
                  "source": {"db": "inventory", "table": "customers"},
                  "op": "d",
                  "ts_ms": 1700000002000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.DELETE);
        assertThat(env.before()).isNotNull();
        assertThat(env.after()).isNull();
    }

    @Test
    void parseReadSnapshotEvent() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1, "name": "Alice"},
                  "source": {"db": "inventory", "table": "customers"},
                  "op": "r",
                  "ts_ms": 1700000003000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
    }

    @Test
    void parseWithPayloadWrapper() {
        String json = """
                {
                  "payload": {
                    "before": null,
                    "after": {"id": 1},
                    "source": {"db": "db1", "table": "t1"},
                    "op": "c",
                    "ts_ms": 1700000000000
                  }
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(env.tableName()).isEqualTo("db1.t1");
    }

    @Test
    void missingOpFieldThrows() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1},
                  "source": {"db": "db1", "table": "t1"},
                  "ts_ms": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("'op' field");
    }

    @Test
    void unknownOpThrows() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1},
                  "source": {"db": "db1", "table": "t1"},
                  "op": "x",
                  "ts_ms": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Unknown Debezium op: x");
    }

    @Test
    void malformedJsonThrows() {
        byte[] garbage = "not json".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> converter.deserialize(garbage))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Failed to parse Debezium JSON");
    }

    @Test
    void missingSourceDefaultsToUnknownTable() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1},
                  "op": "c",
                  "ts_ms": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.tableName()).isEqualTo("unknown");
        assertThat(env.sourceInfo()).isEmpty();
        // No struct_version → falls back to sorted field-name hash of ["id"]
        assertThat(env.schemaVersion()).isNotZero();
    }

    @Test
    void nonNumericSchemaVersionDefaultsToFieldHash() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1},
                  "source": {"db": "db1", "table": "t1", "version": "abc"},
                  "op": "c",
                  "ts_ms": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        // source.version is the connector version (not schema version), no struct_version present
        // → falls back to sorted field-name hash
        assertThat(env.schemaVersion()).isNotZero();
    }

    @Test
    void nullValuesInRowPreserved() {
        String json = """
                {
                  "before": null,
                  "after": {"id": 1, "email": null},
                  "source": {"db": "db1", "table": "t1"},
                  "op": "c",
                  "ts_ms": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.after().containsColumn("email")).isTrue();
        assertThat(env.after().get("email")).isNull();
    }

    @Test
    void variousValueTypes() {
        String json = """
                {
                  "before": null,
                  "after": {"i": 42, "l": 9999999999, "d": 3.14, "b": true, "s": "text"},
                  "source": {"db": "db1", "table": "t1"},
                  "op": "c",
                  "ts_ms": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.after().get("i")).isEqualTo(42);
        assertThat(env.after().get("l")).isEqualTo(9999999999L);
        assertThat(env.after().get("d")).isEqualTo(3.14);
        assertThat(env.after().get("b")).isEqualTo(true);
        assertThat(env.after().get("s")).isEqualTo("text");
    }
}
