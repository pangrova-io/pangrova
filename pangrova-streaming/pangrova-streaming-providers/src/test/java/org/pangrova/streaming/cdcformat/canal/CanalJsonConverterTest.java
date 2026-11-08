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
package org.pangrova.streaming.cdcformat.canal;

import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConversionException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanalJsonConverterTest {

    private final CanalJsonConverter converter = new CanalJsonConverter();

    @Test
    void formatIdIsCanalJson() {
        assertThat(converter.formatId()).isEqualTo("canal-json");
    }

    @Test
    void parseInsertEvent() {
        String json = """
                {
                  "data": [{"id": 1, "name": "Alice"}],
                  "type": "INSERT",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(env.tableName()).isEqualTo("mydb.users");
        assertThat(env.before()).isNull();
        assertThat(env.after()).isNotNull();
        assertThat(env.after().get("id")).isEqualTo(1);
        assertThat(env.after().get("name")).isEqualTo("Alice");
        assertThat(env.eventTime()).isEqualTo(Instant.ofEpochMilli(1700000000000L));
    }

    @Test
    void parseUpdateEventWithOld() {
        String json = """
                {
                  "data": [{"id": 1, "name": "Bob"}],
                  "old": [{"id": 1, "name": "Alice"}],
                  "type": "UPDATE",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000001000
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
                  "data": [{"id": 1, "name": "Alice"}],
                  "type": "DELETE",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000002000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.DELETE);
        assertThat(env.before()).isNotNull();
        assertThat(env.before().get("name")).isEqualTo("Alice");
        assertThat(env.after()).isNull();
    }

    @Test
    void multipleRowsInDataArray() {
        String json = """
                {
                  "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
                  "type": "INSERT",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        List<CdcEnvelope> envelopes = converter.convertBatch(
                List.of(json.getBytes(StandardCharsets.UTF_8)),
                (payload, ex) -> { throw new AssertionError("unexpected dead letter"); });

        assertThat(envelopes).hasSize(2);
        assertThat(envelopes.get(0).after().get("name")).isEqualTo("Alice");
        assertThat(envelopes.get(1).after().get("name")).isEqualTo("Bob");
    }

    @Test
    void deserializeReturnsSingleRowFromMultiRowMessage() {
        String json = """
                {
                  "data": [{"id": 1}, {"id": 2}],
                  "type": "INSERT",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.after().get("id")).isEqualTo(1);
    }

    @Test
    void missingTypeFieldThrows() {
        String json = """
                {
                  "data": [{"id": 1}],
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("'type' field");
    }

    @Test
    void missingDataArrayThrows() {
        String json = """
                {
                  "type": "INSERT",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("'data' array");
    }

    @Test
    void emptyDataArrayThrows() {
        String json = """
                {
                  "data": [],
                  "type": "INSERT",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void unknownOperationTypeThrows() {
        String json = """
                {
                  "data": [{"id": 1}],
                  "type": "TRUNCATE",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        assertThatThrownBy(() -> converter.deserialize(json.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Unknown Canal operation type");
    }

    @Test
    void malformedJsonThrows() {
        byte[] garbage = "not json".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> converter.deserialize(garbage))
                .isInstanceOf(CdcRecordConversionException.class)
                .hasMessageContaining("Failed to parse Canal JSON");
    }

    @Test
    void convertBatchRoutesFailuresToDeadLetter() {
        byte[] valid = """
                {"data": [{"id": 1}], "type": "INSERT", "database": "db", "table": "t", "ts": 0}
                """.getBytes(StandardCharsets.UTF_8);
        byte[] invalid = "bad".getBytes(StandardCharsets.UTF_8);

        List<byte[]> deadLettered = new ArrayList<>();
        List<CdcEnvelope> results = converter.convertBatch(
                List.of(valid, invalid),
                (payload, ex) -> deadLettered.add(payload));

        assertThat(results).hasSize(1);
        assertThat(deadLettered).hasSize(1);
        assertThat(deadLettered.get(0)).isSameAs(invalid);
    }

    @Test
    void caseInsensitiveOperationType() {
        String json = """
                {
                  "data": [{"id": 1}],
                  "type": "insert",
                  "database": "mydb",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.operation()).isEqualTo(CdcOperation.INSERT);
    }

    @Test
    void missingDatabaseUsesTableOnly() {
        String json = """
                {
                  "data": [{"id": 1}],
                  "type": "INSERT",
                  "table": "users",
                  "ts": 1700000000000
                }
                """;
        CdcEnvelope env = converter.deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertThat(env.tableName()).isEqualTo("users");
    }
}
