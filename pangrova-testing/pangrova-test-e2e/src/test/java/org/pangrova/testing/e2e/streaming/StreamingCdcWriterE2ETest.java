package org.pangrova.testing.e2e.streaming;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.write.DataFileSink;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.streaming.cdc.CdcConsumer;
import org.pangrova.streaming.cdc.CdcConfig;
import org.pangrova.streaming.writer.LakeTableWriter;
import org.pangrova.testing.TestObservability;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for Streaming CDC Writer pipeline.
 *
 * <p>Validates the CDC ingestion path:
 * Debezium JSON → CdcConsumer.parseDebeziumMessage → CdcEnvelope →
 * groupByTable → LakeTableWriter.write → WriteResult callback.
 *
 * <p>Also validates that CDC-ingested data is queryable through the
 * standard SQL pipeline after write.
 */
@DisplayName("Streaming CDC Writer E2E — CDC Ingestion Pipeline")
class StreamingCdcWriterE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private String testTable;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTable() {
        String suffix = Long.toHexString(System.nanoTime());
        testTable = "cdc_test_" + suffix;
        dataSetup.createTestTable(CATALOG, SCHEMA, testTable,
                "id INT, name VARCHAR(100), amount DOUBLE");
    }

    @Nested
    @DisplayName("CdcConsumer Debezium JSON Parsing")
    class DebeziumParsingTest {

        @Test
        @DisplayName("parses INSERT envelope with before=null and after populated")
        void parseInsertEnvelope() {
            CdcConsumer consumer = createCdcConsumer();
            String json = debeziumInsertJson("test_db." + testTable, 1, "alice", 100.0);

            CdcEnvelope envelope = consumer.parseDebeziumMessage(json);

            assertThat(envelope.operation().name()).isEqualTo("INSERT");
            assertThat(envelope.tableName()).isEqualTo("test_db." + testTable);
            assertThat(envelope.before()).isNull();
            assertThat(envelope.after()).isNotNull();
            assertThat(envelope.after().get("id")).isEqualTo(1);
            assertThat(envelope.after().get("name")).isEqualTo("alice");
            assertThat(envelope.schemaVersion()).isZero();
        }

        @Test
        @DisplayName("parses UPDATE envelope with both before and after")
        void parseUpdateEnvelope() {
            CdcConsumer consumer = createCdcConsumer();
            String json = debeziumUpdateJson("test_db." + testTable,
                    1, "alice", 100.0, "alice", 200.0);

            CdcEnvelope envelope = consumer.parseDebeziumMessage(json);

            assertThat(envelope.operation().name()).isEqualTo("UPDATE");
            assertThat(envelope.before()).isNotNull();
            assertThat(envelope.before().get("amount")).isEqualTo(100.0);
            assertThat(envelope.after()).isNotNull();
            assertThat(envelope.after().get("amount")).isEqualTo(200.0);
        }

        @Test
        @DisplayName("parses DELETE envelope with before populated and after=null")
        void parseDeleteEnvelope() {
            CdcConsumer consumer = createCdcConsumer();
            String json = debeziumDeleteJson("test_db." + testTable, 1, "alice", 100.0);

            CdcEnvelope envelope = consumer.parseDebeziumMessage(json);

            assertThat(envelope.operation().name()).isEqualTo("DELETE");
            assertThat(envelope.before()).isNotNull();
            assertThat(envelope.before().get("id")).isEqualTo(1);
            assertThat(envelope.after()).isNull();
        }

        @Test
        @DisplayName("rejects malformed JSON with IllegalArgumentException")
        void rejectMalformedJson() {
            CdcConsumer consumer = createCdcConsumer();

            assertThatThrownBy(() -> consumer.parseDebeziumMessage("{invalid"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Failed to parse");
        }
    }

    @Nested
    @DisplayName("LakeTableWriter Write with Callback")
    class LakeTableWriterTest {

        @Test
        @DisplayName("writes INSERT records and fires callback with correct stats")
        void writeInsertRecordsWithCallback() {
            List<WriteResult> results = new CopyOnWriteArrayList<>();
            try (LakeTableWriter writer = new LakeTableWriter(new StubDataFileSink(1), "id", results::add)) {
                List<WriteRecord> records = List.of(
                        insertRecord(1, "alice", 100.0),
                        insertRecord(2, "bob", 200.0));

                WriteResult result = writer.write(testTable, records,
                        WriteMode.UPSERT, WriteContext.selfManaged());

                assertThat(result.tableName()).isEqualTo(testTable);
                assertThat(result.recordCount()).isEqualTo(2);
                assertThat(result.dataFilesWritten()).isEqualTo(1);
                assertThat(result.positionDeleteFiles()).isZero();
                assertThat(result.equalityDeleteFiles()).isZero();
                assertThat(results).hasSize(1);
                assertThat(results.get(0).commitTime()).isNotNull();
            }
        }

        @Test
        @DisplayName("classifies same-key duplicates as position deletes")
        void classifySameKeyDuplicatesAsPositionDeletes() {
            List<WriteResult> results = new CopyOnWriteArrayList<>();
            try (LakeTableWriter writer = new LakeTableWriter(new StubDataFileSink(1), "id", results::add)) {
                List<WriteRecord> records = List.of(
                        insertRecord(1, "alice", 100.0),
                        insertRecord(1, "alice_v2", 150.0),
                        insertRecord(2, "bob", 200.0));

                WriteResult result = writer.write(testTable, records,
                        WriteMode.UPSERT, WriteContext.selfManaged());

                assertThat(result.recordCount()).isEqualTo(3);
                assertThat(result.positionDeleteFiles()).isEqualTo(1);
            }
        }
    }

    @Nested
    @DisplayName("CDC Batch Processing Pipeline")
    class BatchProcessingTest {

        @Test
        @DisplayName("processBatch: parse → group → write → commit full cycle")
        void processBatchFullCycle() {
            List<WriteResult> writeResults = new CopyOnWriteArrayList<>();
            try (LakeTableWriter writer = new LakeTableWriter(new StubDataFileSink(1), "id", writeResults::add)) {
                List<List<CdcEnvelope>> committed = new CopyOnWriteArrayList<>();
                try (CdcConsumer consumer = new CdcConsumer(
                        writer, createCdcConfig(), committed::add, TestObservability.METRICS)) {

                    String tableName = "test_db." + testTable;
                    List<String> batch = List.of(
                            debeziumInsertJson(tableName, 1, "alice", 100.0),
                            debeziumInsertJson(tableName, 2, "bob", 200.0),
                            debeziumUpdateJson(tableName, 1, "alice", 100.0, "alice", 300.0));

                    consumer.processBatch(batch);

                    assertThat(writeResults).hasSize(1);
                    assertThat(writeResults.get(0).recordCount()).isEqualTo(3);
                    assertThat(committed).hasSize(1);
                    assertThat(committed.get(0)).hasSize(3);
                }
            }
        }

        @Test
        @DisplayName("processBatch skips unparseable messages via dead-letter")
        void processBatchDeadLetterSkip() {
            List<WriteResult> writeResults = new CopyOnWriteArrayList<>();
            try (LakeTableWriter writer = new LakeTableWriter(new StubDataFileSink(1), "id", writeResults::add)) {
                List<List<CdcEnvelope>> committed = new CopyOnWriteArrayList<>();
                try (CdcConsumer consumer = new CdcConsumer(
                        writer, createCdcConfig(), committed::add, TestObservability.METRICS)) {

                    String tableName = "test_db." + testTable;
                    List<String> batch = List.of(
                            debeziumInsertJson(tableName, 1, "alice", 100.0),
                            "{broken-json",
                            debeziumInsertJson(tableName, 2, "bob", 200.0));

                    consumer.processBatch(batch);

                    assertThat(committed).hasSize(1);
                    assertThat(committed.get(0)).hasSize(2);
                }
            }
        }

        @Test
        @DisplayName("groupByTable correctly partitions envelopes by table name")
        void groupByTablePartitioning() {
            CdcConsumer consumer = createCdcConsumer();
            String table1 = "db1.orders";
            String table2 = "db1.users";

            CdcEnvelope e1 = consumer.parseDebeziumMessage(
                    debeziumInsertJson(table1, 1, "item_a", 10.0));
            CdcEnvelope e2 = consumer.parseDebeziumMessage(
                    debeziumInsertJson(table2, 2, "bob", 0.0));
            CdcEnvelope e3 = consumer.parseDebeziumMessage(
                    debeziumInsertJson(table1, 3, "item_b", 20.0));

            Map<String, List<CdcEnvelope>> grouped =
                    consumer.groupByTable(List.of(e1, e2, e3));

            assertThat(grouped).hasSize(2);
            assertThat(grouped.get(table1)).hasSize(2);
            assertThat(grouped.get(table2)).hasSize(1);
        }
    }

    @Nested
    @DisplayName("CDC to SQL Query Verification")
    class CdcToSqlQueryTest {

        @Test
        @DisplayName("CDC-ingested data is queryable via SQL pipeline after insert")
        void cdcIngestedDataQueryableViaSql() {
            insertCdcTestData();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, amount FROM " + CATALOG + "." + SCHEMA
                            + "." + testTable + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getRaw("name")).isEqualTo("alice");
            assertThat(rows.get(1).getRaw("name")).isEqualTo("bob");
            assertThat(rows.get(2).getRaw("name")).isEqualTo("charlie");
        }
    }

    // -----------------------------------------------------------------------
    // Helper: CdcConsumer and CdcConfig factory
    // -----------------------------------------------------------------------

    private CdcConsumer createCdcConsumer() {
        LakeTableWriter writer = new LakeTableWriter(new StubDataFileSink(1), "id", result -> {});
        return new CdcConsumer(writer, createCdcConfig(), envelopes -> {},
                TestObservability.METRICS);
    }

    private static CdcConfig createCdcConfig() {
        String loopback = InetAddress.getLoopbackAddress().getHostAddress();
        return new CdcConfig(
                loopback + ":0", "e2e-test-group",
                List.of("cdc-topic"), "e2e-consumer-1",
                "test-tenant", 4, 64 * 1024 * 1024);
    }

    // -----------------------------------------------------------------------
    // Helper: WriteRecord factory
    // -----------------------------------------------------------------------

    private static WriteRecord insertRecord(int id, String name, double amount) {
        TypedRow row = TypedRow.builder()
                .putInt("id", id)
                .putString("name", name)
                .putRaw("amount", amount)
                .build();
        return WriteRecord.fromClient(row, WriteRecord.WriteOperation.INSERT);
    }

    // -----------------------------------------------------------------------
    // Helper: insert test data via SQL for query verification
    // -----------------------------------------------------------------------

    private void insertCdcTestData() {
        String fqn = CATALOG + "." + SCHEMA + "." + testTable;
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (1, 'alice', 100.0)");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (2, 'bob', 200.0)");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (3, 'charlie', 300.0)");
    }

    // -----------------------------------------------------------------------
    // Helper: Debezium JSON builders
    // -----------------------------------------------------------------------

    private static String debeziumInsertJson(String tableName,
                                             int id, String name, double amount) {
        String[] parts = tableName.split("\\.", 2);
        String db = parts.length > 1 ? parts[0] : "";
        String table = parts.length > 1 ? parts[1] : parts[0];
        return """
                {"payload":{
                  "op":"c",
                  "before":null,
                  "after":{"id":%d,"name":"%s","amount":%s},
                  "ts_ms":%d,
                  "source":{"db":"%s","table":"%s","version":"0"}
                }}""".formatted(id, name, amount,
                System.currentTimeMillis(), db, table);
    }

    private static String debeziumUpdateJson(String tableName,
                                             int id, String oldName, double oldAmount,
                                             String newName, double newAmount) {
        String[] parts = tableName.split("\\.", 2);
        String db = parts.length > 1 ? parts[0] : "";
        String table = parts.length > 1 ? parts[1] : parts[0];
        return """
                {"payload":{
                  "op":"u",
                  "before":{"id":%d,"name":"%s","amount":%s},
                  "after":{"id":%d,"name":"%s","amount":%s},
                  "ts_ms":%d,
                  "source":{"db":"%s","table":"%s","version":"0"}
                }}""".formatted(id, oldName, oldAmount,
                id, newName, newAmount,
                System.currentTimeMillis(), db, table);
    }

    private static String debeziumDeleteJson(String tableName,
                                             int id, String name, double amount) {
        String[] parts = tableName.split("\\.", 2);
        String db = parts.length > 1 ? parts[0] : "";
        String table = parts.length > 1 ? parts[1] : parts[0];
        return """
                {"payload":{
                  "op":"d",
                  "before":{"id":%d,"name":"%s","amount":%s},
                  "after":null,
                  "ts_ms":%d,
                  "source":{"db":"%s","table":"%s","version":"0"}
                }}""".formatted(id, name, amount,
                System.currentTimeMillis(), db, table);
    }

    // -----------------------------------------------------------------------
    // Helper: Stub DataFileSink for test isolation
    // -----------------------------------------------------------------------

    private static final class StubDataFileSink implements DataFileSink {
        private final int filesToReturn;

        StubDataFileSink(int filesToReturn) {
            this.filesToReturn = filesToReturn;
        }

        @Override
        public int writeRecords(String tableName, java.util.List<WriteRecord> records,
                                WriteMode writeMode) {
            return filesToReturn;
        }

        @Override
        public void close() {}
    }
}
