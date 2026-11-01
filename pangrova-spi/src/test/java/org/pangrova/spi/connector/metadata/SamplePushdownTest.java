package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates sample and ANALYZE pushdown SPI surface.
 */
class SamplePushdownTest {

    private record TestTable(String name) implements ConnectorTableHandle {}

    private static class StubMetadata implements ConnectorMetadata {
        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return null;
        }
        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }
        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }
        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }
        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }
        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return List.of();
        }
    }

    @Test
    void applySampleDefaultReturnsEmpty() {
        ConnectorMetadata metadata = new StubMetadata();
        assertThat(metadata.applySample(null, new TestTable("t"),
                new SampleInfo(10.0, SampleInfo.SampleMethod.BERNOULLI))).isEmpty();
    }

    @Test
    void applySampleOverrideReturnsResult() {
        ConnectorTableHandle rewritten = new TestTable("t$sampled");
        ConnectorMetadata metadata = new StubMetadata() {
            @Override
            public Optional<ApplicationResult<ConnectorTableHandle>> applySample(
                    ConnectorSession session, ConnectorTableHandle table, SampleInfo sample) {
                return Optional.of(ApplicationResult.residual(rewritten));
            }
        };

        var result = metadata.applySample(null, new TestTable("t"),
                new SampleInfo(50.0, SampleInfo.SampleMethod.SYSTEM));
        assertThat(result).isPresent();
        assertThat(result.get().handle()).isSameAs(rewritten);
        assertThat(result.get().guaranteed()).isFalse();
    }

    @Test
    void sampleInfoRejectsInvalidPercentage() {
        assertThatThrownBy(() -> new SampleInfo(0.0, SampleInfo.SampleMethod.BERNOULLI))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SampleInfo(101.0, SampleInfo.SampleMethod.SYSTEM))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sampleInfoRejectsNullMethod() {
        assertThatThrownBy(() -> new SampleInfo(50.0, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void analyzeTableDefaultReturnsEmpty() {
        ConnectorMetadata metadata = new StubMetadata();
        assertThat(metadata.analyzeTable(null, new TestTable("t"), List.of(), 100.0)).isEmpty();
    }

    @Test
    void analyzeTableOverrideCollectsStats() {
        ConnectorMetadata metadata = new StubMetadata() {
            @Override
            public Optional<TableStatistics> analyzeTable(
                    ConnectorSession session, ConnectorTableHandle table,
                    List<String> columns, double samplePercent) {
                return Optional.of(new TableStatistics(1000, -1, -1, Map.of()));
            }
        };

        var result = metadata.analyzeTable(null, new TestTable("t"), List.of("id", "name"), 10.0);
        assertThat(result).isPresent();
        assertThat(result.get().rowCount()).isEqualTo(1000);
    }
}
