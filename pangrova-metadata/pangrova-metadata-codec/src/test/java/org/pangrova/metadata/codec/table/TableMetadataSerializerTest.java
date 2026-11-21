package org.pangrova.metadata.codec.table;

import org.pangrova.metadata.api.exception.MetadataSerializationException;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link TableMetadataSerializer} covering round-trip serialization,
 * edge cases, and error handling.
 */
class TableMetadataSerializerTest {

    private TableMetadataSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new TableMetadataSerializer();
    }

    // ── Helpers ──

    private static TableMetadata sampleTable() {
        return new TableMetadata(
                "catalog1", "schema1", "table1",
                List.of(
                        new ColumnMetadata("id", ColumnType.of("INT"), false, "primary key", Map.of("pk", "true")),
                        new ColumnMetadata("name", ColumnType.varchar(255), true, "user name", Map.of())),
                Map.of("year", "identity"),
                List.of("id"),
                Map.of("format", "parquet"),
                Instant.parse("2024-01-01T00:00:00Z"),
                Instant.parse("2024-06-15T12:30:00Z"));
    }

    // ── Round-trip ──

    @Test
    void roundTripPreservesAllFields() {
        TableMetadata original = sampleTable();

        String json = serializer.serialize(original);
        TableMetadata restored = serializer.deserialize(json);

        assertThat(restored.catalog()).isEqualTo(original.catalog());
        assertThat(restored.schema()).isEqualTo(original.schema());
        assertThat(restored.table()).isEqualTo(original.table());
        assertThat(restored.columns()).isEqualTo(original.columns());
        assertThat(restored.partitionSpec()).isEqualTo(original.partitionSpec());
        assertThat(restored.sortOrder()).isEqualTo(original.sortOrder());
        assertThat(restored.properties()).isEqualTo(original.properties());
        assertThat(restored.createdAt()).isEqualTo(original.createdAt());
        assertThat(restored.updatedAt()).isEqualTo(original.updatedAt());
    }

    @Test
    void roundTripWithMinimalTable() {
        TableMetadata minimal = new TableMetadata(
                "c", "s", "t",
                List.of(), Map.of(), List.of(), Map.of(),
                null, null);

        String json = serializer.serialize(minimal);
        TableMetadata restored = serializer.deserialize(json);

        assertThat(restored.catalog()).isEqualTo("c");
        assertThat(restored.schema()).isEqualTo("s");
        assertThat(restored.table()).isEqualTo("t");
        assertThat(restored.columns()).isEmpty();
        assertThat(restored.partitionSpec()).isEmpty();
    }

    @Test
    void roundTripWithDecimalAndVarcharTypes() {
        TableMetadata table = new TableMetadata(
                "cat", "sch", "tbl",
                List.of(
                        new ColumnMetadata("price", ColumnType.decimal(18, 4), false, "", Map.of()),
                        new ColumnMetadata("desc", ColumnType.varchar(1000), true, "", Map.of())),
                Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());

        String json = serializer.serialize(table);
        TableMetadata restored = serializer.deserialize(json);

        assertThat(restored.columns().get(0).type().precision()).isEqualTo(18);
        assertThat(restored.columns().get(0).type().scale()).isEqualTo(4);
        assertThat(restored.columns().get(1).type().length()).isEqualTo(1000);
    }

    // ── Serialization output format ──

    @Test
    void serializeProducesValidJson() {
        TableMetadata table = sampleTable();

        String json = serializer.serialize(table);

        assertThat(json).startsWith("{");
        assertThat(json).contains("\"catalog\"");
        assertThat(json).contains("\"schema\"");
        assertThat(json).contains("\"table\"");
    }

    @Test
    void serializeDatesAsIso8601NotTimestamps() {
        TableMetadata table = sampleTable();

        String json = serializer.serialize(table);

        assertThat(json).contains("2024-01-01T00:00:00Z");
        assertThat(json).doesNotContain("1704067200");
    }

    // ── Error handling ──

    @Test
    void deserializeInvalidJsonThrowsSerializationException() {
        assertThatThrownBy(() -> serializer.deserialize("not-json"))
                .isInstanceOf(MetadataSerializationException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    @Test
    void deserializeEmptyStringThrowsSerializationException() {
        assertThatThrownBy(() -> serializer.deserialize(""))
                .isInstanceOf(MetadataSerializationException.class);
    }

    @Test
    void deserializeNullThrowsSerializationException() {
        assertThatThrownBy(() -> serializer.deserialize(null))
                .isInstanceOf(MetadataSerializationException.class);
    }

    // ── Column metadata preservation ──

    @Test
    void columnTagsPreservedThroughRoundTrip() {
        TableMetadata table = new TableMetadata(
                "cat", "sch", "tbl",
                List.of(new ColumnMetadata("col1", ColumnType.of("INT"), true, "tagged",
                        Map.of("pii", "true", "classification", "sensitive"))),
                Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());

        String json = serializer.serialize(table);
        TableMetadata restored = serializer.deserialize(json);

        assertThat(restored.columns().get(0).tags())
                .containsEntry("pii", "true")
                .containsEntry("classification", "sensitive");
    }

    @Test
    void multipleColumnsPreserveOrder() {
        TableMetadata table = new TableMetadata(
                "cat", "sch", "tbl",
                List.of(
                        new ColumnMetadata("a", ColumnType.of("INT"), false, "", Map.of()),
                        new ColumnMetadata("b", ColumnType.of("BIGINT"), true, "", Map.of()),
                        new ColumnMetadata("c", ColumnType.of("VARCHAR"), true, "", Map.of())),
                Map.of(), List.of(), Map.of(),
                Instant.now(), Instant.now());

        String json = serializer.serialize(table);
        TableMetadata restored = serializer.deserialize(json);

        assertThat(restored.columns()).extracting(ColumnMetadata::name)
                .containsExactly("a", "b", "c");
    }
}
