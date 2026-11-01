package org.pangrova.metadata.api.schema;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.ColumnType;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.store.jdbc.MetadataJsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual value ownership contracts only; these values are never offered as Native execution evidence.
 *  Artifact/indexing value-type ownership contracts live in pangrova-indexing (ArtifactValueOwnershipTest). */
class MetadataCollectionOwnershipTest {
    @Test
    void tableOwnsAllFourCollectionsAndNestedColumnTags() throws Exception {
        var tags = new HashMap<>(Map.of("classification", "public"));
        var column = new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", tags);
        var columns = new ArrayList<>(List.of(column));
        var partitions = new HashMap<>(Map.of("id", "identity"));
        var order = new ArrayList<>(List.of("id"));
        var properties = new HashMap<>(Map.of("format", "parquet"));
        var table = new TableMetadata("catalog", "schema", "table", columns, partitions,
                order, properties, Instant.EPOCH, Instant.EPOCH);
        byte[] canonical = MetadataJsonMapper.writeValueAsBytes(table);
        String digest = DigestUtils.sha256Hex(canonical);

        tags.clear();
        columns.clear();
        partitions.clear();
        order.clear();
        properties.clear();

        assertThat(table.columns()).containsExactly(column);
        assertThat(table.columns().get(0).tags()).containsExactlyEntriesOf(Map.of("classification", "public"));
        assertThat(table.partitionSpec()).containsExactlyEntriesOf(Map.of("id", "identity"));
        assertThat(table.sortOrder()).containsExactly("id");
        assertThat(table.properties()).containsExactlyEntriesOf(Map.of("format", "parquet"));
        assertThatThrownBy(() -> table.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.partitionSpec().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.sortOrder().set(0, "other")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.properties().put("format", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.columns().get(0).tags().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(MetadataJsonMapper.writeValueAsBytes(table)).containsExactly(canonical);
        assertThat(DigestUtils.sha256Hex(MetadataJsonMapper.writeValueAsBytes(table))).isEqualTo(digest);
    }

    @Test
    void nullableTableCollectionsBecomeImmutableEmptyValues() {
        var table = new TableMetadata("catalog", "schema", "table", null, null, null, null, null, null);
        assertThat(table.columns()).isEmpty();
        assertThat(table.partitionSpec()).isEmpty();
        assertThat(table.sortOrder()).isEmpty();
        assertThat(table.properties()).isEmpty();
        assertThat(table.createdAt()).isNull();
        assertThat(table.updatedAt()).isNull();
        assertThatThrownBy(() -> table.columns().add(new ColumnMetadata("id", ColumnType.of("INT"), true, null, null)))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.partitionSpec().put("id", "identity"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.sortOrder().add("id")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> table.properties().put("format", "parquet"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(new ColumnMetadata("id", ColumnType.of("INT"), true, null, null).tags()).isEmpty();
    }
}
