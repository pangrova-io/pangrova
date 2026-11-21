package org.pangrova.transport.client;

import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultBatchOwnershipTest {
    private static final ColumnDescriptor COLUMN = new ColumnDescriptor("value", Types.OTHER, "OTHER", 0, 0,
            java.util.Optional.empty());

    @Test
    void nullableCoordinatesRemainTypedAndDetachedAcrossSnapshots() {
        Float[] floats = {1.0f, null, 3.0f};
        Double[] doubles = {1.0000000000000002, null, -123456789.12345679};
        var batch = batch(java.util.Arrays.asList(floats, doubles));
        floats[0] = 99.0f;
        doubles[0] = 99.0;
        var first = (List<?>) batch.rows().get(0)[0];
        assertThat(first.get(0)).isInstanceOf(Float[].class);
        assertThat(first.get(1)).isInstanceOf(Double[].class);
        assertThat((Float[]) first.get(0)).containsExactly(1.0f, null, 3.0f);
        assertThat((Double[]) first.get(1)).containsExactly(1.0000000000000002, null, -123456789.12345679);
        ((Float[]) first.get(0))[1] = 99.0f;
        ((Double[]) first.get(1))[1] = 99.0;
        var second = (List<?>) batch.rows().get(0)[0];
        assertThat(((Float[]) second.get(0))[1]).isNull();
        assertThat(((Double[]) second.get(1))[1]).isNull();
    }

    @Test
    void snapshotsCallerCollectionsAndRowArrays() {
        var columns = new ArrayList<>(List.of(COLUMN));
        Object[] row = {42};
        var rows = new ArrayList<Object[]>();
        rows.add(row);
        var batch = new ResultBatch(columns, rows);
        columns.clear();
        rows.clear();
        row[0] = 99;
        assertThat(batch.columns()).containsExactly(COLUMN);
        assertThat(batch.rows()).singleElement().satisfies(value -> assertThat(value).containsExactly(42));
        assertThatThrownBy(() -> batch.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void returnedRowsCannotChangeTheSnapshot() {
        var batch = batch(42);
        var returned = batch.rows();
        returned.get(0)[0] = 99;
        assertThat(batch.rows().get(0)).containsExactly(42);
        assertThatThrownBy(returned::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void detachesNestedVectorBinaryListAndMapValuesIncludingNulls() {
        float[] vector = {0.25f, -0.5f};
        byte[] bytes = {1, 2};
        var list = new ArrayList<>(Arrays.asList(vector, null));
        var map = new LinkedHashMap<String, Object>();
        map.put("coordinates", list);
        map.put("binary", bytes);
        map.put("null", null);
        var batch = batch(map);
        vector[0] = 99;
        bytes[0] = 99;
        list.clear();
        map.clear();
        var first = (Map<?, ?>) batch.rows().get(0)[0];
        var nested = (List<?>) first.get("coordinates");
        assertThat(nested).hasSize(2);
        assertThat(nested.get(1)).isNull();
        assertThat((float[]) nested.get(0)).containsExactly(0.25f, -0.5f);
        assertThat((byte[]) first.get("binary")).containsExactly((byte) 1, (byte) 2);
        assertThat(first.containsKey("null")).isTrue();
        assertThatThrownBy(first::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(nested::clear).isInstanceOf(UnsupportedOperationException.class);
        ((float[]) nested.get(0))[0] = 88;
        ((byte[]) first.get("binary"))[0] = 88;
        var second = (Map<?, ?>) batch.rows().get(0)[0];
        assertThat((float[]) ((List<?>) second.get("coordinates")).get(0)).containsExactly(0.25f, -0.5f);
        assertThat((byte[]) second.get("binary")).containsExactly((byte) 1, (byte) 2);
    }

    @Test
    void detachesMutableArrowTextAndJdbcTimestampWithoutChangingPrecision() {
        var text = new Text("original");
        var timestamp = Timestamp.valueOf("2026-10-04 12:34:56.123456789");
        var batch = batch(List.of(text, timestamp));
        text.set("changed");
        timestamp.setNanos(0);
        var returned = (List<?>) batch.rows().get(0)[0];
        assertThat(returned.get(0).toString()).isEqualTo("original");
        assertThat(((Timestamp) returned.get(1)).getNanos()).isEqualTo(123456789);
        ((Text) returned.get(0)).clear();
        ((Timestamp) returned.get(1)).setNanos(0);
        var next = (List<?>) batch.rows().get(0)[0];
        assertThat(next.get(0).toString()).isEqualTo("original");
        assertThat(((Timestamp) next.get(1)).getNanos()).isEqualTo(123456789);
    }

    @Test
    void rejectsUnknownMutableCellsAndRecursiveContainers() {
        assertThatThrownBy(() -> batch(new AtomicInteger(42)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported result cell");
        var cycle = new ArrayList<Object>();
        cycle.add(cycle);
        assertThatThrownBy(() -> batch(cycle)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting exceeds");
    }

    private static ResultBatch batch(Object value) {
        return new ResultBatch(List.of(COLUMN), java.util.Collections.singletonList(new Object[] {value}));
    }
}
