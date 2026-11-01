package org.pangrova.spi.connector.property.connector.data;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.spi.connector.data.block.Block;
import org.pangrova.spi.connector.data.block.DictionaryBlock;
import org.pangrova.spi.connector.data.block.IntBlock;
import org.pangrova.spi.connector.data.block.LazyBlock;
import org.pangrova.spi.connector.data.block.LongBlock;
import org.pangrova.spi.connector.data.block.RunLengthEncodedBlock;
import org.pangrova.spi.connector.data.page.ColumnarPage;
import org.pangrova.spi.connector.data.page.RowPageConverter;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for columnar Block implementations and RowPageConverter.
 *
 * <p>Validates: Requirements 23.1-23.6</p>
 */
class ColumnarBlockPropertyTest {

    // ── Property 1: IntBlock round-trip ──

    @Property(tries = 10)
    void intBlockRoundTrip(
            @ForAll @Size(min = 1, max = 100) int[] values,
            @ForAll("booleanArray") boolean[] rawNulls) {
        int count = values.length;
        boolean[] nulls = fitNulls(rawNulls, count);
        IntBlock block = new IntBlock(values, nulls, count);

        assertThat(block.getPositionCount()).isEqualTo(count);
        for (int i = 0; i < count; i++) {
            assertThat(block.isNull(i)).isEqualTo(nulls[i]);
            assertThat(block.getInt(i)).isEqualTo(values[i]);
            assertThat(block.getLong(i)).isEqualTo((long) values[i]);
        }
        verifyIntBlockRegion(block, count);
    }

    private void verifyIntBlockRegion(IntBlock block, int count) {
        if (count < 2) {
            return;
        }
        int offset = count / 4;
        int length = count / 2;
        Block region = block.getRegion(offset, length);
        assertThat(region.getPositionCount()).isEqualTo(length);
        for (int i = 0; i < length; i++) {
            assertThat(region.getInt(i)).isEqualTo(block.getInt(offset + i));
            assertThat(region.isNull(i)).isEqualTo(block.isNull(offset + i));
        }
    }

    // ── Property 2: DictionaryBlock delegation ──

    @Property(tries = 10)
    void dictionaryBlockDelegation(
            @ForAll @Size(min = 1, max = 50) long[] dictValues,
            @ForAll("booleanArray") boolean[] rawNulls) {
        int dictSize = dictValues.length;
        boolean[] dictNulls = fitNulls(rawNulls, dictSize);
        LongBlock dictionary = new LongBlock(dictValues, dictNulls, dictSize);

        int posCount = Math.max(1, dictSize * 2);
        int[] ids = new int[posCount];
        for (int i = 0; i < posCount; i++) {
            ids[i] = i % dictSize;
        }

        DictionaryBlock block = new DictionaryBlock(dictionary, ids);
        assertThat(block.getPositionCount()).isEqualTo(posCount);

        for (int i = 0; i < posCount; i++) {
            int dictIdx = ids[i];
            assertThat(block.isNull(i)).isEqualTo(dictionary.isNull(dictIdx));
            assertThat(block.getLong(i)).isEqualTo(dictionary.getLong(dictIdx));
        }
    }

    // ── Property 3: LazyBlock loads once ──

    @Property(tries = 10)
    void lazyBlockLoadsOnce(
            @ForAll @Size(min = 1, max = 100) int[] values,
            @ForAll("booleanArray") boolean[] rawNulls) {
        int count = values.length;
        boolean[] nulls = fitNulls(rawNulls, count);
        IntBlock inner = new IntBlock(values, nulls, count);

        AtomicInteger loadCount = new AtomicInteger(0);
        LazyBlock lazy = new LazyBlock(() -> {
            loadCount.incrementAndGet();
            return inner;
        }, count);

        assertThat(lazy.getPositionCount()).isEqualTo(count);
        // Access multiple positions to trigger and verify single load
        for (int i = 0; i < count; i++) {
            assertThat(lazy.getInt(i)).isEqualTo(values[i]);
        }
        assertThat(loadCount.get()).isEqualTo(1);
    }

    // ── Property 4: RunLengthEncodedBlock repeats single value ──

    @Property(tries = 10)
    void rleBlockRepeatsValue(
            @ForAll long singleValue,
            @ForAll boolean isNull,
            @ForAll @IntRange(min = 1, max = 200) int positionCount) {
        LongBlock valueBlock = new LongBlock(
                new long[]{singleValue},
                new boolean[]{isNull},
                1);
        RunLengthEncodedBlock rle = new RunLengthEncodedBlock(valueBlock, positionCount);

        assertThat(rle.getPositionCount()).isEqualTo(positionCount);
        for (int i = 0; i < positionCount; i++) {
            assertThat(rle.isNull(i)).isEqualTo(isNull);
            assertThat(rle.getLong(i)).isEqualTo(singleValue);
        }

        // Region also repeats
        if (positionCount > 2) {
            Block region = rle.getRegion(1, positionCount - 2);
            assertThat(region.getPositionCount()).isEqualTo(positionCount - 2);
            assertThat(region.getLong(0)).isEqualTo(singleValue);
        }
    }

    // ── Property 5: RowPageConverter round-trip ──

    @Property(tries = 10)
    void legacyPageAdapterRoundTrip(
            @ForAll @IntRange(min = 1, max = 50) int rowCount) {
        List<String> columns = List.of("id", "name", "score");
        List<TypedRow> rows = buildTestRows(rowCount);

        Page original = new Page(columns, rows);
        ColumnarPage columnar = RowPageConverter.fromLegacy(original);
        List<TypedRow> restored = RowPageConverter.toLegacy(columnar, columns);

        assertThat(restored).hasSize(rowCount);
        for (int i = 0; i < rowCount; i++) {
            TypedRow orig = rows.get(i);
            TypedRow rest = restored.get(i);
            assertThat(rest.getInt("id")).isEqualTo(orig.getInt("id"));
            assertThat(rest.getString("name")).isEqualTo(orig.getString("name"));
            assertThat(rest.getLong("score")).isEqualTo(orig.getLong("score"));
        }
    }

    // ── Helpers ──

    @Provide
    Arbitrary<boolean[]> booleanArray() {
        return Arbitraries.of(true, false)
                .array(boolean[].class)
                .ofMinSize(1)
                .ofMaxSize(100);
    }

    private static boolean[] fitNulls(boolean[] raw, int size) {
        boolean[] result = new boolean[size];
        for (int i = 0; i < size; i++) {
            result[i] = raw[i % raw.length];
        }
        return result;
    }

    private static List<TypedRow> buildTestRows(int count) {
        TypedRow[] rows = new TypedRow[count];
        for (int i = 0; i < count; i++) {
            Map<String, ColumnValue> vals = new LinkedHashMap<>();
            vals.put("id", new ColumnValue.IntValue(i));
            vals.put("name", new ColumnValue.StringValue("row_" + i));
            vals.put("score", new ColumnValue.LongValue(i * 100L));
            rows[i] = TypedRow.of(vals);
        }
        return List.of(rows);
    }
}
