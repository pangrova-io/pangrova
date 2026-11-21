package org.pangrova.batch.api.page;

import org.junit.jupiter.api.Test;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RowPageConverterBinaryTest {
    @Test
    void sourceAndReturnedArraysCannotMutateSharedPageOrRegionValues() {
        byte[] source = {1, 2, 3};
        var row = TypedRow.builder().putRaw("v", source).build();
        var page = RowPageConverter.fromLegacy(List.of("v"), List.of(row));
        var block = page.getBlock(0);
        var region = block.getRegion(0, 1);

        source[0] = 99;
        block.getSlice(0)[1] = 99;
        region.getSlice(0)[2] = 99;
        var restored = RowPageConverter.toLegacy(page, List.of("v")).get(0);
        ((byte[]) restored.getRaw("v"))[0] = 99;

        assertThat(block.getSlice(0)).containsExactly(1, 2, 3);
        assertThat(region.getSlice(0)).containsExactly(1, 2, 3);
        assertThat((byte[]) row.getRaw("v")).containsExactly(1, 2, 3);
        assertThat((byte[]) restored.getRaw("v")).containsExactly(1, 2, 3);
    }

    @Test
    void arbitraryBinaryAndMixedStringsKeepTheirLogicalTypes() {
        byte[] bytes = {(byte) 0xff, 0, (byte) 0x80};
        var rows = List.of(TypedRow.of(Map.of("v", new ColumnValue.BytesValue(bytes))),
                TypedRow.of(Map.of("v", new ColumnValue.StringValue("text"))),
                TypedRow.of(Map.of("v", new ColumnValue.NullValue())));
        var restored = RowPageConverter.toLegacy(RowPageConverter.fromLegacy(List.of("v"), rows), List.of("v"));
        assertThat(restored.get(0).get("v")).isInstanceOf(ColumnValue.BytesValue.class);
        assertThat(((ColumnValue.BytesValue) restored.get(0).get("v")).value()).containsExactly(bytes);
        assertThat(restored.get(1).get("v")).isEqualTo(rows.get(1).get("v"));
        assertThat(restored.get(2).get("v")).isInstanceOf(ColumnValue.NullValue.class);
    }
}
