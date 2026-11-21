package org.pangrova.types.value;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ColumnValueBinaryOwnershipTest {
    @Test
    void constructorOwnsSnapshotOfSuppliedBytes() {
        byte[] source = {(byte) 0xff, 0, (byte) 0x80};
        var value = new ColumnValue.BytesValue(source);

        source[0] = 7;

        assertArrayEquals(new byte[]{(byte) 0xff, 0, (byte) 0x80}, value.value());
    }

    @Test
    void accessorDoesNotExposeOwnedBytes() {
        var value = new ColumnValue.BytesValue(new byte[]{1, 2, 3});

        value.value()[1] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, value.value());
    }

    @Test
    void typedRowRawBoundaryDetachesBothInputAndOutput() {
        byte[] source = {1, 2, 3};
        var row = TypedRow.builder().putRaw("binary", source).build();
        source[0] = 99;
        byte[] firstRead = (byte[]) row.getRaw("binary");
        assertArrayEquals(new byte[]{1, 2, 3}, firstRead);

        firstRead[1] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) row.getRaw("binary"));
    }

    @Test
    void emptyBinaryIsDistinctFromSqlNullAndNullPayloadIsRejected() {
        assertArrayEquals(new byte[0], new ColumnValue.BytesValue(new byte[0]).value());
        assertInstanceOf(ColumnValue.NullValue.class, ColumnValue.of(null));
        assertNull(ColumnValue.toRaw(ColumnValue.of(null)));
        assertThrows(NullPointerException.class, () -> new ColumnValue.BytesValue(null));
    }
}
