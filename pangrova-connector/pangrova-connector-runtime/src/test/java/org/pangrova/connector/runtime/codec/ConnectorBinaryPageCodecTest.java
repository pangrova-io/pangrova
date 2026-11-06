package org.pangrova.connector.runtime.codec;

import org.junit.jupiter.api.Test;
import org.pangrova.batch.api.page.BatchFormat;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.batch.api.page.Page;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.types.value.TypedRow;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorBinaryPageCodecTest {
    private static Page row(ColumnValue value) {
        return new Page(List.of("v"), List.of(TypedRow.of(Map.of("v", value))));
    }

    private static NativePage payload(byte[] bytes) {
        return new NativePage(bytes, -1, -1, BatchFormat.CUSTOM_BINARY);
    }

    @Test
    void rejectsLossyConversionsInsteadOfStringifying() {
        for (ColumnValue value : List.of(new ColumnValue.BytesValue(new byte[]{1}),
                new ColumnValue.DecimalValue(new java.math.BigDecimal("12.34")),
                new ColumnValue.DateValue(java.time.LocalDate.of(2026, 1, 1)),
                new ColumnValue.FloatValue(1.5f), new ColumnValue.ArrayValue(List.of(1, 2)))) {
            assertThatThrownBy(() -> ConnectorBinaryPageCodec.fromPage(row(value)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not support");
        }
    }

    @Test
    void checksFormatAndDeclaredDimensionsWithoutTakingPageOwnership() {
        try (NativePage encoded = ConnectorBinaryPageCodec.fromPage(row(new ColumnValue.IntValue(7)))) {
            assertThat(ConnectorBinaryPageCodec.toPage(encoded).rows().get(0).getInt("v")).isEqualTo(7);
            assertThat(encoded.isClosed()).isFalse();
            try (NativePage wrongFormat = new NativePage(encoded.serializedBatch(), 1, 1, BatchFormat.ARROW_IPC);
                 NativePage wrongRows = new NativePage(encoded.serializedBatch(), 2, 1, BatchFormat.CUSTOM_BINARY);
                 NativePage wrongColumns = new NativePage(encoded.serializedBatch(), 1, 2, BatchFormat.CUSTOM_BINARY)) {
                for (NativePage invalid : List.of(wrongFormat, wrongRows, wrongColumns)) {
                    assertThatThrownBy(() -> ConnectorBinaryPageCodec.toPage(invalid))
                            .isInstanceOf(IllegalArgumentException.class);
                }
            }
        }
    }

    @Test
    void rejectsTruncationLengthsCountsTrailingBytesAndNonCanonicalValues() {
        byte[] valid;
        try (NativePage encoded = ConnectorBinaryPageCodec.fromPage(row(new ColumnValue.BooleanValue(true)))) {
            valid = encoded.serializedBatch();
        }
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            try (NativePage page = payload(truncated)) {
                assertThatThrownBy(() -> ConnectorBinaryPageCodec.toPage(page)).isInstanceOf(IllegalArgumentException.class);
            }
        }
        byte[] negativeRows = valid.clone(); ByteBuffer.wrap(negativeRows).putInt(1, -1);
        byte[] hugeRows = valid.clone(); ByteBuffer.wrap(hugeRows).putInt(1, Integer.MAX_VALUE);
        byte[] hugeColumns = valid.clone(); ByteBuffer.wrap(hugeColumns).putInt(5, Integer.MAX_VALUE);
        byte[] negativeLength = valid.clone(); ByteBuffer.wrap(negativeLength).putInt(9, -1);
        byte[] hugeLength = valid.clone(); ByteBuffer.wrap(hugeLength).putInt(9, Integer.MAX_VALUE);
        byte[] invalidBoolean = valid.clone(); invalidBoolean[invalidBoolean.length - 1] = 2;
        byte[] invalidUtf8 = valid.clone(); invalidUtf8[13] = (byte) 0xff;
        byte[] unknownVersion = valid.clone(); unknownVersion[0] = 2;
        byte[] unknownTag = valid.clone(); unknownTag[valid.length - 2] = 99;
        for (byte[] invalid : List.of(negativeRows, hugeRows, hugeColumns, negativeLength, hugeLength,
                invalidBoolean, invalidUtf8, unknownVersion, unknownTag, Arrays.copyOf(valid, valid.length + 1))) {
            try (NativePage page = payload(invalid)) {
                assertThatThrownBy(() -> ConnectorBinaryPageCodec.toPage(page)).isInstanceOf(IllegalArgumentException.class);
            }
        }
    }
}
