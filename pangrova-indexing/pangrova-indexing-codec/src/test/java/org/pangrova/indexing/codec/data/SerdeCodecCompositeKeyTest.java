package org.pangrova.indexing.codec.data;

import org.junit.jupiter.api.Test;
import org.pangrova.indexing.api.data.CompositeIndexKey;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerdeCodecCompositeKeyTest {
    @Test
    void roundTripsEverySupportedScalarWithoutTypeCoercion() {
        var key = new CompositeIndexKey(List.of("雪", 1, 2L, 3.5, 4.5f, (short) 6, (byte) 7,
                new BigDecimal("12.3400"), new BigInteger("12345678901234567890"), new UUID(1, 2)));
        byte[] bytes = SerdeCodec.toBytes(out -> SerdeCodec.writeComparable(out, key));
        assertThat(SerdeCodec.fromBytes(bytes, SerdeCodec::readComparable)).isEqualTo(key);
        assertThat(bytes[0]).isEqualTo((byte) 11);
        for (int length = 0; length < bytes.length; length++) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            assertThatThrownBy(() -> SerdeCodec.fromBytes(truncated, SerdeCodec::readComparable))
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void rejectsInvalidCountsBeforeAllocationAndRejectsNestedOrNullComponents() {
        for (int count : new int[]{-1, 0, 1, 1025, Integer.MAX_VALUE}) {
            byte[] payload = SerdeCodec.toBytes(out -> { out.writeByte(11); out.writeInt(count); });
            assertThatThrownBy(() -> SerdeCodec.fromBytes(payload, SerdeCodec::readComparable))
                    .isInstanceOf(UncheckedIOException.class);
        }
        for (int tag : new int[]{0, 11, 127}) {
            byte[] payload = SerdeCodec.toBytes(out -> { out.writeByte(11); out.writeInt(2); out.writeByte(tag); });
            assertThatThrownBy(() -> SerdeCodec.fromBytes(payload, SerdeCodec::readComparable))
                    .isInstanceOf(UncheckedIOException.class);
        }
    }
}
