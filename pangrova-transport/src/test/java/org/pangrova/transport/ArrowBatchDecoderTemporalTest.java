package org.pangrova.transport;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class ArrowBatchDecoderTemporalTest {
    @Test
    void ipcDatesHaveTheSameJavaTypeAtRootAndInsideStructs() throws Exception {
        Field date = new Field("day", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        Field nested = new Field("nested", FieldType.nullable(new ArrowType.Struct()), List.of(date));
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(new Schema(List.of(date, nested)), allocator)) {
            root.allocateNew();
            var top = (DateDayVector) root.getVector(0);
            var struct = (StructVector) root.getVector(1);
            var child = struct.getChild("day", DateDayVector.class);
            int[] days = {-719162, -1, 0, 2932896};
            for (int i = 0; i < days.length; i++) {
                top.setSafe(i, days[i]);
                struct.setIndexDefined(i);
                child.setSafe(i, days[i]);
            }
            top.setNull(4);
            struct.setIndexDefined(4);
            child.setNull(4);
            struct.setNull(5);
            top.setNull(5);
            root.setRowCount(6);
            var bytes = new ByteArrayOutputStream();
            try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(bytes))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            var rows = ArrowBatchDecoder.decode(bytes.toByteArray()).rows();
            for (int i = 0; i < days.length; i++) {
                assertThat(rows.get(i)[0]).isEqualTo(LocalDate.ofEpochDay(days[i]));
                assertThat(rows.get(i)[1]).isEqualTo(Map.of("day", LocalDate.ofEpochDay(days[i])));
            }
            var nullDay = new java.util.LinkedHashMap<String, Object>();
            nullDay.put("day", null);
            assertThat(rows.get(4)[1]).isEqualTo(nullDay);
            assertThat(rows.get(5)[1]).isNull();
        }
    }

    static Stream<Arguments> numericTemporalValues() {
        return Stream.of(
                Arguments.of(new ArrowType.Date(DateUnit.MILLISECOND), -86_400_000L, LocalDate.of(1969, 12, 31)),
                Arguments.of(new ArrowType.Time(TimeUnit.SECOND, 32), 86_399L, LocalTime.of(23, 59, 59)),
                Arguments.of(new ArrowType.Time(TimeUnit.MILLISECOND, 32), 86_399_999L,
                        LocalTime.of(23, 59, 59, 999_000_000)),
                Arguments.of(new ArrowType.Time(TimeUnit.MICROSECOND, 64), 86_399_999_999L,
                        LocalTime.of(23, 59, 59, 999_999_000)),
                Arguments.of(new ArrowType.Time(TimeUnit.NANOSECOND, 64), 86_399_999_999_999L,
                        LocalTime.of(23, 59, 59, 999_999_999)));
    }

    @ParameterizedTest
    @MethodSource("numericTemporalValues")
    void declaredUnitsApplyRecursivelyToListAndMapValues(ArrowType type, long value, Object expected) {
        Field scalar = new Field("value", FieldType.nullable(type), null);
        Field list = new Field("items", FieldType.nullable(new ArrowType.List()), List.of(scalar));
        Field key = new Field("key", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), List.of(key, scalar));
        Field map = new Field("props", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        assertThat(ArrowBatchDecoder.normalizeComplexValue(List.of(value), list)).isEqualTo(List.of(expected));
        assertThat(ArrowBatchDecoder.normalizeComplexValue(
                List.of(Map.of("key", 1, "value", value)), map)).isEqualTo(Map.of(1, expected));
        assertThat(ArrowBatchDecoder.normalizeComplexValue(null, scalar)).isNull();
    }
}
