package org.pangrova.coordinator.query.execution.dispatch;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FixtureOutputHandleCodecTest {

    private static final SchemaTableName TARGET = new SchemaTableName("analytics", "events");

    @Test
    void roundTripsExactBoundHandleAndColumns() {
        FixtureOutputHandleCodec codec = codec();
        OutputHandle handle = new OutputHandle(TARGET, "INSERT");

        byte[] encoded = codec.serialize(handle);

        assertEquals(handle, codec.deserialize(codec.codecVersion(), encoded));
        assertArrayEquals(encoded, codec.serialize(codec.deserialize(codec.codecVersion(), encoded)));
        assertEquals(List.of("id"), codec.writeColumnNames(handle));
    }

    @Test
    void rejectsWrongVersionTargetModeAndTrailingBytes() {
        FixtureOutputHandleCodec codec = codec();
        byte[] encoded = codec.serialize(new OutputHandle(TARGET, "INSERT"));
        byte[] trailing = java.util.Arrays.copyOf(encoded, encoded.length + 1);

        assertThrows(IllegalArgumentException.class,
                () -> codec.deserialize(codec.codecVersion() + 1, encoded));
        assertThrows(IllegalArgumentException.class,
                () -> codec.deserialize(codec.codecVersion(), trailing));
        assertThrows(IllegalArgumentException.class,
                () -> codec.serialize(new OutputHandle(new SchemaTableName("other", "events"), "INSERT")));
        assertThrows(IllegalArgumentException.class,
                () -> codec.serialize(new OutputHandle(TARGET, "DELETE")));
    }

    private static FixtureOutputHandleCodec codec() {
        return new FixtureOutputHandleCodec("pangrova.io/test/fixture-output-handle",
                TARGET, Set.of("INSERT"), List.of("id"), mode -> new OutputHandle(TARGET, mode));
    }

    private record OutputHandle(SchemaTableName target, String writeMode)
            implements ConnectorOutputTableHandle {
        @Override public Optional<SchemaTableName> targetTable() { return Optional.of(target); }
    }
}
