package org.pangrova.client.jdbc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.transport.client.ArrowBatchDecoder;
import org.pangrova.transport.client.ResultBatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaResultSetUuidTest {
    private static final UUID[] VALUES = {new UUID(0, 0), new UUID(-1, -1),
            UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), null};

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uuidValuesAndMetadataComeFromTheActualWireDeclaration(boolean empty) throws Exception {
        try (var result = new PangrovaResultSet(batch(empty, true), null)) {
            var metadata = result.getMetaData();
            assertThat(metadata.getColumnType(1)).isEqualTo(Types.OTHER);
            assertThat(metadata.getColumnTypeName(1)).isEqualTo("UUID");
            assertThat(metadata.getColumnClassName(1)).isEqualTo(UUID.class.getName());
            assertThat(metadata.getColumnDisplaySize(1)).isEqualTo(36);
            assertThat(metadata.getPrecision(1)).isZero();
            assertThat(metadata.getScale(1)).isZero();
            assertThatThrownBy(() -> metadata.getColumnDisplaySize(0)).isInstanceOf(SQLException.class);
            if (!empty) {
                for (UUID value : VALUES) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getObject("id")).isEqualTo(value);
                    assertThat(result.wasNull()).isEqualTo(value == null);
                    assertThat(result.getString(1)).isEqualTo(value == null ? null : value.toString());
                }
            }
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    @org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.LOCALE)
    void jdbcLabelsPreserveAsciiCaseInsensitivityUnderTurkishLocale() throws Exception {
        var original = java.util.Locale.getDefault();
        try {
            java.util.Locale.setDefault(java.util.Locale.forLanguageTag("tr-TR"));
            try (var result = new PangrovaResultSet(batch(false, true), null)) {
                assertThat(result.findColumn("ID")).isOne();
                assertThat(result.next()).isTrue();
                assertThat(result.getObject("iD")).isEqualTo(VALUES[0]);
            }
        } finally {
            java.util.Locale.setDefault(original);
        }
    }

    @Test
    void unannotatedSixteenByteBinaryDoesNotBecomeUuid() throws Exception {
        try (var result = new PangrovaResultSet(batch(false, false), null)) {
            assertThat(result.getMetaData().getColumnTypeName(1)).isNotEqualTo("UUID");
            assertThat(result.getMetaData().getColumnClassName(1)).isNotEqualTo(UUID.class.getName());
            assertThat(result.next()).isTrue();
            assertThat((byte[]) result.getObject(1)).containsExactly(new byte[16]);
        }
    }

    private static ResultBatch batch(boolean empty, boolean uuid) throws Exception {
        var metadata = uuid ? Map.of("ARROW:extension:name", "arrow.uuid", "ARROW:extension:metadata", "")
                : Map.<String, String>of();
        var field = new Field("ID", new FieldType(true, new ArrowType.FixedSizeBinary(16), null, metadata), null);
        var output = new ByteArrayOutputStream();
        try (var allocator = new RootAllocator();
                var root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            var vector = (FixedSizeBinaryVector) root.getVector(0);
            if (!empty) {
                for (int i = 0; i < VALUES.length; i++) {
                    UUID value = VALUES[i];
                    if (value == null) vector.setNull(i);
                    else vector.setSafe(i, ByteBuffer.allocate(16)
                            .putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()).array());
                }
            }
            root.setRowCount(empty ? 0 : VALUES.length);
            try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(output))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        return ArrowBatchDecoder.decode(output.toByteArray());
    }
}
