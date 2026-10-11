package org.pangrova.connector.lance;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.pangrova.spi.connector.data.type.ArrayType;
import org.pangrova.spi.connector.data.type.MapType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.data.type.PangrovaTypeMapping;
import org.pangrova.spi.connector.data.type.RowField;
import org.pangrova.spi.connector.data.type.RowType;
import org.pangrova.spi.connector.data.type.Type;
import org.pangrova.spi.connector.data.type.VectorType;
import org.pangrova.spi.connector.data.type.primitive.DecimalType;
import org.pangrova.spi.connector.data.type.primitive.temporal.TimestampType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalInt;

/** Decodes the actual Arrow IPC schema exported by the real Native writer fixture. */
final class LanceFixtureSchema {
    private LanceFixtureSchema() {}

    static ConnectorTableMetadata metadata(SchemaTableName table, byte[] ipc) {
        if (ipc.length == 0 || ipc.length > 4 * 1024 * 1024) {
            throw new IllegalArgumentException("Fixture requires bounded actual Arrow schema bytes");
        }
        var schema = Schema.deserializeMessage(ByteBuffer.wrap(ipc));
        var columns = schema.getFields().stream().map(field -> {
            var type = type(field);
            return new ConnectorTableMetadata.Column(field.getName(), PangrovaTypeMapping.toPangrovaType(type),
                    OptionalInt.empty(), type);
        }).toList();
        return new ConnectorTableMetadata(table, columns.stream().map(ConnectorTableMetadata.Column::name).toList(),
                columns, Map.of());
    }

    private static Type type(Field field) {
        var arrow = field.getType();
        if (arrow instanceof ArrowType.Int integer) {
            if (!integer.getIsSigned()) throw new IllegalArgumentException("Unsigned fixture SQL column");
            return scalar(switch (integer.getBitWidth()) {
                case 8 -> PangrovaType.TINYINT;
                case 16 -> PangrovaType.SMALLINT;
                case 32 -> PangrovaType.INTEGER;
                case 64 -> PangrovaType.BIGINT;
                default -> throw new IllegalArgumentException("Unknown Arrow integer width");
            });
        }
        if (arrow instanceof ArrowType.FloatingPoint floating) {
            return scalar(switch (floating.getPrecision()) {
                case SINGLE -> PangrovaType.FLOAT;
                case DOUBLE -> PangrovaType.DOUBLE;
                default -> throw new IllegalArgumentException("Unsupported half-precision SQL column");
            });
        }
        if (arrow instanceof ArrowType.Bool) return scalar(PangrovaType.BOOLEAN);
        if (arrow instanceof ArrowType.Utf8 || arrow instanceof ArrowType.LargeUtf8) return scalar(PangrovaType.VARCHAR);
        if (arrow instanceof ArrowType.Binary || arrow instanceof ArrowType.LargeBinary
                || arrow instanceof ArrowType.FixedSizeBinary) return scalar(PangrovaType.VARBINARY);
        if (arrow instanceof ArrowType.Decimal decimal) return new DecimalType(decimal.getPrecision(), decimal.getScale());
        if (arrow instanceof ArrowType.Date) return scalar(PangrovaType.DATE);
        if (arrow instanceof ArrowType.Time) return scalar(PangrovaType.TIME);
        if (arrow instanceof ArrowType.Timestamp timestamp) return new TimestampType(timestamp.getTimezone() != null);
        if (arrow instanceof ArrowType.Struct) return new RowType(field.getChildren().stream()
                .map(child -> new RowField(child.getName(), type(child))).toList());
        return structural(field);
    }

    private static Type structural(Field field) {
        var arrow = field.getType();
        if (arrow instanceof ArrowType.List || arrow instanceof ArrowType.LargeList) {
            if (field.getChildren().size() != 1) throw new IllegalArgumentException("Malformed list schema");
            return new ArrayType(type(field.getChildren().get(0)));
        }
        if (arrow instanceof ArrowType.FixedSizeList vector) {
            if (field.getChildren().size() != 1) throw new IllegalArgumentException("Malformed vector schema");
            var element = PangrovaTypeMapping.toPangrovaType(type(field.getChildren().get(0)));
            return new VectorType(vector.getListSize(), switch (element) {
                case FLOAT -> VectorType.ElementType.FLOAT;
                case DOUBLE -> VectorType.ElementType.DOUBLE;
                default -> throw new IllegalArgumentException("Fixed-size list is not a supported SQL vector");
            });
        }
        if (arrow instanceof ArrowType.Map && field.getChildren().size() == 1) {
            var entries = field.getChildren().get(0).getChildren();
            if (entries.size() != 2) throw new IllegalArgumentException("Malformed map schema");
            return new MapType(type(entries.get(0)), type(entries.get(1)));
        }
        throw new IllegalArgumentException("Unsupported actual fixture Arrow type: " + arrow);
    }

    private static Type scalar(PangrovaType type) { return PangrovaTypeMapping.toType(type); }
}
