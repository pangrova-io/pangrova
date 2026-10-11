package org.pangrova.planner.substrait;

import io.substrait.extension.ExtensionCollector;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import io.substrait.type.proto.TypeProtoConverter;
import org.pangrova.planner.type.ConnectorColumnTypeMapper;
import org.pangrova.planner.type.PlannerTypeFactory;
import org.pangrova.planner.type.StorageVectorRelDataType;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

import java.util.HashSet;
import java.util.List;

/** Compiles metadata into the exact canonical schema used by Native plan conversion. */
public final class NativeDatasetSchemaCompiler {
    private NativeDatasetSchemaCompiler() {}

    /** Immutable physical schema and the ordered logical column constraints. */
    public record CompiledSchema(io.substrait.proto.NamedStruct schema, List<LogicalType> columnTypes) {
        public CompiledSchema {
            columnTypes = List.copyOf(columnTypes);
        }
    }

    /** No storage access: only the Native Worker computes the physical schema fingerprint. */
    public static CompiledSchema compile(List<ConnectorTableMetadata.Column> columns) {
        validate(columns);
        var row = ConnectorColumnTypeMapper.rowType(columns, new PlannerTypeFactory());
        var mapper = new TypeMapper();
        var converter = new TypeProtoConverter(new ExtensionCollector());
        var struct = NamedStruct.of(SubstraitFieldNames.flatten(row, List.of()), TypeCreator.of(false).struct(
                row.getFieldList().stream().map(field -> mapper.map(field.getType())).toList())).toProto(converter);
        List<LogicalType> logical = java.util.stream.IntStream.range(0, columns.size())
                .mapToObj(index -> logicalType(columns.get(index), row.getFieldList().get(index).getType(),
                        struct.getStruct().getTypes(index))).toList();
        return new CompiledSchema(struct, logical);
    }

    private static void validate(List<ConnectorTableMetadata.Column> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Native schema requires columns");
        }
        HashSet<String> names = new HashSet<>();
        for (var column : columns) {
            if (column.name().isBlank() || !names.add(column.name()) || column.type() == PangrovaType.UNKNOWN) {
                throw new IllegalArgumentException("Native schema requires unique names and concrete types");
            }
            if (column.parameterizedType() == null && requiresParameters(column.type())) {
                throw new IllegalArgumentException("Native schema requires structural type parameters");
            }
        }
    }

    private static boolean requiresParameters(PangrovaType type) {
        return switch (type) {
            case ARRAY, MAP, STRUCT, VECTOR, DECIMAL -> true;
            default -> false;
        };
    }

    private static LogicalType logicalType(ConnectorTableMetadata.Column column,
            org.apache.calcite.rel.type.RelDataType sqlType, io.substrait.proto.Type physical) {
        if (sqlType instanceof StorageVectorRelDataType vector) {
            LogicalType type = vector.logicalType();
            if (!type.getSubstraitType().equals(physical)) {
                throw new IllegalArgumentException(
                        "VECTOR physical schema differs from its canonical logical declaration");
            }
            return type;
        }
        String name = column.parameterizedType() == null
                ? column.type().name() : column.parameterizedType().getDisplayName();
        return LogicalType.newBuilder().setCanonicalSql(name).setSubstraitType(physical).build();
    }
}
